import { S3Client, PutObjectCommand, GetObjectCommand, CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand, AbortMultipartUploadCommand } from "@aws-sdk/client-s3";
import { Readable, pipeline, PassThrough } from "stream";
import { promisify } from "util";
import * as core from "@actions/core";
import * as fs from "fs";
import * as path from "path";
import { createGunzip, createGzip } from 'zlib';
import * as zlib from "zlib";
import * as tar from "tar";
import * as os from "os";
import { spawn } from "child_process";

export let s3Client: S3Client;

// Check if zstd is available on the system
async function isZstdAvailable(): Promise<boolean> {
    return new Promise((resolve) => {
        const proc = spawn('zstd', ['--version']);
        proc.on('close', (code) => resolve(code === 0));
        proc.on('error', () => resolve(false));
    });
}

// Create a zstd decompression stream using command-line zstd
function createZstdDecompressStream(): NodeJS.ReadWriteStream {
    const proc = spawn('zstd', ['-d', '--stdout'], {
        stdio: ['pipe', 'pipe', 'inherit']
    });

    const passThrough = new PassThrough();
    proc.stdout.pipe(passThrough);

    // Create a duplex-like stream
    const stream = new PassThrough();
    stream.pipe(proc.stdin);

    // Forward data from proc.stdout to our output
    (stream as any).readable = passThrough;

    return stream as any;
}

// Create a zstd compression stream using command-line zstd
function createZstdCompressStream(level: number = 3): NodeJS.ReadWriteStream {
    const proc = spawn('zstd', [`-${level}`, '--stdout'], {
        stdio: ['pipe', 'pipe', 'inherit']
    });

    const passThrough = new PassThrough();
    proc.stdout.pipe(passThrough);

    const stream = new PassThrough();
    stream.pipe(proc.stdin);

    (stream as any).readable = passThrough;

    return stream as any;
}

export function initializeS3Client(): S3Client {
    if (s3Client) {
        return s3Client;
    }

    const accessKeyId = process.env.BP_CACHE_AWS_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY_ID;
    const secretAccessKey = process.env.BP_CACHE_AWS_SECRET_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY;
    const region = process.env.BP_CACHE_AWS_REGION || process.env.AWS_REGION;

    if (!accessKeyId || !secretAccessKey || !region) {
        throw new Error("AWS credentials or region not provided");
    }

    core.info(`[S3 Debug] Region: ${region}`);
    core.info(`[S3 Debug] Access Key ID: ${accessKeyId.substring(0, 8)}...`);
    core.info(`[S3 Debug] Bucket: ${process.env.BP_CACHE_S3_BUCKET}`);

    s3Client = new S3Client({
        credentials: {
            accessKeyId,
            secretAccessKey
        },
        region,
        followRegionRedirects: true,
        forcePathStyle: true
    });

    return s3Client;
}

async function compressData(filePath: string, key: string, useZstd: boolean): Promise<string> {
    const ext = useZstd ? '.zst' : '.gz';
    const compressedFilePath = path.join(
        os.tmpdir(),
        `${path.basename(key)}${ext}`
    );
    const fileContent = await fs.promises.readFile(filePath);

    return new Promise((resolve, reject) => {
        const writeStream = fs.createWriteStream(compressedFilePath);

        if (useZstd) {
            const proc = spawn('zstd', ['-3', '--stdout'], {
                stdio: ['pipe', 'pipe', 'inherit']
            });
            const readStream = Readable.from(fileContent);
            readStream.pipe(proc.stdin);
            proc.stdout.pipe(writeStream);
            writeStream.on('finish', () => resolve(compressedFilePath));
            writeStream.on('error', reject);
            proc.on('error', reject);
        } else {
            const gzip = zlib.createGzip();
            const readStream = Readable.from(fileContent);
            readStream
                .pipe(gzip)
                .pipe(writeStream)
                .on('finish', () => resolve(compressedFilePath))
                .on('error', reject);
        }
    });
}

async function compressDirectory(dirPath: string, key: string, useZstd: boolean): Promise<string> {
    const ext = useZstd ? '.tar.zst' : '.tar.gz';
    const tempFile = path.join(os.tmpdir(), `${path.basename(key)}${ext}`);

    if (useZstd) {
        // Use command-line tar with zstd for best performance
        return new Promise((resolve, reject) => {
            const proc = spawn('tar', [
                '-cf', tempFile,
                '--use-compress-program=zstd',
                '-C', path.dirname(dirPath),
                path.basename(dirPath)
            ], {
                stdio: ['inherit', 'inherit', 'inherit']
            });

            proc.on('close', (code) => {
                if (code === 0) {
                    resolve(tempFile);
                } else {
                    reject(new Error(`tar exited with code ${code}`));
                }
            });
            proc.on('error', reject);
        });
    } else {
        await tar.create(
            {
                gzip: true,
                file: tempFile,
                cwd: path.dirname(dirPath)
            },
            [path.basename(dirPath)]
        );
        return tempFile;
    }
}


export async function uploadToS3(bucketName: string, key: string, filePath: string): Promise<void> {
    core.info(`[S3 Debug] uploadToS3 - Bucket: ${bucketName}, Key: ${key}, FilePath: ${filePath}`);
    const client = initializeS3Client();
    let compressedFilePath: string;
    let isCompressed = false;

    const useZstd = await isZstdAvailable();
    if (useZstd) {
        core.info(`Using zstd for compression`);
    } else {
        core.info(`Using gzip for compression (zstd not available)`);
    }

    if (fs.statSync(filePath).isDirectory()) {
        compressedFilePath = await compressDirectory(filePath, key, useZstd);
        isCompressed = true;
    } else {
        compressedFilePath = await compressData(filePath, key, useZstd);
        isCompressed = true;
    }

    const fileSize = fs.statSync(compressedFilePath).size;
    const chunkSize = 5 * 1024 * 1024; // 5MB chunk size

    if (fileSize <= chunkSize) {
        // Small file, use simple upload
        const fileStream = fs.createReadStream(compressedFilePath);
        const command = new PutObjectCommand({
            Bucket: bucketName,
            Key: key,
            Body: fileStream
        });

        await client.send(command);
    } else {
        // Large file, use multipart upload
        const multipartUpload = await client.send(new CreateMultipartUploadCommand({
            Bucket: bucketName,
            Key: key
        }));

        const uploadId = multipartUpload.UploadId;
        const parts: { ETag: string; PartNumber: number }[] = [];

        try {
            let partNumber = 1;
            const fileStream = fs.createReadStream(compressedFilePath, { highWaterMark: chunkSize });
            let partBuffer = Buffer.alloc(0);

            for await (const chunk of fileStream) {
                const uploadPartCommand = new UploadPartCommand({
                    Bucket: bucketName,
                    Key: key,
                    UploadId: uploadId,
                    PartNumber: partNumber,
                    Body: chunk
                });

                const { ETag } = await client.send(uploadPartCommand);
                parts.push({ ETag: ETag!, PartNumber: partNumber });
                partNumber++;
            }

            await client.send(new CompleteMultipartUploadCommand({
                Bucket: bucketName,
                Key: key,
                UploadId: uploadId,
                MultipartUpload: { Parts: parts }
            }));
        } catch (error) {
            await client.send(new AbortMultipartUploadCommand({
                Bucket: bucketName,
                Key: key,
                UploadId: uploadId
            }));
            throw error;
        }
    }

    core.info(`Successfully uploaded ${isCompressed ? 'compressed ' : ''}${filePath} to S3 bucket ${bucketName} with key ${key}`);
}

// Detect compression format from magic bytes
// gzip: 0x1f 0x8b
// zstd: 0x28 0xb5 0x2f 0xfd
function detectCompressionFormat(header: Buffer): 'gzip' | 'zstd' | 'unknown' {
    if (header.length >= 2 && header[0] === 0x1f && header[1] === 0x8b) {
        return 'gzip';
    }
    if (header.length >= 4 && header[0] === 0x28 && header[1] === 0xb5 && header[2] === 0x2f && header[3] === 0xfd) {
        return 'zstd';
    }
    return 'unknown';
}

export async function downloadFromS3(bucketName: string, key: string, destinationPath: string): Promise<void> {
    const directory = path.dirname(destinationPath) || '.';
    const client = initializeS3Client();
    const command = new GetObjectCommand({
        Bucket: bucketName,
        Key: key
    });

    try {
        const { Body } = await client.send(command);

        if (!(Body instanceof Readable)) {
            throw new Error("Invalid response body from S3");
        }

        // Ensure destination directory exists
        if (directory && directory !== '.') {
            fs.mkdirSync(directory, { recursive: true });
        }

        // Download to temp file first (more reliable than streaming with format detection)
        const tempFile = path.join(os.tmpdir(), `cache-download-${Date.now()}`);
        const writeStream = fs.createWriteStream(tempFile);

        await promisify(pipeline)(Body, writeStream);

        // Detect format from temp file
        const fd = await fs.promises.open(tempFile, 'r');
        const header = Buffer.alloc(4);
        await fd.read(header, 0, 4, 0);
        await fd.close();

        const format = detectCompressionFormat(header);
        const zstdAvailable = await isZstdAvailable();

        if (format === 'zstd' && zstdAvailable) {
            core.info(`Detected zstd compression, extracting with zstd`);

            await new Promise<void>((resolve, reject) => {
                const tarProc = spawn('tar', ['-xf', tempFile, '--use-compress-program=zstd', '-C', directory || '.'], {
                    stdio: ['inherit', 'inherit', 'inherit']
                });

                tarProc.on('close', (code) => {
                    if (code === 0) {
                        resolve();
                    } else {
                        reject(new Error(`tar exited with code ${code}`));
                    }
                });

                tarProc.on('error', reject);
            });
        } else if (format === 'gzip' || format === 'unknown') {
            core.info(`Detected gzip compression, extracting with gzip`);

            await promisify(pipeline)(
                fs.createReadStream(tempFile),
                createGunzip(),
                tar.extract({ cwd: directory || '.' })
            );
        } else {
            // zstd format but zstd not available
            throw new Error(`Cache is zstd compressed but zstd is not available on this runner`);
        }

        // Clean up temp file
        fs.unlinkSync(tempFile);

        core.info(`Successfully downloaded and extracted cache from S3 bucket ${bucketName} with key ${key} to ${destinationPath}`);
    } catch (error) {
        throw new Error(`Failed to download file from S3: ${error}`);
    }
}

async function isTarFile(filePath: string): Promise<boolean> {
    const fd = await fs.promises.open(filePath, 'r');
    const buffer = Buffer.alloc(512); // Read the first 512 bytes (tar header size)

    await fd.read(buffer, 0, 512, 0);
    await fd.close();

    // The magic number "ustar" is located at byte positions 257-262
    const tarMagic = buffer.toString('ascii', 257, 262);

    return tarMagic === 'ustar';
}

async function isTarGz(filePath: string): Promise<boolean> {
    const fd = await fs.promises.open(filePath, 'r');
    const buffer = Buffer.alloc(262);
    await fd.read(buffer, 0, 262, 0);
    await fd.close();
    const isGzip = buffer[0] === 0x1f && buffer[1] === 0x8b;
    const isTar = buffer.toString('ascii', 257, 262) === 'ustar';
    return isGzip && isTar;
}
