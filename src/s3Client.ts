import { S3Client, PutObjectCommand, GetObjectCommand, CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand, AbortMultipartUploadCommand } from "@aws-sdk/client-s3";
import { Readable, pipeline } from "stream";
import { promisify } from "util";
import * as core from "@actions/core";
import * as fs from "fs";
import * as path from "path";
import { createGunzip } from 'zlib';
import * as zlib from "zlib";
import * as tar from "tar";
import * as os from "os";

export let s3Client: S3Client;

export function initializeS3Client(): S3Client {
    if (s3Client) {
        return s3Client;
    }

    const accessKeyId = process.env.AWS_ACCESS_KEY_ID;
    const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
    const region = process.env.AWS_REGION;

    if (!accessKeyId || !secretAccessKey || !region) {
        throw new Error("AWS credentials or region not provided");
    }

    s3Client = new S3Client({
        credentials: {
            accessKeyId,
            secretAccessKey
        },
        region
    });

    return s3Client;
}

async function compressData(filePath: string): Promise<string> {
    const compressedFilePath = path.join(os.tmpdir(), `${filePath}.gz`);
    const fileContent = await fs.promises.readFile(filePath);

    return new Promise((resolve, reject) => {
        const writeStream = fs.createWriteStream(compressedFilePath);
        const gzip = zlib.createGzip();

        const readStream = Readable.from(fileContent);
        readStream
            .pipe(gzip)
            .pipe(writeStream)
            .on('finish', () => resolve(compressedFilePath))
            .on('error', reject);
    });
}

async function compressDirectory(dirPath: string, key: string): Promise<string> {
    const tempFile = path.join(os.tmpdir(), `${path.basename(key)}.tar.gz`);

    await tar.create(
        {
            gzip: true,
            file: tempFile,
            cwd: path.dirname(dirPath)
        },
        [path.basename(dirPath)]
    );

    return tempFile; // Return path of compressed tarball
}


export async function uploadToS3(bucketName: string, key: string, filePath: string): Promise<void> {
    const client = initializeS3Client();
    let compressedFilePath: string;
    let isCompressed = false;

    if (fs.statSync(filePath).isDirectory()) {
        compressedFilePath = await compressDirectory(filePath, key);
        isCompressed = true;
    } else {
        compressedFilePath = await compressData(filePath);
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

export async function downloadFromS3(bucketName: string, key: string, destinationPath: string): Promise<void> {
    const directory = path.dirname(destinationPath);
    const compressedPath = path.join(directory, 'compressed');
    const archiveDestinationPath = path.join(compressedPath, path.basename(destinationPath + ".gz"));
    const client = initializeS3Client();
    const command = new GetObjectCommand({
        Bucket: bucketName,
        Key: key
    });

    try {
        const { Body } = await client.send(command);


        if (Body instanceof Readable) {
            // make 'compressed' directory if it doesn't exist
            if (!fs.existsSync(compressedPath)) {
                fs.mkdirSync(compressedPath, { recursive: true });
            }

            let writeStream = fs.createWriteStream(archiveDestinationPath);

            await promisify(pipeline)(Body, writeStream);

            // Unzip the .gz file first
            const gunzipStream = createGunzip();
            await promisify(pipeline)(
                fs.createReadStream(archiveDestinationPath).pipe(gunzipStream),
                fs.createWriteStream(destinationPath)
            );

            const isTar = await isTarFile(destinationPath);
            if (isTar) {
                await promisify(pipeline)(
                    fs.createReadStream(destinationPath),
                    tar.extract({ cwd: directory })
                );
            }
        } else {
            throw new Error("Invalid response body from S3");
        }
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
