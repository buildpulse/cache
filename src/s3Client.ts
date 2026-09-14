import * as core from "@actions/core";
import {
    AbortMultipartUploadCommand,
    CompleteMultipartUploadCommand,
    CreateMultipartUploadCommand,
    GetObjectCommand,
    PutObjectCommand,
    S3Client,
    UploadPartCommand
} from "@aws-sdk/client-s3";
import { spawn } from "child_process";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { PassThrough, pipeline, Readable } from "stream";
import * as tar from "tar";
import { promisify } from "util";
import { createGunzip, createGzip } from "zlib";
import * as zlib from "zlib";

import {
    CredentialSource,
    resolveCredentials,
    resolveRegion
} from "./credentials";

export let s3Client: S3Client;

// Check if zstd is available on the system
async function isZstdAvailable(): Promise<boolean> {
    return new Promise(resolve => {
        const proc = spawn("zstd", ["--version"]);
        proc.on("close", code => resolve(code === 0));
        proc.on("error", () => resolve(false));
    });
}

// Create a zstd decompression stream using command-line zstd
function createZstdDecompressStream(): NodeJS.ReadWriteStream {
    const proc = spawn("zstd", ["-d", "--stdout"], {
        stdio: ["pipe", "pipe", "inherit"]
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
function createZstdCompressStream(level = 3): NodeJS.ReadWriteStream {
    const proc = spawn("zstd", [`-${level}`, "--stdout"], {
        stdio: ["pipe", "pipe", "inherit"]
    });

    const passThrough = new PassThrough();
    proc.stdout.pipe(passThrough);

    const stream = new PassThrough();
    stream.pipe(proc.stdin);

    (stream as any).readable = passThrough;

    return stream as any;
}

/**
 * Where this client's credentials came from, for the log line and for the
 * error message when a request is denied. Set by initializeS3Client.
 */
let credentialSource: string = CredentialSource.None;

export function resolvedCredentialSource(): string {
    return credentialSource;
}

export function initializeS3Client(): S3Client {
    if (s3Client) {
        return s3Client;
    }

    const { region, fromAmbient } = resolveRegion();
    if (!region) {
        throw new Error(
            "No region for the cache bucket. Set the aws-region input or the " +
                "BP_CACHE_AWS_REGION environment variable."
        );
    }

    const resolved = resolveCredentials();
    credentialSource = resolved.source;
    if (!resolved.credentials) {
        throw new Error(
            "No credentials for the cache bucket. On a BuildPulse runner these " +
                "are provided automatically; if you are running elsewhere, set " +
                "the aws-access-key-id and aws-secret-access-key inputs, or " +
                "aws-credentials-file."
        );
    }

    core.info(
        `[cache] region ${region}${
            fromAmbient ? " (from AWS_REGION; prefer BP_CACHE_AWS_REGION)" : ""
        }`
    );
    core.info(`[cache] bucket ${process.env.BP_CACHE_S3_BUCKET || "(unset)"}`);
    core.info(
        `[cache] credentials from ${resolved.source}${
            resolved.detail ? ` -- ${resolved.detail}` : ""
        }`
    );
    if (process.env.BP_CACHE_KEY_PREFIX) {
        core.info(`[cache] key prefix ${process.env.BP_CACHE_KEY_PREFIX}`);
    }

    // The provider is always explicit. Handing the SDK a config with no
    // `credentials` would fall back to its default chain, whose first link is
    // the ambient AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY pair -- the exact
    // hijack this resolution exists to prevent.
    s3Client = new S3Client({ region, credentials: resolved.credentials });

    return s3Client;
}

async function compressData(
    filePath: string,
    key: string,
    useZstd: boolean
): Promise<string> {
    const ext = useZstd ? ".zst" : ".gz";
    const compressedFilePath = path.join(
        os.tmpdir(),
        `${path.basename(key)}${ext}`
    );
    const fileContent = await fs.promises.readFile(filePath);

    return new Promise((resolve, reject) => {
        const writeStream = fs.createWriteStream(compressedFilePath);

        if (useZstd) {
            const proc = spawn("zstd", ["-3", "--stdout"], {
                stdio: ["pipe", "pipe", "inherit"]
            });
            const readStream = Readable.from(fileContent);
            readStream.pipe(proc.stdin);
            proc.stdout.pipe(writeStream);
            writeStream.on("finish", () => resolve(compressedFilePath));
            writeStream.on("error", reject);
            proc.on("error", reject);
        } else {
            const gzip = zlib.createGzip();
            const readStream = Readable.from(fileContent);
            readStream
                .pipe(gzip)
                .pipe(writeStream)
                .on("finish", () => resolve(compressedFilePath))
                .on("error", reject);
        }
    });
}

async function compressDirectory(
    dirPath: string,
    key: string,
    useZstd: boolean
): Promise<string> {
    const ext = useZstd ? ".tar.zst" : ".tar.gz";
    const tempFile = path.join(os.tmpdir(), `${path.basename(key)}${ext}`);

    if (useZstd) {
        // Use command-line tar with zstd for best performance
        return new Promise((resolve, reject) => {
            // --ignore-failed-read: a cache directory is not guaranteed to be
            // fully readable by the runner user. /var/cache/apt/archives holds a
            // root-owned 0700 `partial/`, and GNU tar exits 2 on the first
            // unreadable entry, aborting the whole archive. Without this flag the
            // save fails and saveImpl downgrades it to a warning, so the job stays
            // green and the cache is simply never written. Skipping the unreadable
            // entry is strictly better than caching nothing.
            const proc = spawn(
                "tar",
                [
                    "-cf",
                    tempFile,
                    "--ignore-failed-read",
                    "--use-compress-program=zstd",
                    "-C",
                    path.dirname(dirPath),
                    path.basename(dirPath)
                ],
                {
                    stdio: ["inherit", "inherit", "inherit"]
                }
            );

            proc.on("close", code => {
                // 0 = clean. 1 = "some files differ"/were skipped, which is the
                // documented exit for --ignore-failed-read having done its job;
                // the archive is valid and worth uploading. 2 is a real failure.
                if (code === 0 || code === 1) {
                    if (code === 1) {
                        core.warning(
                            `tar skipped one or more unreadable entries under ${dirPath}; cached what it could`
                        );
                    }
                    resolve(tempFile);
                } else {
                    reject(new Error(`tar exited with code ${code}`));
                }
            });
            proc.on("error", reject);
        });
    } else {
        // Same tolerance on the node-tar fallback: warn on an unreadable entry
        // rather than rejecting the whole archive.
        await tar.create(
            {
                gzip: true,
                file: tempFile,
                cwd: path.dirname(dirPath),
                onwarn: (code: string, message: string) => {
                    core.warning(`tar: ${code}: ${message}`);
                }
            } as tar.CreateOptions & { file: string },
            [path.basename(dirPath)]
        );
        return tempFile;
    }
}

export async function uploadToS3(
    bucketName: string,
    key: string,
    filePath: string
): Promise<void> {
    core.info(
        `[S3 Debug] uploadToS3 - Bucket: ${bucketName}, Key: ${key}, FilePath: ${filePath}`
    );
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
        const multipartUpload = await client.send(
            new CreateMultipartUploadCommand({
                Bucket: bucketName,
                Key: key
            })
        );

        const uploadId = multipartUpload.UploadId;
        const parts: { ETag: string; PartNumber: number }[] = [];

        try {
            let partNumber = 1;
            const fileStream = fs.createReadStream(compressedFilePath, {
                highWaterMark: chunkSize
            });
            const partBuffer = Buffer.alloc(0);

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

            await client.send(
                new CompleteMultipartUploadCommand({
                    Bucket: bucketName,
                    Key: key,
                    UploadId: uploadId,
                    MultipartUpload: { Parts: parts }
                })
            );
        } catch (error) {
            await client.send(
                new AbortMultipartUploadCommand({
                    Bucket: bucketName,
                    Key: key,
                    UploadId: uploadId
                })
            );
            throw error;
        }
    }

    core.info(
        `Successfully uploaded ${
            isCompressed ? "compressed " : ""
        }${filePath} to S3 bucket ${bucketName} with key ${key}`
    );
}

// Detect compression format from magic bytes
// gzip: 0x1f 0x8b
// zstd: 0x28 0xb5 0x2f 0xfd
function detectCompressionFormat(header: Buffer): "gzip" | "zstd" | "unknown" {
    if (header.length >= 2 && header[0] === 0x1f && header[1] === 0x8b) {
        return "gzip";
    }
    if (
        header.length >= 4 &&
        header[0] === 0x28 &&
        header[1] === 0xb5 &&
        header[2] === 0x2f &&
        header[3] === 0xfd
    ) {
        return "zstd";
    }
    return "unknown";
}

export async function downloadFromS3(
    bucketName: string,
    key: string,
    destinationPath: string
): Promise<void> {
    const directory = path.dirname(destinationPath) || ".";
    const client = initializeS3Client();
    const command = new GetObjectCommand({
        Bucket: bucketName,
        Key: key
    });

    // No try/catch here. It used to wrap the whole body and rethrow a bare
    // Error built by stringifying the SDK's, which discarded `name` and
    // `$metadata` -- the only things that tell "nothing is cached under this
    // key" apart from "we are not allowed to read it". The error now reaches
    // the caller intact.
    {
        const { Body } = await client.send(command);

        if (!(Body instanceof Readable)) {
            throw new Error("Invalid response body from S3");
        }

        // Ensure destination directory exists
        if (directory && directory !== ".") {
            fs.mkdirSync(directory, { recursive: true });
        }

        // Download to temp file first (more reliable than streaming with format detection)
        const tempFile = path.join(os.tmpdir(), `cache-download-${Date.now()}`);
        const writeStream = fs.createWriteStream(tempFile);

        await promisify(pipeline)(Body, writeStream);

        // Detect format from temp file
        const fd = await fs.promises.open(tempFile, "r");
        const header = Buffer.alloc(4);
        await fd.read(header, 0, 4, 0);
        await fd.close();

        const format = detectCompressionFormat(header);
        const zstdAvailable = await isZstdAvailable();

        if (format === "zstd" && zstdAvailable) {
            core.info(`Detected zstd compression, extracting with zstd`);

            await new Promise<void>((resolve, reject) => {
                const tarProc = spawn(
                    "tar",
                    [
                        "-xf",
                        tempFile,
                        "--use-compress-program=zstd",
                        "-C",
                        directory || "."
                    ],
                    {
                        stdio: ["inherit", "inherit", "inherit"]
                    }
                );

                tarProc.on("close", code => {
                    if (code === 0) {
                        resolve();
                    } else {
                        reject(new Error(`tar exited with code ${code}`));
                    }
                });

                tarProc.on("error", reject);
            });
        } else if (format === "gzip" || format === "unknown") {
            core.info(`Detected gzip compression, extracting with gzip`);

            await promisify(pipeline)(
                fs.createReadStream(tempFile),
                createGunzip(),
                tar.extract({ cwd: directory || "." })
            );
        } else {
            // zstd format but zstd not available
            throw new Error(
                `Cache is zstd compressed but zstd is not available on this runner`
            );
        }

        // Clean up temp file
        fs.unlinkSync(tempFile);

        core.info(
            `Successfully downloaded and extracted cache from S3 bucket ${bucketName} with key ${key} to ${destinationPath}`
        );
    }
}

async function isTarFile(filePath: string): Promise<boolean> {
    const fd = await fs.promises.open(filePath, "r");
    const buffer = Buffer.alloc(512); // Read the first 512 bytes (tar header size)

    await fd.read(buffer, 0, 512, 0);
    await fd.close();

    // The magic number "ustar" is located at byte positions 257-262
    const tarMagic = buffer.toString("ascii", 257, 262);

    return tarMagic === "ustar";
}

async function isTarGz(filePath: string): Promise<boolean> {
    const fd = await fs.promises.open(filePath, "r");
    const buffer = Buffer.alloc(262);
    await fd.read(buffer, 0, 262, 0);
    await fd.close();
    const isGzip = buffer[0] === 0x1f && buffer[1] === 0x8b;
    const isTar = buffer.toString("ascii", 257, 262) === "ustar";
    return isGzip && isTar;
}
