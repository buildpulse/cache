import { S3Client, PutObjectCommand, GetObjectCommand, CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand, AbortMultipartUploadCommand } from "@aws-sdk/client-s3";
import { Readable } from "stream";
import * as core from "@actions/core";
import * as fs from "fs";
import * as path from "path";

let s3Client: S3Client | null = null;

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

export async function uploadToS3(bucketName: string, key: string, filePath: string): Promise<void> {
    const client = initializeS3Client();
    const fileSize = fs.statSync(filePath).size;
    const chunkSize = 5 * 1024 * 1024; // 5MB chunk size

    if (fileSize <= chunkSize) {
        // Small file, use simple upload
        const fileContent = fs.readFileSync(filePath);
        const command = new PutObjectCommand({
            Bucket: bucketName,
            Key: key,
            Body: fileContent
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
            const fileStream = fs.createReadStream(filePath);

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

    core.info(`Successfully uploaded ${filePath} to S3 bucket ${bucketName} with key ${key}`);
}

export async function downloadFromS3(bucketName: string, key: string, destinationPath: string): Promise<void> {
    const client = initializeS3Client();
    const command = new GetObjectCommand({
        Bucket: bucketName,
        Key: key
    });

    try {
        const { Body } = await client.send(command);

        if (Body instanceof Readable) {
            const directory = path.dirname(destinationPath);
            if (!fs.existsSync(directory)) {
                fs.mkdirSync(directory, { recursive: true });
            }

            const writeStream = fs.createWriteStream(destinationPath);
            await new Promise((resolve, reject) => {
                Body.pipe(writeStream)
                    .on("error", reject)
                    .on("finish", resolve);
            });

            core.info(`Successfully downloaded S3 object ${key} from bucket ${bucketName} to ${destinationPath}`);
        } else {
            throw new Error("Invalid response body from S3");
        }
    } catch (error) {
        throw new Error(`Failed to download file from S3: ${error}`);
    }
}
