import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Readable } from "stream";

import {
    DEFAULT_DOWNLOAD_CONCURRENCY,
    downloadConcurrency,
    downloadRanges
} from "../src/s3Client";

jest.mock("@actions/core");

// A stand-in for S3Client.send that serves GetObject ranges from a buffer the
// way S3 does: 206 with Content-Range for a satisfiable range, 416
// InvalidRange for a range of an empty object, 412 when If-Match differs.
function fakeS3(
    data: Buffer,
    opts: {
        etag?: string;
        failOnce?: Set<number>; // range starts that fail once with a 500
        changeAfterFirst?: boolean; // the ETag changes after the first range
        missing?: boolean;
    } = {}
) {
    const calls: { range?: string; ifMatch?: string }[] = [];
    let etag = opts.etag ?? '"v1"';
    const failOnce = new Set(opts.failOnce ?? []);
    const send = jest.fn(async (cmd: any) => {
        const input = cmd.input;
        calls.push({ range: input.Range, ifMatch: input.IfMatch });
        if (opts.missing) {
            throw Object.assign(
                new Error("The specified key does not exist."),
                {
                    name: "NoSuchKey",
                    $metadata: { httpStatusCode: 404 }
                }
            );
        }
        if (input.IfMatch !== undefined && input.IfMatch !== etag) {
            throw Object.assign(
                new Error(
                    "At least one of the pre-conditions you specified did not hold"
                ),
                {
                    name: "PreconditionFailed",
                    $metadata: { httpStatusCode: 412 }
                }
            );
        }
        if (!input.Range) {
            return { Body: Readable.from([data]), ETag: etag };
        }
        if (data.length === 0) {
            throw Object.assign(
                new Error("The requested range is not satisfiable"),
                {
                    name: "InvalidRange",
                    $metadata: { httpStatusCode: 416 }
                }
            );
        }
        const m = /^bytes=(\d+)-(\d+)$/.exec(input.Range)!;
        const start = Number(m[1]);
        const end = Math.min(Number(m[2]), data.length - 1);
        if (failOnce.delete(start)) {
            throw Object.assign(
                new Error("We encountered an internal error."),
                {
                    name: "InternalError",
                    $metadata: { httpStatusCode: 500 }
                }
            );
        }
        const body = Readable.from([data.subarray(start, end + 1)]);
        const res = {
            Body: body,
            ETag: etag,
            ContentRange: `bytes ${start}-${end}/${data.length}`
        };
        if (opts.changeAfterFirst && start === 0) {
            etag = '"v2"';
        }
        return res;
    });
    return { client: { send } as any, calls };
}

function tmpFile(): string {
    return path.join(
        fs.mkdtempSync(path.join(os.tmpdir(), "s3dl-")),
        "archive"
    );
}

function bytes(n: number): Buffer {
    const b = Buffer.alloc(n);
    for (let i = 0; i < n; i++) b[i] = (i * 31 + 7) & 0xff;
    return b;
}

describe("downloadRanges", () => {
    test("reassembles a multi-range object in order, with If-Match on every later range", async () => {
        const data = bytes(1000);
        const { client, calls } = fakeS3(data);
        const file = tmpFile();
        await downloadRanges(client, "b", "k", file, 4, 64);
        expect(fs.readFileSync(file).equals(data)).toBe(true);
        expect(calls).toHaveLength(Math.ceil(1000 / 64));
        expect(calls[0]).toEqual({ range: "bytes=0-63", ifMatch: undefined });
        for (const c of calls.slice(1)) expect(c.ifMatch).toBe('"v1"');
    });

    test("an object smaller than one range takes one request", async () => {
        const data = bytes(10);
        const { client, calls } = fakeS3(data);
        const file = tmpFile();
        await downloadRanges(client, "b", "k", file, 16, 64);
        expect(fs.readFileSync(file).equals(data)).toBe(true);
        expect(calls).toHaveLength(1);
    });

    test("an exact multiple of the range size has no empty trailing range", async () => {
        const data = bytes(256);
        const { client, calls } = fakeS3(data);
        const file = tmpFile();
        await downloadRanges(client, "b", "k", file, 3, 64);
        expect(fs.readFileSync(file).equals(data)).toBe(true);
        expect(calls.map(c => c.range)).toEqual([
            "bytes=0-63",
            "bytes=64-127",
            "bytes=128-191",
            "bytes=192-255"
        ]);
    });

    test("an empty object (416 InvalidRange) falls back to a plain GET", async () => {
        const { client, calls } = fakeS3(Buffer.alloc(0));
        const file = tmpFile();
        await downloadRanges(client, "b", "k", file, 16, 64);
        expect(fs.readFileSync(file)).toHaveLength(0);
        expect(calls).toEqual([
            { range: "bytes=0-63", ifMatch: undefined },
            { range: undefined, ifMatch: undefined }
        ]);
    });

    test("a missing key reaches the caller as NoSuchKey (a cache miss, not an error)", async () => {
        const { client } = fakeS3(bytes(100), { missing: true });
        await expect(
            downloadRanges(client, "b", "k", tmpFile(), 16, 64)
        ).rejects.toMatchObject({ name: "NoSuchKey" });
    });

    test("a transient 500 on one range is retried", async () => {
        const data = bytes(500);
        const { client, calls } = fakeS3(data, { failOnce: new Set([128]) });
        const file = tmpFile();
        await downloadRanges(client, "b", "k", file, 4, 64);
        expect(fs.readFileSync(file).equals(data)).toBe(true);
        expect(calls.filter(c => c.range === "bytes=128-191")).toHaveLength(2);
    });

    test("an object overwritten mid-download fails with 412 and is not retried", async () => {
        const { client, calls } = fakeS3(bytes(500), {
            changeAfterFirst: true
        });
        await expect(
            downloadRanges(client, "b", "k", tmpFile(), 1, 64)
        ).rejects.toMatchObject({ $metadata: { httpStatusCode: 412 } });
        // first range, then one refused range: concurrency 1 stops at the first failure
        expect(calls).toHaveLength(2);
    });
});

describe("downloadConcurrency", () => {
    const saved = process.env.BP_CACHE_DOWNLOAD_CONCURRENCY;
    afterEach(() => {
        if (saved === undefined)
            delete process.env.BP_CACHE_DOWNLOAD_CONCURRENCY;
        else process.env.BP_CACHE_DOWNLOAD_CONCURRENCY = saved;
    });

    test.each([
        [undefined, DEFAULT_DOWNLOAD_CONCURRENCY],
        ["", DEFAULT_DOWNLOAD_CONCURRENCY],
        ["1", 1],
        ["32", 32],
        ["500", 64],
        ["0", DEFAULT_DOWNLOAD_CONCURRENCY],
        ["abc", DEFAULT_DOWNLOAD_CONCURRENCY]
    ])("%p -> %p", (raw, want) => {
        if (raw === undefined) delete process.env.BP_CACHE_DOWNLOAD_CONCURRENCY;
        else process.env.BP_CACHE_DOWNLOAD_CONCURRENCY = raw;
        expect(downloadConcurrency()).toBe(want);
    });
});
