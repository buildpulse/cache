import { spawnSync } from "child_process";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

import { extractArchive, packCachePath } from "../src/s3Client";

// The property that matters: whatever save uploads for a path, restore puts
// back at that same path. A single file used to be uploaded as a bare
// compressed blob that restore could never extract, so these run the real
// save format through the real restore extraction, with no S3 in between.

// The zstd archive is written with GNU tar flags, so that variant only runs
// where GNU tar and zstd are both present (the Linux runners).
const gnuTarWithZstd =
    (
        spawnSync("tar", ["--version"], { encoding: "utf8" }).stdout || ""
    ).includes("GNU tar") && spawnSync("zstd", ["--version"]).status === 0;

const compressions: [string, boolean][] = [["gzip", false]];
if (gnuTarWithZstd) {
    compressions.push(["zstd", true]);
}

let work: string;

beforeEach(() => {
    work = fs.mkdtempSync(path.join(os.tmpdir(), "cache-archive-"));
});

afterEach(() => {
    fs.rmSync(work, { recursive: true, force: true });
});

async function saveThenRestore(
    cachePath: string,
    useZstd: boolean
): Promise<void> {
    const archive = await packCachePath(
        cachePath,
        `key-${path.basename(work)}`,
        useZstd
    );
    try {
        fs.rmSync(cachePath, { recursive: true, force: true });
        await extractArchive(archive, cachePath);
    } finally {
        fs.rmSync(archive, { force: true });
    }
}

describe.each(compressions)("cache archive round trip (%s)", (_, useZstd) => {
    test("a single file comes back at the same path with the same bytes", async () => {
        const file = path.join(work, "deps", "lockfile.bin");
        fs.mkdirSync(path.dirname(file), { recursive: true });
        const content = Buffer.from(
            Array.from({ length: 70000 }, (_v, i) => i % 251)
        );
        fs.writeFileSync(file, content);

        await saveThenRestore(file, useZstd);

        expect(fs.existsSync(file)).toBe(true);
        expect(fs.readFileSync(file).equals(content)).toBe(true);
    });

    test("a directory comes back at the same path with its contents", async () => {
        const dir = path.join(work, "node_modules");
        fs.mkdirSync(path.join(dir, "pkg"), { recursive: true });
        fs.writeFileSync(path.join(dir, "pkg", "index.js"), "exports.a = 1;\n");

        await saveThenRestore(dir, useZstd);

        expect(fs.readFileSync(path.join(dir, "pkg", "index.js"), "utf8")).toBe(
            "exports.a = 1;\n"
        );
    });
});
