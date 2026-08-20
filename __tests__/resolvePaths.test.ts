import * as fs from "fs";
import * as os from "os";
import * as path from "path";

import * as actionUtils from "../src/utils/actionUtils";

// resolvePaths used to drop every resolved path that fell outside
// GITHUB_WORKSPACE. Paths inside the repo (node_modules, public/packs-test)
// kept working, so the breakage read as "that cache never hits" rather than
// "save is a no-op" — and it silently disabled every system/home-directory
// cache entry for months. The two that matter in the wild are
// /var/cache/apt/archives and ~/.cache/Cypress.
//
// These tests fail against the old implementation and pass against the new one.

let tmpRoot: string;
let workspace: string;
let outside: string;
const originalWorkspace = process.env["GITHUB_WORKSPACE"];

beforeEach(() => {
    tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "bp-resolve-"));
    workspace = path.join(tmpRoot, "workspace");
    outside = path.join(tmpRoot, "outside");
    fs.mkdirSync(path.join(workspace, "node_modules"), { recursive: true });
    fs.writeFileSync(path.join(workspace, "node_modules", "a.txt"), "inside");
    fs.mkdirSync(path.join(outside, "archives"), { recursive: true });
    fs.writeFileSync(path.join(outside, "archives", "b.deb"), "outside");
    process.env["GITHUB_WORKSPACE"] = workspace;
});

afterEach(() => {
    if (originalWorkspace === undefined) {
        delete process.env["GITHUB_WORKSPACE"];
    } else {
        process.env["GITHUB_WORKSPACE"] = originalWorkspace;
    }
    fs.rmSync(tmpRoot, { recursive: true, force: true });
});

test("resolves a path OUTSIDE the workspace — the apt/Cypress case", async () => {
    const target = path.join(outside, "archives");
    const resolved = await actionUtils.resolvePaths([target]);

    // The whole bug: this used to come back empty, saveImpl logged
    // "No files found matching the cache path patterns" and returned green.
    expect(resolved.length).toBeGreaterThan(0);
    expect(resolved.some(p => p.includes(path.join("outside", "archives")))).toBe(
        true
    );
});

test("still resolves a path inside the workspace", async () => {
    const resolved = await actionUtils.resolvePaths([
        path.join(workspace, "node_modules")
    ]);
    expect(resolved.length).toBeGreaterThan(0);
});

test("expands a leading ~ to the home directory", async () => {
    const marker = `.bp-cache-tilde-${process.pid}`;
    const homeDir = path.join(os.homedir(), marker);
    fs.mkdirSync(homeDir, { recursive: true });
    fs.writeFileSync(path.join(homeDir, "c.txt"), "home");
    try {
        const resolved = await actionUtils.resolvePaths([`~/${marker}`]);
        // A literal "~" directory never exists, so without expansion this
        // resolves to nothing and the cache silently no-ops.
        expect(resolved.length).toBeGreaterThan(0);
        expect(resolved.some(p => p.startsWith(os.homedir()))).toBe(true);
    } finally {
        fs.rmSync(homeDir, { recursive: true, force: true });
    }
});

test("a pattern matching nothing still resolves to empty", async () => {
    const resolved = await actionUtils.resolvePaths([
        path.join(tmpRoot, "does-not-exist-anywhere")
    ]);
    expect(resolved).toEqual([]);
});
