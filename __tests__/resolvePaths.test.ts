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
    expect(
        resolved.some(p => p.includes(path.join("outside", "archives")))
    ).toBe(true);
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

// The restore path takes a SECOND route to the filesystem. On restore the
// target usually does not exist yet, so the globber matches nothing and
// restoreImpl falls back to the raw `path` inputs — which are then used as the
// tar extraction destination. An unexpanded "~/x" makes path.dirname() return
// "~", so the archive is written to a directory literally named "~" under the
// CWD while the action still reports cache-hit=true. Caught by an out-of-
// workspace e2e; resolvePaths' own expansion does not cover this route.
describe("expandHome", () => {
    it("expands a bare ~ to the home directory", () => {
        expect(actionUtils.expandHome("~")).toBe(os.homedir());
    });

    it("expands a leading ~/ so dirname() yields a real parent", () => {
        const expanded = actionUtils.expandHome("~/.cache/Cypress");
        expect(expanded).toBe(path.join(os.homedir(), ".cache", "Cypress"));
        // The actual defect: dirname of the raw pattern is "~".
        expect(path.dirname(expanded)).not.toBe("~");
        expect(path.isAbsolute(expanded)).toBe(true);
    });

    it("leaves absolute and relative paths untouched", () => {
        expect(actionUtils.expandHome("/var/cache/apt/archives")).toBe(
            "/var/cache/apt/archives"
        );
        expect(actionUtils.expandHome("node_modules")).toBe("node_modules");
    });

    it("does not expand a ~ that is not a home reference", () => {
        // "~foo" is a username reference, not this action's job to resolve.
        expect(actionUtils.expandHome("~foo/bar")).toBe("~foo/bar");
        expect(actionUtils.expandHome("dir/~/x")).toBe("dir/~/x");
    });
});

// This is the branch the out-of-workspace e2e caught: save succeeded, restore
// reported cache-hit=true, and the files landed in "./~/..." instead of $HOME.
// These assertions fail against the pre-fix fallback (`: patterns`).
describe("effectiveCachePaths — the restore fallback", () => {
    it("home-expands the fallback when nothing resolved", () => {
        const out = actionUtils.effectiveCachePaths([], ["~/.cache/Cypress"]);
        expect(out).toEqual([path.join(os.homedir(), ".cache", "Cypress")]);
        // The defect in one assertion: dirname is the tar extraction target.
        expect(path.dirname(out[0])).not.toBe("~");
    });

    it("prefers resolved paths when the target already exists", () => {
        const resolved = ["/tmp/already/there"];
        expect(
            actionUtils.effectiveCachePaths(resolved, ["~/ignored"])
        ).toEqual(resolved);
    });

    it("expands every pattern, not just the first", () => {
        const out = actionUtils.effectiveCachePaths(
            [],
            ["~/a", "/var/cache/apt/archives", "~/b"]
        );
        expect(out).toEqual([
            path.join(os.homedir(), "a"),
            "/var/cache/apt/archives",
            path.join(os.homedir(), "b")
        ]);
    });
});
