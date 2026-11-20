import * as core from "@actions/core";

import { resetReportState } from "../src/cacheErrors";
import { Events, RefKey } from "../src/constants";
import { restoreImpl } from "../src/restoreImpl";
import * as s3 from "../src/s3Client";
import { saveImpl } from "../src/saveImpl";
import { StateProvider } from "../src/stateProvider";
import * as actionUtils from "../src/utils/actionUtils";
import * as testUtils from "../src/utils/testUtils";

// The restore step and the post step talk only through action state. Each has
// its own tests with hand-written state, and those agreed with each other while
// production did not: restore stored the S3 object key as the matched key, save
// compared it with the user's key, and every exact hit re-uploaded the whole
// cache (rclone bench 2026-11-19: 70-155 s of "Post" per cache step). These
// tests run the real restore, carry its state into the real save, and assert
// the end-to-end rule.
jest.mock("../src/s3Client", () => ({
    downloadFromS3: jest.fn(),
    uploadToS3: jest.fn(),
    initializeS3Client: jest.fn(),
    resolvedCredentialSource: jest.fn(() => "test credentials"),
    s3Client: { send: jest.fn() }
}));

const download = s3.downloadFromS3 as jest.MockedFunction<
    typeof s3.downloadFromS3
>;
const upload = s3.uploadToS3 as jest.MockedFunction<typeof s3.uploadToS3>;

// A fake bucket: download succeeds only for objects that were "saved".
let store: Set<string>;
const noSuchKey = (): Error =>
    Object.assign(new Error("NoSuchKey"), { name: "NoSuchKey" });
const useStore = (): void => {
    download.mockImplementation(async (_b, objectKey) => {
        if (!store.has(objectKey)) throw noSuchKey();
    });
    upload.mockImplementation(async (_b, objectKey) => {
        store.add(objectKey);
    });
};
const outputs = (): Record<string, string> =>
    Object.fromEntries(
        (core.setOutput as jest.Mock).mock.calls.map(([k, v]) => [k, String(v)])
    );

const key = "go-build-linux-abc123";
const restoreKey = "go-build-linux-";

let state: Record<string, string>;

beforeEach(() => {
    jest.restoreAllMocks();
    download.mockReset();
    upload.mockReset();
    resetReportState();
    state = {};
    store = new Set();
    useStore();

    process.env[Events.Key] = Events.Push;
    process.env[RefKey] = "refs/heads/main";
    process.env.BP_CACHE_S3_BUCKET = "test-bucket";

    jest.spyOn(actionUtils, "isCacheFeatureAvailable").mockReturnValue(true);
    jest.spyOn(core, "saveState").mockImplementation((name, value) => {
        state[name] = String(value);
    });
    jest.spyOn(core, "getState").mockImplementation(name => state[name] || "");
    for (const fn of ["info", "warning", "error", "setOutput"] as const) {
        jest.spyOn(core, fn).mockImplementation();
    }
    jest.spyOn(core, "setFailed").mockImplementation(msg => {
        throw new Error(`setFailed: ${msg}`);
    });
});

afterEach(() => {
    testUtils.clearInputs();
    delete process.env[Events.Key];
    delete process.env[RefKey];
    delete process.env.BP_CACHE_S3_BUCKET;
    delete process.env.BP_CACHE_KEY_PREFIX;
});

// Production always sets a per-org prefix, so cover both shapes.
describe.each([
    ["no key prefix", undefined],
    ["a per-org key prefix", "org-acme-1234/"]
])("with %s", (_label, prefix) => {
    beforeEach(() => {
        if (prefix) process.env.BP_CACHE_KEY_PREFIX = prefix;
    });

    test("an exact hit is not uploaded again", async () => {
        store.add(actionUtils.cacheObjectKey(key, __dirname));
        testUtils.setInputs({
            path: __dirname,
            key,
            restoreKeys: [restoreKey]
        });

        await restoreImpl(new StateProvider());
        await saveImpl(new StateProvider());

        expect(download).toHaveBeenCalledTimes(1);
        expect(upload).not.toHaveBeenCalled();
    });

    test("a restore-key hit is uploaded under the primary key", async () => {
        store.add(actionUtils.cacheObjectKey(restoreKey, __dirname));
        testUtils.setInputs({
            path: __dirname,
            key,
            restoreKeys: [restoreKey]
        });

        await restoreImpl(new StateProvider());
        await saveImpl(new StateProvider());

        expect(upload).toHaveBeenCalledTimes(1);
        const objectKey = upload.mock.calls[0][1];
        expect(objectKey).toMatch(
            new RegExp(`^${prefix ?? ""}${key}/__tests__\\.[0-9a-f]{12}$`)
        );
    });

    test("a miss is uploaded under the primary key", async () => {
        testUtils.setInputs({ path: __dirname, key });

        await restoreImpl(new StateProvider());
        await saveImpl(new StateProvider());

        expect(upload).toHaveBeenCalledTimes(1);
    });
});

// Multi-path caches. actions/setup-go creates ~/.cache/go-build before the
// cache step runs, and restore used to pull only the paths that already
// existed, so `path: [go-build, golangci-lint]` restored go-build alone,
// reported an exact hit, and never saved again: golangci-lint ran cold on
// every job (rclone lint 2026-11-20: 15 s per run instead of about 1 s).
describe("a cache with several paths", () => {
    const absent = `${__dirname}/__bp_not_created_yet__`;
    const paths = `${__dirname}\n${absent}`;

    test("pulls every path, including one that does not exist yet", async () => {
        store.add(actionUtils.cacheObjectKey(key, __dirname));
        store.add(actionUtils.cacheObjectKey(key, absent));
        testUtils.setInputs({ path: paths, key });

        await restoreImpl(new StateProvider());
        await saveImpl(new StateProvider());

        expect(download.mock.calls.map(c => c[2])).toEqual([__dirname, absent]);
        expect(outputs()["cache-hit"]).toBe("true");
        expect(upload).not.toHaveBeenCalled();
    });

    test("an entry missing a later path restores the rest, is not a hit, and is saved", async () => {
        store.add(actionUtils.cacheObjectKey(key, __dirname));
        testUtils.setInputs({ path: paths, key });

        await restoreImpl(new StateProvider());
        await saveImpl(new StateProvider());

        expect(outputs()["cache-hit"]).toBe("false");
        expect(state["CACHE_RESULT"]).toBe(key);
        // Save runs although the key matched exactly, so the entry heals.
        expect(upload).toHaveBeenCalled();
    });

    test("a missing first path is still a miss for that key", async () => {
        store.add(actionUtils.cacheObjectKey(key, absent));
        store.add(actionUtils.cacheObjectKey(restoreKey, __dirname));
        store.add(actionUtils.cacheObjectKey(restoreKey, absent));
        testUtils.setInputs({ path: paths, key, restoreKeys: [restoreKey] });

        await restoreImpl(new StateProvider());

        expect(state["CACHE_RESULT"]).toBe(restoreKey);
        expect(outputs()["cache-hit"]).toBe("false");
    });

    test("a real error on a later path is not treated as a partial hit", async () => {
        store.add(actionUtils.cacheObjectKey(key, __dirname));
        download.mockImplementation(async (_b, objectKey) => {
            if (objectKey === actionUtils.cacheObjectKey(key, __dirname))
                return;
            throw Object.assign(new Error("boom"), { name: "InternalError" });
        });
        testUtils.setInputs({ path: paths, key });

        await restoreImpl(new StateProvider());

        expect(state["CACHE_RESULT"]).toBeUndefined();
        expect(state["CACHE_PARTIAL"]).toBeUndefined();
        expect(outputs()["cache-hit"]).toBe("false");
    });
});

// Two paths with the same last segment used to share one S3 object
// (key/basename): the second upload overwrote the first, and restore put that
// one archive into both places.
describe("paths that share a basename", () => {
    const a = `${__dirname}/__fixtures__/a/node_modules`;
    const b = `${__dirname}/__fixtures__/b/node_modules`;

    test("get different object names", () => {
        expect(actionUtils.cacheObjectKey(key, a)).not.toBe(
            actionUtils.cacheObjectKey(key, b)
        );
        // Same path spelled differently is the same object.
        expect(actionUtils.cacheObjectKey(key, `${a}/`)).toBe(
            actionUtils.cacheObjectKey(key, a)
        );
    });

    test("a legacy entry where they collided restores, is not a hit, and is saved under the new names", async () => {
        // Pre-v7.0.5 layout: one object for both paths.
        store.add(actionUtils.legacyCacheObjectKey(key, a));
        testUtils.setInputs({ path: `${a}\n${b}`, key });

        await restoreImpl(new StateProvider());

        expect(download.mock.calls.map(c => c[2])).toEqual([a, a, b, b]);
        expect(outputs()["cache-hit"]).toBe("false");
        expect(state["CACHE_PARTIAL"]).toBe("true");
        expect(state["CACHE_RESULT"]).toBe(key);
    });

    test("a legacy entry with distinct basenames is still an exact hit", async () => {
        store.add(actionUtils.legacyCacheObjectKey(key, __dirname));
        testUtils.setInputs({ path: __dirname, key });

        await restoreImpl(new StateProvider());
        await saveImpl(new StateProvider());

        expect(outputs()["cache-hit"]).toBe("true");
        expect(upload).not.toHaveBeenCalled();
    });
});
