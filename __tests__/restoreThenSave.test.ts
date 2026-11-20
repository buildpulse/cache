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

const key = "go-build-linux-abc123";
const restoreKey = "go-build-linux-";

let state: Record<string, string>;

beforeEach(() => {
    jest.restoreAllMocks();
    download.mockReset();
    upload.mockReset();
    resetReportState();
    state = {};

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
        testUtils.setInputs({
            path: __dirname,
            key,
            restoreKeys: [restoreKey]
        });
        download.mockResolvedValue(undefined);

        await restoreImpl(new StateProvider());
        await saveImpl(new StateProvider());

        expect(download).toHaveBeenCalledTimes(1);
        expect(upload).not.toHaveBeenCalled();
    });

    test("a restore-key hit is uploaded under the primary key", async () => {
        testUtils.setInputs({
            path: __dirname,
            key,
            restoreKeys: [restoreKey]
        });
        download
            .mockRejectedValueOnce(
                Object.assign(new Error("NoSuchKey"), { name: "NoSuchKey" })
            )
            .mockResolvedValueOnce(undefined);

        await restoreImpl(new StateProvider());
        await saveImpl(new StateProvider());

        expect(upload).toHaveBeenCalledTimes(1);
        const objectKey = `${key}/__tests__`;
        expect(upload.mock.calls[0][1]).toBe(
            prefix ? `org-acme-1234/${objectKey}` : objectKey
        );
    });

    test("a miss is uploaded under the primary key", async () => {
        testUtils.setInputs({ path: __dirname, key });
        download.mockRejectedValue(
            Object.assign(new Error("NoSuchKey"), { name: "NoSuchKey" })
        );

        await restoreImpl(new StateProvider());
        await saveImpl(new StateProvider());

        expect(upload).toHaveBeenCalledTimes(1);
    });
});
