import * as core from "@actions/core";

import { Events, RefKey } from "../src/constants";
import { restoreRun } from "../src/restoreImpl";
import * as s3 from "../src/s3Client";
import * as actionUtils from "../src/utils/actionUtils";
import * as testUtils from "../src/utils/testUtils";

// restoreRun is the main step of the combined action: it hands the keys to the
// post step through state. restoreImpl.test.ts covers the restore itself.
jest.mock("../src/s3Client", () => ({
    downloadFromS3: jest.fn(),
    initializeS3Client: jest.fn(),
    resolvedCredentialSource: jest.fn(() => "test credentials"),
    s3Client: { send: jest.fn() }
}));

const download = s3.downloadFromS3 as jest.MockedFunction<
    typeof s3.downloadFromS3
>;

const path = "bp-missing-cache-dir";
const key = "node-test";

let failedMock: jest.SpyInstance;
let outputMock: jest.SpyInstance;
let stateMock: jest.SpyInstance;

beforeEach(() => {
    jest.restoreAllMocks();
    download.mockReset();
    process.env[Events.Key] = Events.Push;
    process.env[RefKey] = "refs/heads/feature-branch";
    process.env.BP_CACHE_S3_BUCKET = "test-bucket";

    jest.spyOn(actionUtils, "isCacheFeatureAvailable").mockReturnValue(true);
    failedMock = jest.spyOn(core, "setFailed").mockImplementation();
    outputMock = jest.spyOn(core, "setOutput").mockImplementation();
    stateMock = jest.spyOn(core, "saveState").mockImplementation();
    jest.spyOn(core, "info").mockImplementation();
});

afterEach(() => {
    testUtils.clearInputs();
    delete process.env[Events.Key];
    delete process.env[RefKey];
    delete process.env.BP_CACHE_S3_BUCKET;
});

test("restoreRun leaves the primary and matched keys in state for the post step", async () => {
    testUtils.setInputs({ path, key });
    download.mockResolvedValue(undefined);

    await restoreRun();

    expect(stateMock).toHaveBeenCalledWith("CACHE_KEY", key);
    expect(stateMock).toHaveBeenCalledWith("CACHE_RESULT", `${key}/${path}`);
    expect(outputMock).not.toHaveBeenCalledWith(
        "cache-primary-key",
        expect.anything()
    );
    expect(failedMock).not.toHaveBeenCalled();
});

test("restoreRun with earlyExit exits 0 after a restore", async () => {
    testUtils.setInputs({ path, key });
    download.mockResolvedValue(undefined);
    const exitMock = jest
        .spyOn(process, "exit")
        .mockImplementation((() => undefined) as never);

    await restoreRun(true);

    expect(exitMock).toHaveBeenCalledWith(0);
    expect(failedMock).not.toHaveBeenCalled();
});
