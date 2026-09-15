import * as core from "@actions/core";

import { Events, RefKey } from "../src/constants";
import { restoreOnlyRun } from "../src/restoreImpl";
import * as s3 from "../src/s3Client";
import * as actionUtils from "../src/utils/actionUtils";
import * as testUtils from "../src/utils/testUtils";

// restoreOnlyRun backs the standalone restore action, which has no post step,
// so the keys are reported as outputs instead of state.
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

let outputMock: jest.SpyInstance;
let stateMock: jest.SpyInstance;

beforeEach(() => {
    jest.restoreAllMocks();
    download.mockReset();
    process.env[Events.Key] = Events.Push;
    process.env[RefKey] = "refs/heads/feature-branch";
    process.env.BP_CACHE_S3_BUCKET = "test-bucket";

    jest.spyOn(actionUtils, "isCacheFeatureAvailable").mockReturnValue(true);
    jest.spyOn(core, "setFailed").mockImplementation();
    jest.spyOn(core, "info").mockImplementation();
    outputMock = jest.spyOn(core, "setOutput").mockImplementation();
    stateMock = jest.spyOn(core, "saveState").mockImplementation();
});

afterEach(() => {
    testUtils.clearInputs();
    delete process.env[Events.Key];
    delete process.env[RefKey];
    delete process.env.BP_CACHE_S3_BUCKET;
});

test("a hit reports the primary and matched keys as outputs", async () => {
    testUtils.setInputs({ path, key });
    download.mockResolvedValue(undefined);

    await restoreOnlyRun();

    expect(outputMock).toHaveBeenCalledWith("cache-primary-key", key);
    expect(outputMock).toHaveBeenCalledWith(
        "cache-matched-key",
        `${key}/${path}`
    );
    expect(outputMock).toHaveBeenCalledWith("cache-hit", "true");
    expect(stateMock).not.toHaveBeenCalled();
});

test("a miss reports the primary key and no matched key", async () => {
    testUtils.setInputs({ path, key });
    download.mockRejectedValue(
        Object.assign(new Error("missing"), { name: "NoSuchKey" })
    );

    await restoreOnlyRun();

    expect(outputMock).toHaveBeenCalledWith("cache-primary-key", key);
    expect(outputMock).not.toHaveBeenCalledWith(
        "cache-matched-key",
        expect.anything()
    );
    expect(outputMock).toHaveBeenCalledWith("cache-hit", "false");
});
