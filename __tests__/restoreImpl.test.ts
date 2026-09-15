import * as core from "@actions/core";

import { resetReportState } from "../src/cacheErrors";
import { Events, Inputs, RefKey } from "../src/constants";
import { restoreImpl } from "../src/restoreImpl";
import * as s3 from "../src/s3Client";
import { StateProvider } from "../src/stateProvider";
import * as actionUtils from "../src/utils/actionUtils";
import * as testUtils from "../src/utils/testUtils";

// The S3 client is the boundary. Everything above it -- which keys are tried
// and in what order, what counts as a hit, and when a failure is quiet, loud or
// fatal -- runs for real.
jest.mock("../src/s3Client", () => ({
    downloadFromS3: jest.fn(),
    initializeS3Client: jest.fn(),
    resolvedCredentialSource: jest.fn(() => "test credentials"),
    s3Client: { send: jest.fn() }
}));

const download = s3.downloadFromS3 as jest.MockedFunction<
    typeof s3.downloadFromS3
>;
const headObject = s3.s3Client.send as unknown as jest.Mock;

const bucket = "test-bucket";
// Does not exist, so path resolution falls back to the raw input, as it does
// on a real restore where the target has not been created yet.
const path = "bp-missing-cache-dir";
const key = "node-test";
const restoreKey = "node-";

function awsError(name: string): Error {
    return Object.assign(new Error(`${name} from S3`), { name });
}

let failedMock: jest.SpyInstance;
let warningMock: jest.SpyInstance;
let errorMock: jest.SpyInstance;
let infoMock: jest.SpyInstance;
let outputMock: jest.SpyInstance;
let stateMock: jest.SpyInstance;

beforeEach(() => {
    jest.restoreAllMocks();
    download.mockReset();
    headObject.mockReset();
    resetReportState();

    process.env[Events.Key] = Events.Push;
    process.env[RefKey] = "refs/heads/feature-branch";
    process.env.BP_CACHE_S3_BUCKET = bucket;

    jest.spyOn(actionUtils, "isCacheFeatureAvailable").mockReturnValue(true);
    failedMock = jest.spyOn(core, "setFailed").mockImplementation();
    warningMock = jest.spyOn(core, "warning").mockImplementation();
    errorMock = jest.spyOn(core, "error").mockImplementation();
    infoMock = jest.spyOn(core, "info").mockImplementation();
    outputMock = jest.spyOn(core, "setOutput").mockImplementation();
    stateMock = jest.spyOn(core, "saveState").mockImplementation();
});

afterEach(() => {
    testUtils.clearInputs();
    delete process.env[Events.Key];
    delete process.env[RefKey];
    delete process.env.BP_CACHE_S3_BUCKET;
    delete process.env.BP_CACHE_KEY_PREFIX;
});

test("an event without a ref warns and restores nothing", async () => {
    process.env[Events.Key] = "commit_comment";
    delete process.env[RefKey];

    await restoreImpl(new StateProvider());

    expect(warningMock).toHaveBeenCalledWith(
        "Event Validation Error: The event type commit_comment is not supported because it's not tied to a branch or tag ref."
    );
    expect(download).not.toHaveBeenCalled();
    expect(failedMock).not.toHaveBeenCalled();
});

test("an unconfigured cache restores nothing and reports no hit", async () => {
    jest.spyOn(actionUtils, "isCacheFeatureAvailable").mockReturnValue(false);
    testUtils.setInputs({ path, key });

    await restoreImpl(new StateProvider());

    expect(download).not.toHaveBeenCalled();
    expect(outputMock).toHaveBeenCalledWith("cache-hit", "false");
    expect(failedMock).not.toHaveBeenCalled();
});

test("a missing key fails the step before anything is fetched", async () => {
    testUtils.setInput(Inputs.Path, path);

    await restoreImpl(new StateProvider());

    expect(failedMock).toHaveBeenCalledWith(
        "Input required and not supplied: key"
    );
    expect(download).not.toHaveBeenCalled();
});

test("a missing path fails the step before anything is fetched", async () => {
    testUtils.setInput(Inputs.Key, key);

    await restoreImpl(new StateProvider());

    expect(failedMock).toHaveBeenCalledWith(
        "Input required and not supplied: path"
    );
    expect(download).not.toHaveBeenCalled();
});

test("a hit on the primary key restores it and is an exact hit", async () => {
    testUtils.setInputs({ path, key, restoreKeys: [restoreKey] });
    download.mockResolvedValue(undefined);

    await restoreImpl(new StateProvider());

    expect(download).toHaveBeenCalledTimes(1);
    expect(download).toHaveBeenCalledWith(bucket, `${key}/${path}`, path);
    expect(outputMock).toHaveBeenCalledWith("cache-hit", "true");
    expect(stateMock).toHaveBeenCalledWith("CACHE_KEY", key);
    expect(stateMock).toHaveBeenCalledWith("CACHE_RESULT", `${key}/${path}`);
    expect(failedMock).not.toHaveBeenCalled();
});

test("a miss on the primary key falls back to the restore key, which is not an exact hit", async () => {
    testUtils.setInputs({ path, key, restoreKeys: [restoreKey] });
    download
        .mockRejectedValueOnce(awsError("NoSuchKey"))
        .mockResolvedValueOnce(undefined);

    await restoreImpl(new StateProvider());

    expect(download.mock.calls.map(call => call[1])).toEqual([
        `${key}/${path}`,
        `${restoreKey}/${path}`
    ]);
    expect(outputMock).toHaveBeenCalledWith("cache-hit", "false");
    expect(stateMock).toHaveBeenCalledWith(
        "CACHE_RESULT",
        `${restoreKey}/${path}`
    );
    expect(warningMock).not.toHaveBeenCalled();
    expect(errorMock).not.toHaveBeenCalled();
    expect(failedMock).not.toHaveBeenCalled();
});

test("nothing cached under any key is a quiet miss", async () => {
    testUtils.setInputs({ path, key, restoreKeys: [restoreKey] });
    download.mockRejectedValue(awsError("NoSuchKey"));

    await restoreImpl(new StateProvider());

    expect(download).toHaveBeenCalledTimes(2);
    expect(outputMock).toHaveBeenCalledWith("cache-hit", "false");
    expect(infoMock).toHaveBeenCalledWith(
        `Cache not found for input keys: ${key}, ${restoreKey}`
    );
    expect(stateMock).not.toHaveBeenCalledWith(
        "CACHE_RESULT",
        expect.anything()
    );
    expect(warningMock).not.toHaveBeenCalled();
    expect(errorMock).not.toHaveBeenCalled();
    expect(failedMock).not.toHaveBeenCalled();
});

test("fail-on-cache-miss fails the step when nothing is cached", async () => {
    testUtils.setInputs({ path, key, failOnCacheMiss: true });
    download.mockRejectedValue(awsError("NoSuchKey"));

    await restoreImpl(new StateProvider());

    expect(failedMock).toHaveBeenCalledWith(
        `Failed to restore cache entry. Exiting as fail-on-cache-miss is set. Input key: ${key}`
    );
});

test("a denied restore is an error rather than a miss, and keeps the step green by default", async () => {
    testUtils.setInputs({ path, key });
    download.mockRejectedValue(awsError("AccessDenied"));

    await restoreImpl(new StateProvider());

    expect(errorMock).toHaveBeenCalledTimes(1);
    expect(errorMock.mock.calls[0][0]).toContain("was denied");
    expect(outputMock).toHaveBeenCalledWith("cache-error", "auth");
    expect(outputMock).toHaveBeenCalledWith("cache-hit", "false");
    expect(failedMock).not.toHaveBeenCalled();
});

test("on-cache-error: error fails the step on a denied restore", async () => {
    testUtils.setInputs({ path, key });
    testUtils.setInput(Inputs.OnCacheError, "error");
    download.mockRejectedValue(awsError("AccessDenied"));

    await restoreImpl(new StateProvider());

    expect(failedMock).toHaveBeenCalledWith(
        "Cache auth error and on-cache-error is set to error."
    );
});

test("lookup-only reports a hit without downloading when the entry exists", async () => {
    testUtils.setInputs({ path, key, lookupOnly: true });
    headObject.mockResolvedValue({});

    await restoreImpl(new StateProvider());

    expect(download).not.toHaveBeenCalled();
    expect(headObject).toHaveBeenCalledTimes(1);
    expect(headObject.mock.calls[0][0].input).toEqual({
        Bucket: bucket,
        Key: `${key}/${path}`
    });
    expect(outputMock).toHaveBeenCalledWith("cache-hit", "true");
});

test("lookup-only reports no hit when no entry exists under any key", async () => {
    testUtils.setInputs({
        path,
        key,
        restoreKeys: [restoreKey],
        lookupOnly: true
    });
    headObject.mockRejectedValue(awsError("NotFound"));

    await restoreImpl(new StateProvider());

    expect(headObject.mock.calls.map(call => call[0].input.Key)).toEqual([
        `${key}/${path}`,
        `${restoreKey}/${path}`
    ]);
    expect(download).not.toHaveBeenCalled();
    expect(outputMock).not.toHaveBeenCalledWith("cache-hit", "true");
    expect(outputMock).toHaveBeenCalledWith("cache-hit", "false");
    expect(stateMock).not.toHaveBeenCalledWith(
        "CACHE_RESULT",
        expect.anything()
    );
});

test("BP_CACHE_KEY_PREFIX scopes the object key", async () => {
    process.env.BP_CACHE_KEY_PREFIX = "tenant-a/";
    testUtils.setInputs({ path, key });
    download.mockResolvedValue(undefined);

    await restoreImpl(new StateProvider());

    expect(download).toHaveBeenCalledWith(
        bucket,
        `tenant-a/${key}/${path}`,
        path
    );
    expect(outputMock).toHaveBeenCalledWith("cache-hit", "true");
});

test("a failure with earlyExit exits the process with 1", async () => {
    testUtils.setInput(Inputs.Path, path);
    const exitMock = jest
        .spyOn(process, "exit")
        .mockImplementation((() => undefined) as never);

    await restoreImpl(new StateProvider(), true);

    expect(failedMock).toHaveBeenCalled();
    expect(exitMock).toHaveBeenCalledWith(1);
});
