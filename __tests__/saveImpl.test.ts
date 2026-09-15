import * as core from "@actions/core";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

import { resetReportState } from "../src/cacheErrors";
import { Events, Inputs, RefKey, State } from "../src/constants";
import * as s3 from "../src/s3Client";
import { saveImpl } from "../src/saveImpl";
import { StateProvider } from "../src/stateProvider";
import * as actionUtils from "../src/utils/actionUtils";
import * as testUtils from "../src/utils/testUtils";

// The S3 client is the boundary; key choice, path resolution and failure
// handling above it run for real.
jest.mock("../src/s3Client", () => ({
    initializeS3Client: jest.fn(),
    resolvedCredentialSource: jest.fn(() => "test credentials"),
    uploadToS3: jest.fn()
}));

const upload = s3.uploadToS3 as jest.MockedFunction<typeof s3.uploadToS3>;

const bucket = "test-bucket";
const primaryKey = "Linux-node-bb828da54c148048dd17899ba9fda624811cfb43";

function awsError(name: string): Error {
    return Object.assign(new Error(`${name} from S3`), { name });
}

// What the restore step leaves behind for the post step to read.
function givenState(state: { primaryKey?: string; matchedKey?: string }): void {
    jest.spyOn(core, "getState").mockImplementation(name => {
        if (name === State.CachePrimaryKey) {
            return state.primaryKey || "";
        }
        if (name === State.CacheMatchedKey) {
            return state.matchedKey || "";
        }
        return "";
    });
}

let work: string;
let cacheDir: string;
let failedMock: jest.SpyInstance;
let warningMock: jest.SpyInstance;
let errorMock: jest.SpyInstance;
let infoMock: jest.SpyInstance;

beforeEach(() => {
    jest.restoreAllMocks();
    upload.mockReset();
    resetReportState();

    process.env[Events.Key] = Events.Push;
    process.env[RefKey] = "refs/heads/feature-branch";
    process.env.BP_CACHE_S3_BUCKET = bucket;

    work = fs.mkdtempSync(path.join(os.tmpdir(), "cache-save-"));
    cacheDir = path.join(work, "node_modules");
    fs.mkdirSync(cacheDir);

    jest.spyOn(actionUtils, "isCacheFeatureAvailable").mockReturnValue(true);
    failedMock = jest.spyOn(core, "setFailed").mockImplementation();
    warningMock = jest.spyOn(core, "warning").mockImplementation();
    errorMock = jest.spyOn(core, "error").mockImplementation();
    infoMock = jest.spyOn(core, "info").mockImplementation();
    jest.spyOn(core, "setOutput").mockImplementation();
});

afterEach(() => {
    testUtils.clearInputs();
    delete process.env[Events.Key];
    delete process.env[RefKey];
    delete process.env.BP_CACHE_S3_BUCKET;
    fs.rmSync(work, { recursive: true, force: true });
});

test("an event without a ref warns and saves nothing", async () => {
    process.env[Events.Key] = "commit_comment";
    delete process.env[RefKey];

    await saveImpl(new StateProvider());

    expect(warningMock).toHaveBeenCalledWith(
        "Event Validation Error: The event type commit_comment is not supported because it's not tied to a branch or tag ref."
    );
    expect(upload).not.toHaveBeenCalled();
    expect(failedMock).not.toHaveBeenCalled();
});

test("an unconfigured cache saves nothing", async () => {
    jest.spyOn(actionUtils, "isCacheFeatureAvailable").mockReturnValue(false);
    givenState({ primaryKey });
    testUtils.setInput(Inputs.Path, cacheDir);

    await saveImpl(new StateProvider());

    expect(upload).not.toHaveBeenCalled();
    expect(failedMock).not.toHaveBeenCalled();
});

test("no primary key warns and saves nothing", async () => {
    givenState({});
    testUtils.setInput(Inputs.Path, cacheDir);

    await saveImpl(new StateProvider());

    expect(warningMock).toHaveBeenCalledWith("Key is not specified.");
    expect(upload).not.toHaveBeenCalled();
});

test("an exact hit on the primary key saves nothing", async () => {
    givenState({ primaryKey, matchedKey: primaryKey });
    testUtils.setInput(Inputs.Path, cacheDir);

    await saveImpl(new StateProvider());

    expect(infoMock).toHaveBeenCalledWith(
        `Cache hit occurred on the primary key ${primaryKey}, not saving cache.`
    );
    expect(upload).not.toHaveBeenCalled();
});

test("a resolved path is uploaded under the primary key", async () => {
    givenState({ primaryKey, matchedKey: "Linux-node-" });
    testUtils.setInput(Inputs.Path, cacheDir);
    upload.mockResolvedValue(undefined);

    const saved = await saveImpl(new StateProvider());

    expect(upload).toHaveBeenCalledTimes(1);
    expect(upload).toHaveBeenCalledWith(
        bucket,
        `${primaryKey}/node_modules`,
        expect.stringMatching(/node_modules$/)
    );
    expect(saved).toBe(`${primaryKey}/node_modules`);
    expect(warningMock).not.toHaveBeenCalled();
    expect(failedMock).not.toHaveBeenCalled();
});

test("a path that matches nothing warns and saves nothing", async () => {
    givenState({ primaryKey });
    const missing = path.join(work, "does-not-exist");
    testUtils.setInput(Inputs.Path, missing);

    await saveImpl(new StateProvider());

    expect(warningMock).toHaveBeenCalledWith(
        `No files found matching the cache path patterns: ${missing}`
    );
    expect(upload).not.toHaveBeenCalled();
});

test("a missing path input warns and saves nothing", async () => {
    givenState({ primaryKey });

    await saveImpl(new StateProvider());

    expect(warningMock).toHaveBeenCalledWith(
        "Error saving cache to S3 (including potential compression errors): Input required and not supplied: path"
    );
    expect(upload).not.toHaveBeenCalled();
});

test("no bucket warns and saves nothing", async () => {
    delete process.env.BP_CACHE_S3_BUCKET;
    givenState({ primaryKey });
    testUtils.setInput(Inputs.Path, cacheDir);

    await saveImpl(new StateProvider());

    expect(warningMock).toHaveBeenCalledWith(
        "Error saving cache to S3 (including potential compression errors): BP_CACHE_S3_BUCKET environment variable is not set"
    );
    expect(upload).not.toHaveBeenCalled();
});

test("an upload that fails for an ordinary reason warns and keeps the step green", async () => {
    givenState({ primaryKey });
    testUtils.setInput(Inputs.Path, cacheDir);
    upload.mockRejectedValue(new Error("socket hang up"));

    const saved = await saveImpl(new StateProvider());

    expect(warningMock).toHaveBeenCalledWith(
        "BuildPulse cache save failed: Error: socket hang up"
    );
    expect(saved).toBeUndefined();
    expect(failedMock).not.toHaveBeenCalled();
});

test("a denied upload is an error annotation, and keeps the step green by default", async () => {
    givenState({ primaryKey });
    testUtils.setInput(Inputs.Path, cacheDir);
    upload.mockRejectedValue(awsError("AccessDenied"));

    await saveImpl(new StateProvider());

    expect(errorMock).toHaveBeenCalledTimes(1);
    expect(errorMock.mock.calls[0][0]).toContain("was denied");
    expect(failedMock).not.toHaveBeenCalled();
});

test("on-cache-error: error fails the step on a denied upload", async () => {
    givenState({ primaryKey });
    testUtils.setInput(Inputs.Path, cacheDir);
    testUtils.setInput(Inputs.OnCacheError, "error");
    upload.mockRejectedValue(awsError("AccessDenied"));

    await saveImpl(new StateProvider());

    expect(failedMock).toHaveBeenCalledWith(
        "Cache auth error and on-cache-error is set to error."
    );
});
