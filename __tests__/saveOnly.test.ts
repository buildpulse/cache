import * as core from "@actions/core";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

import { Events, Inputs, RefKey } from "../src/constants";
import * as s3 from "../src/s3Client";
import { saveOnlyRun } from "../src/saveImpl";
import * as actionUtils from "../src/utils/actionUtils";
import * as testUtils from "../src/utils/testUtils";

// saveOnlyRun backs the standalone save action: there is no restore step, so
// the key comes from its own input.
jest.mock("../src/s3Client", () => ({
    initializeS3Client: jest.fn(),
    resolvedCredentialSource: jest.fn(() => "test credentials"),
    uploadToS3: jest.fn()
}));

const upload = s3.uploadToS3 as jest.MockedFunction<typeof s3.uploadToS3>;

const primaryKey = "Linux-node-bb828da54c148048dd17899ba9fda624811cfb43";

let work: string;
let cacheDir: string;
let warningMock: jest.SpyInstance;

beforeEach(() => {
    jest.restoreAllMocks();
    upload.mockReset();
    process.env[Events.Key] = Events.Push;
    process.env[RefKey] = "refs/heads/feature-branch";
    process.env.BP_CACHE_S3_BUCKET = "test-bucket";

    work = fs.mkdtempSync(path.join(os.tmpdir(), "cache-save-only-"));
    cacheDir = path.join(work, "node_modules");
    fs.mkdirSync(cacheDir);

    jest.spyOn(actionUtils, "isCacheFeatureAvailable").mockReturnValue(true);
    jest.spyOn(core, "setFailed").mockImplementation();
    jest.spyOn(core, "info").mockImplementation();
    warningMock = jest.spyOn(core, "warning").mockImplementation();
});

afterEach(() => {
    testUtils.clearInputs();
    delete process.env[Events.Key];
    delete process.env[RefKey];
    delete process.env.BP_CACHE_S3_BUCKET;
    fs.rmSync(work, { recursive: true, force: true });
});

test("saveOnlyRun takes the key from its own input", async () => {
    testUtils.setInput(Inputs.Key, primaryKey);
    testUtils.setInput(Inputs.Path, cacheDir);
    upload.mockResolvedValue(undefined);

    await saveOnlyRun();

    expect(upload).toHaveBeenCalledWith(
        "test-bucket",
        `${primaryKey}/node_modules`,
        expect.stringMatching(/node_modules$/)
    );
    expect(warningMock).not.toHaveBeenCalledWith("Cache save to S3 failed.");
});

test("saveOnlyRun warns when nothing was saved", async () => {
    testUtils.setInput(Inputs.Key, primaryKey);
    testUtils.setInput(Inputs.Path, cacheDir);
    upload.mockRejectedValue(new Error("socket hang up"));

    await saveOnlyRun();

    expect(warningMock).toHaveBeenCalledWith("Cache save to S3 failed.");
});
