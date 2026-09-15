import * as core from "@actions/core";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

import { Events, Inputs, RefKey, State } from "../src/constants";
import * as s3 from "../src/s3Client";
import { saveRun } from "../src/saveImpl";
import * as actionUtils from "../src/utils/actionUtils";
import * as testUtils from "../src/utils/testUtils";

// saveRun is the post step of the combined action: the key comes from the
// state the restore step left. saveImpl.test.ts covers the save itself.
jest.mock("../src/s3Client", () => ({
    initializeS3Client: jest.fn(),
    resolvedCredentialSource: jest.fn(() => "test credentials"),
    uploadToS3: jest.fn()
}));

const upload = s3.uploadToS3 as jest.MockedFunction<typeof s3.uploadToS3>;

const primaryKey = "Linux-node-bb828da54c148048dd17899ba9fda624811cfb43";

let work: string;
let cacheDir: string;

beforeEach(() => {
    jest.restoreAllMocks();
    upload.mockReset();
    process.env[Events.Key] = Events.Push;
    process.env[RefKey] = "refs/heads/feature-branch";
    process.env.BP_CACHE_S3_BUCKET = "test-bucket";

    work = fs.mkdtempSync(path.join(os.tmpdir(), "cache-save-run-"));
    cacheDir = path.join(work, "node_modules");
    fs.mkdirSync(cacheDir);

    jest.spyOn(actionUtils, "isCacheFeatureAvailable").mockReturnValue(true);
    jest.spyOn(core, "getState").mockImplementation(name =>
        name === State.CachePrimaryKey ? primaryKey : ""
    );
    jest.spyOn(core, "setFailed").mockImplementation();
    jest.spyOn(core, "warning").mockImplementation();
    jest.spyOn(core, "info").mockImplementation();
});

afterEach(() => {
    testUtils.clearInputs();
    delete process.env[Events.Key];
    delete process.env[RefKey];
    delete process.env.BP_CACHE_S3_BUCKET;
    fs.rmSync(work, { recursive: true, force: true });
});

test("saveRun uploads under the key the restore step stored", async () => {
    testUtils.setInput(Inputs.Path, cacheDir);
    upload.mockResolvedValue(undefined);

    await saveRun();

    expect(upload).toHaveBeenCalledWith(
        "test-bucket",
        `${primaryKey}/node_modules`,
        expect.stringMatching(/node_modules$/)
    );
});

test("saveRun with earlyExit exits 0 after a save", async () => {
    testUtils.setInput(Inputs.Path, cacheDir);
    upload.mockResolvedValue(undefined);
    const exitMock = jest
        .spyOn(process, "exit")
        .mockImplementation((() => undefined) as never);

    await saveRun(true);

    expect(exitMock).toHaveBeenCalledWith(0);
});
