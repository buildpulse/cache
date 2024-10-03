import * as core from "@actions/core";
import * as path from "path";
import { S3Client, HeadObjectCommand } from "@aws-sdk/client-s3";
import { initializeS3Client, downloadFromS3, s3Client } from "./s3Client";

import { Events, Inputs, Outputs, State } from "./constants";
import {
    IStateProvider,
    NullStateProvider,
    StateProvider
} from "./stateProvider";
import * as utils from "./utils/actionUtils";

export async function restoreImpl(
    stateProvider: IStateProvider,
    earlyExit?: boolean | undefined
): Promise<string | undefined> {
    let cacheKey: string | undefined;
    try {
        if (!utils.isCacheFeatureAvailable()) {
            core.setOutput(Outputs.CacheHit, "false");
            return undefined;
        }

        // Validate inputs, this can cause task failure
        if (!utils.isValidEvent()) {
            utils.logWarning(
                `Event Validation Error: The event type ${
                    process.env[Events.Key]
                } is not supported because it's not tied to a branch or tag ref.`
            );
            return undefined;
        }

        const primaryKey =
            stateProvider.getState(State.CachePrimaryKey) ||
            core.getInput(Inputs.Key);
        stateProvider.setState(State.CachePrimaryKey, primaryKey);

        const restoreKeys = utils.getInputAsArray(Inputs.RestoreKeys).slice(1);
        const cachePaths = utils.getInputAsArray(Inputs.Path, {
            required: true
        });
        const failOnCacheMiss = utils.getInputAsBool(Inputs.FailOnCacheMiss);
        const lookupOnly = utils.getInputAsBool(Inputs.LookupOnly);
        const bucketName = process.env.BP_CACHE_S3_BUCKET || '';

        // Initialize S3 client
        initializeS3Client();

        const allKeys = [primaryKey, ...restoreKeys];
        let found = true;
        for (const key of allKeys) {
            for (const cachePath of cachePaths) {
                const s3Key = `${key}:${cachePath}`;
                try {
                    if (lookupOnly) {
                        const headObjectCommand = new HeadObjectCommand({
                            Bucket: bucketName,
                            Key: s3Key
                        });

                        await s3Client.send(headObjectCommand);
                        core.info(`Cache found and can be restored from key: ${s3Key}`);
                    } else {
                        core.info(`Pulling ${s3Key}`);
                        const destinationPath = cachePath;
                        await downloadFromS3(bucketName, s3Key, destinationPath);

                        core.info(`Cache restored from key: ${s3Key}`);
                        core.setOutput(Outputs.CacheHit, s3Key);
                    }
                } catch (error) {
                    found = false;
                    core.info(`Failed to restore cache from key ${s3Key}: ${(error as Error).message}`);
                }
            }
        }

        if (!found) {
            core.setOutput(Outputs.CacheHit, "false");
            if (failOnCacheMiss) {
                throw new Error(
                    `Failed to restore cache entry. Exiting as fail-on-cache-miss is set. Input key: ${primaryKey}`
                );
            }
            core.info(
                `Cache not found for input keys: ${allKeys.join(', ')}`
            );
            return undefined;
        }

        // Store the matched cache key in states
        stateProvider.setState(State.CacheMatchedKey, allKeys[0]);
    } catch (error: unknown) {
        core.setFailed((error as Error).message);
        if (earlyExit) {
            process.exit(1);
        }
    }
}

async function run(
    stateProvider: IStateProvider,
    earlyExit: boolean | undefined
): Promise<void> {
    await restoreImpl(stateProvider, earlyExit);

    // node will stay alive if any promises are not resolved,
    // which is a possibility if HTTP requests are dangling
    // due to retries or timeouts. We know that if we got here
    // that all promises that we care about have successfully
    // resolved, so simply exit with success.
    if (earlyExit) {
        process.exit(0);
    }
}

export async function restoreOnlyRun(
    earlyExit?: boolean | undefined
): Promise<void> {
    await run(new NullStateProvider(), earlyExit);
}

export async function restoreRun(
    earlyExit?: boolean | undefined
): Promise<void> {
    await run(new StateProvider(), earlyExit);
}
