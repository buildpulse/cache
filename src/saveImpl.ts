import * as core from "@actions/core";
import * as path from "path";

import { Events, Inputs, State } from "./constants";
import { initializeS3Client, uploadToS3 } from "./s3Client";
import {
    IStateProvider,
    NullStateProvider,
    StateProvider
} from "./stateProvider";
import * as utils from "./utils/actionUtils";

// Catch and log any unhandled exceptions.  These exceptions can leak out of the uploadChunk method in
// @actions/toolkit when a failed upload closes the file descriptor causing any in-process reads to
// throw an uncaught exception.  Instead of failing this action, just warn.
process.on("uncaughtException", e => utils.logWarning(e.message));

export async function saveImpl(
    stateProvider: IStateProvider
): Promise<string | void> {
    let cacheKey: string | undefined;
    try {
        if (!utils.isCacheFeatureAvailable()) {
            return;
        }

        if (!utils.isValidEvent()) {
            utils.logWarning(
                `Event Validation Error: The event type ${
                    process.env[Events.Key]
                } is not supported because it's not tied to a branch or tag ref.`
            );
            return;
        }

        // If restore has stored a primary key in state, reuse that
        // Else re-evaluate from inputs
        const primaryKey =
            stateProvider.getState(State.CachePrimaryKey) ||
            core.getInput(Inputs.Key);

        if (!primaryKey) {
            utils.logWarning(`Key is not specified.`);
            return;
        }

        cacheKey = primaryKey;

        // If matched restore key is same as primary key, then do not save cache
        // NO-OP in case of SaveOnly action
        const restoredKey = stateProvider.getCacheState();

        if (utils.isExactKeyMatch(primaryKey, restoredKey)) {
            core.info(
                `Cache hit occurred on the primary key ${primaryKey}, not saving cache.`
            );
            return;
        }

        const cachePaths = utils.getInputAsArray(Inputs.Path, {
            required: true
        });

        const bucketName = process.env.BP_CACHE_S3_BUCKET;
        if (!bucketName) {
            throw new Error("BP_CACHE_S3_BUCKET environment variable is not set");
        }

        // Initialize S3 client
        initializeS3Client();

        // Upload each cache path to S3
        let success = true;
        for (const cachePath of cachePaths) {
            const s3Key = `${primaryKey}:${cachePath}`; // '.gz' will be appended in uploadToS3 if compressed
            try {
                await uploadToS3(bucketName, s3Key, cachePath);
            } catch (error) {
                success = false;
                utils.logWarning(`Failed to upload ${cachePath} to S3: ${(error as Error).message}`);
            }
        }

        if (success) {
            core.info(`Cache saved with key: ${primaryKey}`);
        } else {
            core.warning("Failed to save cache to S3");
        }
    } catch (error: unknown) {
        if (error instanceof Error) {
            utils.logWarning(`Error saving cache to S3 (including potential compression errors): ${error.message}`);
        } else {
            utils.logWarning(`Unknown error occurred while saving cache to S3`);
        }
    }

    return cacheKey;
}

export async function saveOnlyRun(
    earlyExit?: boolean | undefined
): Promise<void> {
    try {
        const cacheId = await saveImpl(new NullStateProvider());
        if (!cacheId) {
            core.warning(`Cache save to S3 failed.`);
        }
    } catch (err) {
        console.error(err);
        if (earlyExit) {
            process.exit(1);
        }
    }

    // node will stay alive if any promises are not resolved,
    // which is a possibility if HTTP requests are dangling
    // due to retries or timeouts. We know that if we got here
    // that all promises that we care about have successfully
    // resolved, so simply exit with success.
    if (earlyExit) {
        process.exit(0);
    }
}

export async function saveRun(earlyExit?: boolean | undefined): Promise<void> {
    try {
        await saveImpl(new StateProvider());
    } catch (err) {
        console.error(err);
        if (earlyExit) {
            process.exit(1);
        }
    }

    // node will stay alive if any promises are not resolved,
    // which is a possibility if HTTP requests are dangling
    // due to retries or timeouts. We know that if we got here
    // that all promises that we care about have successfully
    // resolved, so simply exit with success.
    if (earlyExit) {
        process.exit(0);
    }
}
