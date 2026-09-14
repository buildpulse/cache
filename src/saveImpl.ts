import * as core from "@actions/core";
import * as path from "path";

import { CacheFailure, report } from "./cacheErrors";
import { Events, Inputs, State } from "./constants";
import {
    initializeS3Client,
    resolvedCredentialSource,
    uploadToS3
} from "./s3Client";
import {
    IStateProvider,
    NullStateProvider,
    StateProvider
} from "./stateProvider";
import * as utils from "./utils/actionUtils";

// Catch and log any unhandled exceptions. These can leak out of an upload when
// a failed request closes the file descriptor and an in-process read then
// throws. Reported at the volume the failure deserves rather than swallowed:
// this handler used to route a credential error to an invisible info line.
process.on("uncaughtException", e =>
    report("save", e, {
        credentialSource: resolvedCredentialSource(),
        keyPrefixSet: !!process.env.BP_CACHE_KEY_PREFIX
    })
);

export async function saveImpl(
    stateProvider: IStateProvider
): Promise<string | void> {
    let cacheKey: string | undefined;
    // Anything worse than a plain miss, so the end of the run can fail the step
    // when the caller asked for that.
    let failure: CacheFailure | undefined;
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

        // If matched restore key is same as primary key, then do not save cache
        // NO-OP in case of SaveOnly action
        const restoredKey = stateProvider.getCacheState();

        if (utils.isExactKeyMatch(primaryKey, restoredKey)) {
            core.info(
                `Cache hit occurred on the primary key ${primaryKey}, not saving cache.`
            );
            return;
        }

        const cachePathPatterns = utils.getInputAsArray(Inputs.Path, {
            required: true
        });

        // Resolve glob patterns to actual file paths
        const cachePaths = await utils.resolvePaths(cachePathPatterns);
        if (cachePaths.length === 0) {
            utils.logWarning(
                `No files found matching the cache path patterns: ${cachePathPatterns.join(
                    ", "
                )}`
            );
            return;
        }
        core.info(`Resolved cache paths: ${cachePaths.join(", ")}`);

        const bucketName = process.env.BP_CACHE_S3_BUCKET;
        if (!bucketName) {
            throw new Error(
                "BP_CACHE_S3_BUCKET environment variable is not set"
            );
        }

        // Initialize S3 client
        initializeS3Client();

        // Upload each cache path to S3
        for (const cachePath of cachePaths) {
            const s3Key = utils.cacheObjectKey(primaryKey, cachePath);
            try {
                await uploadToS3(bucketName, s3Key, cachePath);
                if (!cacheKey) {
                    cacheKey = s3Key;
                }
            } catch (error) {
                const kind = report("save", error, {
                    credentialSource: resolvedCredentialSource(),
                    keyPrefixSet: !!process.env.BP_CACHE_KEY_PREFIX
                });
                if (kind !== CacheFailure.Miss) {
                    failure = kind;
                }
            }
        }

        if (cacheKey) {
            core.info(`Cache saved with key: ${cacheKey}`);
        } else if (!failure) {
            core.warning("Failed to save cache to S3");
        }

        if (failure && utils.failOnCacheError()) {
            throw new Error(
                `Cache ${failure} error and on-cache-error is set to error.`
            );
        }
    } catch (error: unknown) {
        if (error instanceof Error) {
            utils.logWarning(
                `Error saving cache to S3 (including potential compression errors): ${error.message}`
            );
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
