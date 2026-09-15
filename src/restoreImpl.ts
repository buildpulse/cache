import * as core from "@actions/core";
import { HeadObjectCommand, S3Client } from "@aws-sdk/client-s3";
import * as path from "path";

import { CacheFailure, report } from "./cacheErrors";
import { Events, Inputs, Outputs, State } from "./constants";
import {
    downloadFromS3,
    initializeS3Client,
    resolvedCredentialSource,
    s3Client
} from "./s3Client";
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
    // Anything worse than a plain miss, remembered so the end of the run can
    // fail the step when the caller asked for that.
    let failure: CacheFailure | undefined;
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

        // Required: an empty key would restore from "<prefix>/<basename>",
        // an entry that every workflow caching the same path name shares.
        const primaryKey =
            stateProvider.getState(State.CachePrimaryKey) ||
            core.getInput(Inputs.Key, { required: true });
        stateProvider.setState(State.CachePrimaryKey, primaryKey);

        // No slice. An earlier version dropped the first entry here, which
        // silently discarded the user's highest-priority fallback key.
        const restoreKeys = utils.getInputAsArray(Inputs.RestoreKeys);
        const cachePathPatterns = utils.getInputAsArray(Inputs.Path, {
            required: true
        });

        // Resolve glob patterns to actual file paths.
        // On restore the target usually does NOT exist yet — that is the whole
        // point — so the globber matches nothing and we fall back to the raw
        // patterns. That fallback must still be home-expanded: `effectivePaths`
        // becomes the tar extraction destination below, and a literal "~/x"
        // makes `path.dirname` return "~", so the archive lands in a directory
        // named "~" under the CWD while the action still reports a cache hit.
        // resolvePaths() expands internally, so only the fallback needs it.
        const cachePaths = await utils.resolvePaths(cachePathPatterns);
        const effectivePaths = utils.effectiveCachePaths(
            cachePaths,
            cachePathPatterns
        );

        const failOnCacheMiss = utils.getInputAsBool(Inputs.FailOnCacheMiss);
        const lookupOnly = utils.getInputAsBool(Inputs.LookupOnly);
        const bucketName = process.env.BP_CACHE_S3_BUCKET || "";

        // Initialize S3 client
        initializeS3Client();

        const allKeys = [primaryKey, ...restoreKeys];
        for (const key of allKeys) {
            let s3Key = utils.cacheObjectKey(key, effectivePaths[0]);
            try {
                if (lookupOnly) {
                    const headObjectCommand = new HeadObjectCommand({
                        Bucket: bucketName,
                        Key: s3Key
                    });
                    try {
                        await s3Client.send(headObjectCommand);
                        core.info(
                            `Cache found and can be restored from key: ${s3Key}`
                        );
                        cacheKey = s3Key;
                        break;
                    } catch (headError) {
                        if ((headError as any).name !== "NotFound") {
                            throw headError;
                        }
                    }
                    // Nothing under this key, so try the next one. This used to
                    // set cacheKey and break here as well, which reported
                    // cache-hit=true for a cache that did not exist and never
                    // looked at the restore keys.
                    core.info(`No cache found for key: ${s3Key}`);
                } else {
                    for (const cachePath of effectivePaths) {
                        s3Key = utils.cacheObjectKey(key, cachePath);

                        core.info(`Pulling ${s3Key}`);
                        const destinationPath = cachePath;
                        await downloadFromS3(
                            bucketName,
                            s3Key,
                            destinationPath
                        );
                    }
                    cacheKey = s3Key;
                    core.info(`Cache restored from key: ${cacheKey}`);
                    break;
                }
            } catch (error) {
                const kind = report("restore", error, {
                    credentialSource: resolvedCredentialSource(),
                    keyPrefixSet: !!process.env.BP_CACHE_KEY_PREFIX
                });
                if (kind !== CacheFailure.Miss) {
                    failure = kind;
                }
                core.info(`No cache restored from ${s3Key}`);
            }
        }

        if (failure && utils.failOnCacheError()) {
            throw new Error(
                `Cache ${failure} error and on-cache-error is set to error.`
            );
        }

        const isExactKeyMatch =
            cacheKey === utils.cacheObjectKey(primaryKey, effectivePaths[0]);
        core.setOutput(Outputs.CacheHit, isExactKeyMatch.toString());

        if (!cacheKey) {
            core.setOutput(Outputs.CacheHit, "false");
            if (failOnCacheMiss) {
                throw new Error(
                    `Failed to restore cache entry. Exiting as fail-on-cache-miss is set. Input key: ${primaryKey}`
                );
            }
            core.info(
                `Cache not found for input keys: ${[...allKeys].join(", ")}`
            );
            return undefined;
        }

        // Store the matched cache key in states
        stateProvider.setState(State.CacheMatchedKey, cacheKey);
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
