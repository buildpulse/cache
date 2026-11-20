import * as core from "@actions/core";
import { HeadObjectCommand, S3Client } from "@aws-sdk/client-s3";
import * as path from "path";

import { CacheFailure, classify, report } from "./cacheErrors";
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

function sharesBasename(cachePath: string, all: string[]): boolean {
    const name = path.basename(cachePath);
    return all.some(p => p !== cachePath && path.basename(p) === name);
}

export async function restoreImpl(
    stateProvider: IStateProvider,
    earlyExit?: boolean | undefined
): Promise<string | undefined> {
    let cacheKey: string | undefined;
    // Anything worse than a plain miss, remembered so the end of the run can
    // fail the step when the caller asked for that.
    let failure: CacheFailure | undefined;
    // Paths after the first that the matched entry did not contain.
    let missingPaths: string[] = [];
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

        // Every literal path, existing or not. See restorePaths for why
        // "only what exists" silently dropped half of a multi-path cache.
        const effectivePaths = await utils.restorePaths(cachePathPatterns);

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
                    // An entry exists if its first path does, under either
                    // the current object name or the pre-v7.0.5 one.
                    let found = false;
                    for (const candidate of [
                        s3Key,
                        utils.legacyCacheObjectKey(key, effectivePaths[0])
                    ]) {
                        try {
                            await s3Client.send(
                                new HeadObjectCommand({
                                    Bucket: bucketName,
                                    Key: candidate
                                })
                            );
                            s3Key = candidate;
                            found = true;
                            break;
                        } catch (headError) {
                            if ((headError as any).name !== "NotFound") {
                                throw headError;
                            }
                        }
                    }
                    if (found) {
                        core.info(
                            `Cache found and can be restored from key: ${s3Key}`
                        );
                        cacheKey = key;
                        break;
                    }
                    // Nothing under this key, so try the next one. This used to
                    // set cacheKey and break here as well, which reported
                    // cache-hit=true for a cache that did not exist and never
                    // looked at the restore keys.
                    core.info(`No cache found for key: ${s3Key}`);
                } else {
                    // The first path decides whether this key exists at all;
                    // a miss there falls through to the next key as before.
                    // A later path can be absent from an entry saved before
                    // that directory existed. Restore the rest and say so,
                    // rather than throwing away a good entry.
                    const missing: string[] = [];
                    for (const [i, cachePath] of effectivePaths.entries()) {
                        s3Key = utils.cacheObjectKey(key, cachePath);
                        core.info(`Pulling ${s3Key}`);
                        try {
                            await downloadFromS3(bucketName, s3Key, cachePath);
                            continue;
                        } catch (pathError) {
                            if (classify(pathError) !== CacheFailure.Miss) {
                                throw pathError;
                            }
                        }

                        // Entries saved before v7.0.5 name each object by its
                        // basename alone. Fall back to that so they keep
                        // hitting. If another path in this step shares the
                        // basename, the object holds whichever path was
                        // uploaded last, so its content is suspect: restore
                        // it (as the old version did) but count the entry as
                        // partial, which re-saves it under the new names.
                        s3Key = utils.legacyCacheObjectKey(key, cachePath);
                        try {
                            await downloadFromS3(bucketName, s3Key, cachePath);
                            if (sharesBasename(cachePath, effectivePaths)) {
                                core.info(
                                    `Restored ${cachePath} from ${s3Key}, which another path with the same name may have overwritten; the entry will be saved again.`
                                );
                                missing.push(cachePath);
                            }
                            continue;
                        } catch (pathError) {
                            if (
                                i === 0 ||
                                classify(pathError) !== CacheFailure.Miss
                            ) {
                                throw pathError;
                            }
                        }
                        core.info(`Not in this cache entry: ${cachePath}`);
                        missing.push(cachePath);
                    }
                    missingPaths = missing;
                    // The matched key is the user's key, not the S3 object
                    // key: the post step compares it with the primary key to
                    // skip re-uploading an exact hit. Storing the object key
                    // (prefix + key + basename) made that comparison always
                    // false, so every exact hit re-uploaded the whole cache.
                    cacheKey = key;
                    core.info(`Cache restored from key: ${key} (${s3Key})`);
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

        // A partial restore is not a hit: callers gate installs on
        // cache-hit, and the post step must save to fill the gap.
        const isExactKeyMatch =
            utils.isExactKeyMatch(primaryKey, cacheKey) &&
            missingPaths.length === 0;
        core.setOutput(Outputs.CacheHit, isExactKeyMatch.toString());
        if (cacheKey && missingPaths.length > 0) {
            core.info(
                `Partial restore: ${missingPaths.join(
                    ", "
                )} not in the cache entry; it will be saved at the end of the job.`
            );
            stateProvider.setState(State.CachePartial, "true");
        }

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
