import * as core from "@actions/core";

/**
 * Telling "there was nothing cached" apart from "the cache is broken".
 *
 * Both used to look identical: every failure, including a credential or
 * permission failure, was funnelled into an info line and the job stayed green
 * with a 0% hit rate. A cache that cannot authenticate is not a cache miss, and
 * reporting it as one is how a broken cache survives for days.
 */
export const enum CacheFailure {
    /** Nothing stored under that key. Normal, quiet. */
    Miss = "miss",
    /** We could not authenticate, or we are not allowed. Loud. */
    Auth = "auth",
    /** Anything else: network, throttling, a malformed archive. Warn. */
    Other = "other"
}

const AUTH_NAMES = new Set([
    "AccessDenied",
    "AccessDeniedException",
    "CredentialsProviderError",
    "ExpiredToken",
    "ExpiredTokenException",
    "InvalidAccessKeyId",
    "InvalidClientTokenId",
    "InvalidToken",
    "SignatureDoesNotMatch",
    "UnrecognizedClientException",
    "Forbidden"
]);

const MISS_NAMES = new Set(["NoSuchKey", "NotFound", "NoSuchBucket"]);

interface MaybeAwsError {
    name?: string;
    message?: string;
    Code?: string;
    $metadata?: { httpStatusCode?: number };
}

export function classify(error: unknown): CacheFailure {
    const e = (error || {}) as MaybeAwsError;
    const name = e.name || e.Code || "";
    if (AUTH_NAMES.has(name)) {
        return CacheFailure.Auth;
    }
    if (MISS_NAMES.has(name)) {
        return CacheFailure.Miss;
    }
    const status = e.$metadata?.httpStatusCode;
    if (status === 401 || status === 403) {
        return CacheFailure.Auth;
    }
    if (status === 404) {
        return CacheFailure.Miss;
    }
    // A bare Error whose message was built by stringifying an SDK error still
    // carries the name. Cheap to check, and it covers the rethrow sites that
    // predate this module.
    const message = e.message || "";
    for (const n of AUTH_NAMES) {
        if (message.includes(n)) {
            return CacheFailure.Auth;
        }
    }
    return CacheFailure.Other;
}

export function describe(error: unknown): string {
    const e = (error || {}) as MaybeAwsError;
    const name = e.name || e.Code;
    const message = e.message || String(error);
    return name && !message.startsWith(name) ? `${name}: ${message}` : message;
}

let authReported = false;

/**
 * Report a failure at the volume it deserves, once.
 *
 * An auth failure repeats per restore key and per cache path, so the first one
 * is an annotation and the rest are plain lines; without that a four-key
 * restore posts four identical red annotations.
 */
export function report(
    phase: "restore" | "save",
    error: unknown,
    context: { credentialSource: string; keyPrefixSet: boolean }
): CacheFailure {
    const kind = classify(error);
    const detail = describe(error);

    if (kind === CacheFailure.Auth) {
        if (!authReported) {
            authReported = true;
            core.error(
                `BuildPulse cache ${phase} was denied: ${detail}. ` +
                    `Credentials came from ${context.credentialSource}. ` +
                    (context.keyPrefixSet
                        ? ""
                        : "BP_CACHE_KEY_PREFIX is not set, which by itself causes a denial. ") +
                    "The cache is not working for this job; it is not a cache miss."
            );
            core.setOutput("cache-error", kind);
        } else {
            core.info(`BuildPulse cache ${phase} denied again: ${detail}`);
        }
        return kind;
    }

    if (kind === CacheFailure.Other) {
        core.warning(`BuildPulse cache ${phase} failed: ${detail}`);
        core.setOutput("cache-error", kind);
        return kind;
    }

    core.info(`BuildPulse cache ${phase}: ${detail}`);
    return kind;
}

/** Test seam: the once-only annotation is module state. */
export function resetReportState(): void {
    authReported = false;
}
