import * as core from "@actions/core";
import * as glob from "@actions/glob";
import * as os from "os";
import * as path from "path";

import { Inputs, RefKey } from "../constants";
import {
    CredentialSource,
    resolveCredentials,
    resolveRegion
} from "../credentials";

export function isGhes(): boolean {
    const ghUrl = new URL(
        process.env["GITHUB_SERVER_URL"] || "https://github.com"
    );
    return ghUrl.hostname.toUpperCase() !== "GITHUB.COM";
}

export function isExactKeyMatch(key: string, cacheKey?: string): boolean {
    return !!(
        cacheKey &&
        cacheKey.localeCompare(key, undefined, {
            sensitivity: "accent"
        }) === 0
    );
}

/**
 * A real warning. This used to be `core.info` with a literal "[warning]"
 * prefix, which produces no workflow command, so nothing it reported appeared
 * as an annotation, in the job summary, or in the Checks API -- every caller
 * believed it was warning and none of it was visible.
 */
export function logWarning(message: string): void {
    core.warning(message);
}

/**
 * Whether a cache failure that is not a plain miss should fail the step.
 * Default is to keep the job green, because a cache is an optimisation; a
 * workflow that would rather know can set on-cache-error: error.
 */
export function failOnCacheError(): boolean {
    return core.getInput(Inputs.OnCacheError).trim().toLowerCase() === "error";
}

// Cache token authorized for all events that are tied to a ref
// See GitHub Context https://help.github.com/actions/automating-your-workflow-with-github-actions/contexts-and-expression-syntax-for-github-actions#github-context
export function isValidEvent(): boolean {
    return RefKey in process.env && Boolean(process.env[RefKey]);
}

export function getInputAsArray(
    name: string,
    options?: core.InputOptions
): string[] {
    return core
        .getInput(name, options)
        .split("\n")
        .map(s => s.replace(/^!\s+/, "!").trim())
        .filter(x => x !== "");
}

export function getInputAsInt(
    name: string,
    options?: core.InputOptions
): number | undefined {
    const value = parseInt(core.getInput(name, options));
    if (isNaN(value) || value < 0) {
        return undefined;
    }
    return value;
}

export function getInputAsBool(
    name: string,
    options?: core.InputOptions
): boolean {
    const result = core.getInput(name, options);
    return result.toLowerCase() === "true";
}

/**
 * Whether this job is configured to use the cache at all.
 *
 * "Not configured" is a different thing from "configured and failing", and the
 * two must not share a message: the first is normal on a runner without the
 * cache, the second is a defect. This only answers the first question --
 * whether a bucket, a region and some credential source are present. Whether
 * those credentials actually work is answered later, loudly, by cacheErrors.
 */
export function validateAwsCredentials(): boolean {
    const missing: string[] = [];
    if (!process.env.BP_CACHE_S3_BUCKET) {
        missing.push("a bucket (BP_CACHE_S3_BUCKET)");
    }
    if (!resolveRegion().region) {
        missing.push("a region (aws-region input or BP_CACHE_AWS_REGION)");
    }
    if (resolveCredentials().source === CredentialSource.None) {
        missing.push(
            "credentials (aws-access-key-id/aws-secret-access-key or " +
                "aws-credentials-file inputs, or BP_CACHE_AWS_CREDENTIALS_FILE)"
        );
    }

    if (missing.length > 0) {
        logWarning(
            `The BuildPulse cache is not configured for this job: no ${missing.join(
                ", no "
            )}. Caching will be skipped.`
        );
        return false;
    }

    return true;
}

/** S3 object key for a cache entry. Optional BP_CACHE_KEY_PREFIX enables
 *  shared-bucket tenant isolation (namespace/) with Pod Identity ABAC. */
export function cacheObjectKey(primaryKey: string, filePath: string): string {
    const prefix = (process.env.BP_CACHE_KEY_PREFIX || "").replace(/\/+$/, "");
    const base = `${primaryKey}/${path.basename(filePath)}`;
    return prefix ? `${prefix}/${base}` : base;
}

export function generateS3Key(primaryKey: string, filePath: string): string {
    return cacheObjectKey(primaryKey, filePath);
}

// Expand a leading "~" to the runner's home directory. glob does not do this,
// and a literal "~" directory never exists, so an unexpanded path resolves to
// nothing and the cache silently no-ops. `path: ~/.cache/Cypress` is the
// idiomatic form in actions/cache, so it has to work here too.
export function expandHome(pattern: string): string {
    if (pattern === "~") {
        return os.homedir();
    }
    if (pattern.startsWith("~/")) {
        return path.join(os.homedir(), pattern.slice(2));
    }
    return pattern;
}

// Which paths should restore actually write to?
//
// On restore the target usually does not exist yet, so the globber matches
// nothing and we must fall back to the raw `path` inputs. Those inputs are used
// directly as the tar extraction destination, so they have to be home-expanded
// first: `path.dirname("~/x")` is "~", which silently extracts the archive into
// a directory named "~" under the CWD while the action still reports a cache
// hit. Split out from restoreImpl so this branch is testable without mocking
// S3 — the defect lived in the one path unit tests never reached.
export function effectiveCachePaths(
    resolved: string[],
    patterns: string[]
): string[] {
    return resolved.length > 0 ? resolved : patterns.map(expandHome);
}

export async function resolvePaths(patterns: string[]): Promise<string[]> {
    const paths: string[] = [];
    const globber = await glob.create(patterns.map(expandHome).join("\n"), {
        implicitDescendants: false
    });

    // NO workspace filter. This function used to drop every resolved path that
    // fell outside GITHUB_WORKSPACE, which silently broke every cache entry
    // pointing at a system or home directory — the two most common ones being
    // `/var/cache/apt/archives` and `~/.cache/Cypress`. Paths inside the repo
    // (node_modules, public/packs-test) kept working, so it read as "that cache
    // just never hits" rather than "save is a no-op", and survived for months.
    //
    // Nothing downstream needs workspace-relative paths: uploadToS3 tars with
    // `-C dirname(p) basename(p)`, so an absolute path anywhere is fine.
    for await (const file of globber.globGenerator()) {
        paths.push(file);
    }

    return paths;
}

export function isCacheFeatureAvailable(): boolean {
    // validateAwsCredentials already says precisely what is missing; a second,
    // vaguer line on top of it only made the real message harder to find.
    return validateAwsCredentials();
}
