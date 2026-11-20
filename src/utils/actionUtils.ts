import * as core from "@actions/core";
import * as glob from "@actions/glob";
import * as crypto from "crypto";
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

function withKeyPrefix(base: string): string {
    const prefix = (process.env.BP_CACHE_KEY_PREFIX || "").replace(/\/+$/, "");
    return prefix ? `${prefix}/${base}` : base;
}

/** S3 object key for one path of a cache entry. Optional BP_CACHE_KEY_PREFIX
 *  scopes every key under a prefix, so one bucket can serve callers that must
 *  not see each other's entries.
 *
 *  The object name is the basename plus a short hash of the absolute path.
 *  It used to be the basename alone, so two paths with the same last segment
 *  (`a/node_modules` and `b/node_modules`, or `x` and `~/x`) were uploaded to
 *  the same object: the second overwrote the first, and restore extracted that
 *  one archive into both places. The basename stays for readability. */
export function cacheObjectKey(primaryKey: string, filePath: string): string {
    const id = crypto
        .createHash("sha256")
        .update(path.resolve(filePath))
        .digest("hex")
        .slice(0, 12);
    return withKeyPrefix(`${primaryKey}/${path.basename(filePath)}.${id}`);
}

/** The basename-only object key that entries saved before v7.0.5 use. Restore
 *  falls back to it so those entries keep hitting; save never writes it. */
export function legacyCacheObjectKey(
    primaryKey: string,
    filePath: string
): string {
    return withKeyPrefix(`${primaryKey}/${path.basename(filePath)}`);
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
// Every literal `path` input, home-expanded, whether or not it exists yet.
// This used to return only the paths that already existed whenever ANY of them
// did, and the raw inputs only when none did. Something earlier in a job often
// creates one of the directories (actions/setup-go creates ~/.cache/go-build),
// so `path: [~/.cache/go-build, ~/.cache/golangci-lint]` restored go-build
// alone, reported an exact hit, and the post step skipped the save, so the
// missing directory never came back. Measured on rclone's lint job: every
// golangci-lint run started cold, 15 s each instead of about 1 s.
//
// Home-expansion matters too: these paths are the tar extraction destination,
// and `path.dirname("~/x")` is "~", which extracts into a directory named "~"
// under the CWD while the action still reports a cache hit.
//
// Glob patterns keep their old behaviour: what they match now, else the raw
// pattern. Negations only shape what save uploads, so restore ignores them.
const GLOB_CHARS = /[*?[\]{}]/;

export async function restorePaths(patterns: string[]): Promise<string[]> {
    const out: string[] = [];
    for (const pattern of patterns) {
        if (!pattern || pattern.startsWith("!")) {
            continue;
        }
        const expanded = expandHome(pattern);
        const matches = GLOB_CHARS.test(pattern)
            ? await resolvePaths([pattern])
            : [];
        for (const p of matches.length > 0 ? matches : [expanded]) {
            if (!out.includes(p)) {
                out.push(p);
            }
        }
    }
    return out;
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
