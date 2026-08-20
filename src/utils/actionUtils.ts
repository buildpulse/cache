import * as core from "@actions/core";
import * as glob from "@actions/glob";
import * as os from "os";
import * as path from "path";

import { RefKey, Inputs } from "../constants";

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

export function logWarning(message: string): void {
    const warningPrefix = "[warning]";
    core.info(`${warningPrefix}${message}`);
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

export function validateAwsCredentials(): boolean {
    // Bucket + region are always required. Static access keys are optional —
    // when absent, buildpulse/cache@v6 uses the SDK default provider chain
    // (EKS Pod Identity).
    const requiredVars = [
        ["BP_CACHE_AWS_REGION", "AWS_REGION"],
        ["BP_CACHE_S3_BUCKET"],
    ];
    const missingEnvVars = requiredVars
        .filter(vars => !vars.some(v => process.env[v]))
        .map(vars => vars[0]);

    if (missingEnvVars.length > 0) {
        logWarning(`Missing required AWS environment variables: ${missingEnvVars.join(", ")}`);
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
function expandHome(pattern: string): string {
    if (pattern === "~") {
        return os.homedir();
    }
    if (pattern.startsWith("~/")) {
        return path.join(os.homedir(), pattern.slice(2));
    }
    return pattern;
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
    if (validateAwsCredentials()) {
        return true;
    }

    logWarning(
        "S3 caching is not available. Please check your AWS credentials and S3 bucket configuration."
    );
    return false;
}
