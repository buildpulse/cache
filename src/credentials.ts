import * as core from "@actions/core";
import type { AwsCredentialIdentityProvider } from "@aws-sdk/types";

import { Inputs } from "./constants";

/**
 * Credential resolution for the cache's S3 backend.
 *
 * The rule this file exists to enforce: the cache resolves its credentials from
 * sources that are explicitly ours, and never from the ambient `AWS_*`
 * environment. Two failures made that rule necessary.
 *
 * 1. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in the job environment were
 *    read as the cache's own credentials. Any workflow that configures AWS for
 *    its own purposes therefore had those credentials used against the cache
 *    bucket, where they have no access. The SDK's default provider chain has the
 *    same ordering (its env provider runs before the shared-credentials file and
 *    before container credentials), so simply not reading the variables here is
 *    not enough -- the chain has to be bypassed as well, which is why every
 *    branch below builds a specific provider instead of falling back to it.
 *
 * 2. The shared-credentials file was located by expanding `$HOME`. A job that
 *    changes `HOME` (a container step, a tool that resets it) lost the
 *    credentials, and the cache silently stopped working. `fromIni` is given an
 *    absolute `filepath` here, so `HOME` is not consulted.
 *
 * Resolution order, most specific first. The first source that is configured
 * wins outright; a configured source that then fails to authenticate is an
 * error, never a reason to try the next one. Silently falling through is how a
 * misconfiguration turns into a cache that is merely slow instead of broken.
 */
export const enum CredentialSource {
    Inputs = "action inputs",
    InputFile = "aws-credentials-file input",
    EnvFile = "BP_CACHE_AWS_CREDENTIALS_FILE",
    EnvKeys = "BP_CACHE_AWS_ACCESS_KEY_ID",
    ContainerRole = "container credentials",
    None = "none"
}

export interface ResolvedCredentials {
    source: CredentialSource;
    /**
     * Undefined means "the caller already has what it needs" -- never "let the
     * SDK work it out". A caller that gets `CredentialSource.None` must fail.
     */
    credentials?: AwsCredentialIdentityProvider;
    /** Human-readable detail for the log line. Never contains secret material. */
    detail?: string;
}

function env(name: string): string {
    return (process.env[name] || "").trim();
}

function input(name: string): string {
    return core.getInput(name).trim();
}

/**
 * The container-credential variables a container platform injects. Reading them is not the ambient-credentials problem: they are
 * set by the platform into the container, they cannot be used to point us at a
 * different principal without also moving the endpoint, and they are the only
 * way the cache authenticates when there is no credentials file.
 */
function hasContainerCredentials(): boolean {
    return !!(
        env("AWS_CONTAINER_CREDENTIALS_FULL_URI") ||
        env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
    );
}

export function resolveCredentials(): ResolvedCredentials {
    const inputKeyId = input(Inputs.AwsAccessKeyId);
    const inputSecret = input(Inputs.AwsSecretAccessKey);
    if (inputKeyId && inputSecret) {
        return {
            source: CredentialSource.Inputs,
            credentials: async () => ({
                accessKeyId: inputKeyId,
                secretAccessKey: inputSecret,
                // Carried deliberately. Omitting it is what made temporary
                // credentials unusable before: they sign, and S3 rejects the
                // signature with an error that names the key, not the token.
                sessionToken: input(Inputs.AwsSessionToken) || undefined
            })
        };
    }
    if (inputKeyId || inputSecret) {
        throw new Error(
            "aws-access-key-id and aws-secret-access-key must be set together"
        );
    }

    const inputFile = input(Inputs.AwsCredentialsFile);
    if (inputFile) {
        return fromIniSource(
            CredentialSource.InputFile,
            inputFile,
            input(Inputs.AwsProfile) || undefined
        );
    }

    const envFile = env("BP_CACHE_AWS_CREDENTIALS_FILE");
    if (envFile) {
        return fromIniSource(
            CredentialSource.EnvFile,
            envFile,
            input(Inputs.AwsProfile) || env("BP_CACHE_AWS_PROFILE") || undefined
        );
    }

    const envKeyId = env("BP_CACHE_AWS_ACCESS_KEY_ID");
    const envSecret = env("BP_CACHE_AWS_SECRET_ACCESS_KEY");
    if (envKeyId && envSecret) {
        return {
            source: CredentialSource.EnvKeys,
            credentials: async () => ({
                accessKeyId: envKeyId,
                secretAccessKey: envSecret,
                sessionToken: env("BP_CACHE_AWS_SESSION_TOKEN") || undefined
            })
        };
    }

    if (hasContainerCredentials()) {
        return {
            source: CredentialSource.ContainerRole,
            // Required lazily so a job that never reaches this branch does not
            // pay for loading the provider.
            credentials: async () => {
                if (env("AWS_CONTAINER_CREDENTIALS_FULL_URI")) {
                    const { fromHttp } = await import(
                        "@aws-sdk/credential-provider-http"
                    );
                    return fromHttp({})();
                }
                const { fromContainerMetadata } = await import(
                    "@smithy/credential-provider-imds"
                );
                return fromContainerMetadata({})();
            }
        };
    }

    return { source: CredentialSource.None };
}

function fromIniSource(
    source: CredentialSource,
    filepath: string,
    profile?: string
): ResolvedCredentials {
    return {
        source,
        detail: profile ? `${filepath} (profile ${profile})` : filepath,
        credentials: async () => {
            const { fromIni } = await import(
                "@aws-sdk/credential-provider-ini"
            );
            // `filepath` is absolute and passed explicitly, so neither HOME nor
            // AWS_SHARED_CREDENTIALS_FILE takes part in locating it. `profile`
            // likewise beats AWS_PROFILE.
            return fromIni({ filepath, profile, ignoreCache: true })();
        }
    };
}

/**
 * The region the cache bucket lives in. Distinct from the caller's own
 * `AWS_REGION`, which is read only as a last resort for workflows predating
 * `BP_CACHE_AWS_REGION`; when that is what we end up using, say so, because a
 * a region pointing at the wrong endpoint produces a signature error
 * that looks like a credentials problem.
 */
export function resolveRegion(): { region: string; fromAmbient: boolean } {
    const explicit = input(Inputs.AwsRegion) || env("BP_CACHE_AWS_REGION");
    if (explicit) {
        return { region: explicit, fromAmbient: false };
    }
    return { region: env("AWS_REGION"), fromAmbient: true };
}
