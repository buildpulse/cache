import { CacheFailure, classify } from "../src/cacheErrors";
import {
    CredentialSource,
    resolveCredentials,
    resolveRegion
} from "../src/credentials";

/**
 * These exist because the bug they cover is a RELATIONSHIP, not a unit: the
 * cache must end up with the credentials the runner gave it, whatever else is
 * in the environment. Testing the pieces separately is what let three versions
 * ship with the ambient environment winning.
 */

const KEEP = { ...process.env };

function setEnv(vars: Record<string, string | undefined>): void {
    for (const k of Object.keys(process.env)) {
        if (
            k.startsWith("AWS_") ||
            k.startsWith("BP_CACHE_") ||
            k.startsWith("INPUT_")
        ) {
            delete process.env[k];
        }
    }
    for (const [k, v] of Object.entries(vars)) {
        if (v !== undefined) {
            process.env[k] = v;
        }
    }
}

/** core.getInput reads INPUT_<NAME>, uppercased with spaces as underscores. */
function setInput(name: string, value: string): void {
    process.env[`INPUT_${name.replace(/ /g, "_").toUpperCase()}`] = value;
}

afterAll(() => {
    process.env = KEEP;
});

describe("credential resolution is immune to the ambient AWS environment", () => {
    // The customer's own credentials, exactly as aws-actions/configure-aws-credentials
    // exports them. Present in every case below.
    const CUSTOMER = {
        AWS_ACCESS_KEY_ID: "ASIACUSTOMERKEY",
        AWS_SECRET_ACCESS_KEY: "customer-secret",
        AWS_SESSION_TOKEN: "customer-token",
        AWS_REGION: "eu-central-1"
    };

    it("prefers the runner's credentials file over the customer's env keys", () => {
        setEnv({
            ...CUSTOMER,
            BP_CACHE_AWS_CREDENTIALS_FILE:
                "/opt/buildpulse/aws/cache-credentials",
            BP_CACHE_AWS_PROFILE: "buildpulse-cache"
        });
        const r = resolveCredentials();
        expect(r.source).toBe(CredentialSource.EnvFile);
        expect(r.detail).toContain("/opt/buildpulse/aws/cache-credentials");
        expect(r.detail).toContain("buildpulse-cache");
    });

    it("prefers the runner's prefixed keys over the customer's env keys", () => {
        setEnv({
            ...CUSTOMER,
            BP_CACHE_AWS_ACCESS_KEY_ID: "AKIARUNNERKEY",
            BP_CACHE_AWS_SECRET_ACCESS_KEY: "runner-secret"
        });
        expect(resolveCredentials().source).toBe(CredentialSource.EnvKeys);
    });

    it("prefers container credentials over the customer's env keys", () => {
        setEnv({
            ...CUSTOMER,
            AWS_CONTAINER_CREDENTIALS_FULL_URI:
                "http://169.254.170.23/v1/credentials"
        });
        expect(resolveCredentials().source).toBe(
            CredentialSource.ContainerRole
        );
    });

    it("never uses the customer's env keys, even with nothing else set", () => {
        setEnv({ ...CUSTOMER });
        expect(resolveCredentials().source).toBe(CredentialSource.None);
    });

    it("lets action inputs win over everything", () => {
        setEnv({
            ...CUSTOMER,
            BP_CACHE_AWS_CREDENTIALS_FILE:
                "/opt/buildpulse/aws/cache-credentials"
        });
        setInput("aws-access-key-id", "AKIAINPUT");
        setInput("aws-secret-access-key", "input-secret");
        expect(resolveCredentials().source).toBe(CredentialSource.Inputs);
    });

    it("rejects a half-configured input pair rather than falling through", () => {
        setEnv({});
        setInput("aws-access-key-id", "AKIAINPUT");
        expect(() => resolveCredentials()).toThrow(/must be set together/);
    });

    it("carries a session token, which is what makes temporary credentials work", async () => {
        setEnv({
            BP_CACHE_AWS_ACCESS_KEY_ID: "ASIARUNNER",
            BP_CACHE_AWS_SECRET_ACCESS_KEY: "runner-secret",
            BP_CACHE_AWS_SESSION_TOKEN: "runner-token"
        });
        const resolved = resolveCredentials();
        const creds = await resolved.credentials!();
        expect(creds.sessionToken).toBe("runner-token");
    });
});

describe("region", () => {
    it("prefers the cache's own region over the customer's", () => {
        setEnv({
            AWS_REGION: "eu-central-1",
            BP_CACHE_AWS_REGION: "us-west-2"
        });
        expect(resolveRegion()).toEqual({
            region: "us-west-2",
            fromAmbient: false
        });
    });

    it("falls back to the ambient region and says so", () => {
        setEnv({ AWS_REGION: "eu-central-1" });
        expect(resolveRegion()).toEqual({
            region: "eu-central-1",
            fromAmbient: true
        });
    });
});

describe("a denied cache is not a cache miss", () => {
    it.each([
        ["AccessDenied", CacheFailure.Auth],
        ["InvalidAccessKeyId", CacheFailure.Auth],
        ["ExpiredToken", CacheFailure.Auth],
        ["SignatureDoesNotMatch", CacheFailure.Auth],
        ["CredentialsProviderError", CacheFailure.Auth],
        ["NoSuchKey", CacheFailure.Miss],
        ["NotFound", CacheFailure.Miss],
        ["TimeoutError", CacheFailure.Other]
    ])("classifies %s", (name, expected) => {
        expect(classify(Object.assign(new Error("boom"), { name }))).toBe(
            expected
        );
    });

    it("classifies by HTTP status when the name is unhelpful", () => {
        expect(
            classify({ name: "Unknown", $metadata: { httpStatusCode: 403 } })
        ).toBe(CacheFailure.Auth);
        expect(
            classify({ name: "Unknown", $metadata: { httpStatusCode: 404 } })
        ).toBe(CacheFailure.Miss);
    });

    it("still recognises an error that was stringified into a plain Error", () => {
        expect(
            classify(
                new Error("Failed to download: AccessDenied: Access Denied")
            )
        ).toBe(CacheFailure.Auth);
    });
});
