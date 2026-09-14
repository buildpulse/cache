export enum Inputs {
    Key = "key", // Input for cache, restore, save action
    Path = "path", // Input for cache, restore, save action
    RestoreKeys = "restore-keys", // Input for cache, restore action
    UploadChunkSize = "upload-chunk-size", // Input for cache, save action
    EnableCrossOsArchive = "enableCrossOsArchive", // Input for cache, restore, save action
    FailOnCacheMiss = "fail-on-cache-miss", // Input for cache, restore action
    LookupOnly = "lookup-only", // Input for cache, restore action

    // Credentials for the cache's S3 backend. Every one of these is optional:
    // when the runner supplies credentials the action finds them on its own,
    // and these exist for the cases it cannot -- a self-hosted setup, a
    // different account, or a workflow that needs to be explicit because its
    // own AWS configuration would otherwise be ambiguous. See src/credentials.ts
    // for the resolution order.
    AwsAccessKeyId = "aws-access-key-id", // Input for cache, restore, save action
    AwsSecretAccessKey = "aws-secret-access-key", // Input for cache, restore, save action
    AwsSessionToken = "aws-session-token", // Input for cache, restore, save action
    AwsCredentialsFile = "aws-credentials-file", // Input for cache, restore, save action
    AwsProfile = "aws-profile", // Input for cache, restore, save action
    AwsRegion = "aws-region", // Input for cache, restore, save action
    OnCacheError = "on-cache-error" // Input for cache, restore, save action
}

export enum Outputs {
    CacheHit = "cache-hit", // Output from cache, restore action
    CachePrimaryKey = "cache-primary-key", // Output from restore action
    CacheMatchedKey = "cache-matched-key", // Output from restore action
    CacheError = "cache-error" // Output from cache, restore, save action
}

export enum State {
    CachePrimaryKey = "CACHE_KEY",
    CacheMatchedKey = "CACHE_RESULT"
}

export enum Events {
    Key = "GITHUB_EVENT_NAME",
    Push = "push",
    PullRequest = "pull_request"
}

export const RefKey = "GITHUB_REF";
