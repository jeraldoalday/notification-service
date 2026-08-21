-- API keys follow the same shape as the other entities: the full record
-- lives in `data` as JSON, with only the columns the repository keys or
-- filters on broken out.
--
-- `token_digest` is UNIQUE for two reasons at once. It is the column
-- authentication looks a presented token up by, and the uniqueness index
-- is what makes that lookup a point query rather than a scan of every key
-- the deployment has ever issued. It also makes a duplicate digest --
-- which would mean two tenants' keys authenticating to each other, or far
-- more likely a bug in key generation -- fail loudly at the insert rather
-- than silently resolving to whichever row came first.
--
-- The clear token is never stored. There is no column for it here and no
-- field for it on `ns_auth::ApiKey`.

CREATE TABLE api_keys (
    id           TEXT PRIMARY KEY NOT NULL,
    tenant_id    TEXT NOT NULL,
    token_digest TEXT NOT NULL UNIQUE,
    created_at   TEXT NOT NULL,
    revoked_at   TEXT,
    data         TEXT NOT NULL
);

-- Serves the per-tenant listing, in the newest-first order it returns.
CREATE INDEX idx_api_keys_tenant ON api_keys (tenant_id, created_at DESC);
