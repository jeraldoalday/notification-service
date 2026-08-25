-- Addresses a tenant may no longer send to.
--
-- The primary key is (tenant_id, channel, address_key), which is the same
-- shape as `templates`: no surrogate id, because a suppression has no
-- identity beyond the address it names. That makes the lookup the worker
-- runs before every send a point query on the primary key index, and makes
-- re-suppressing an address a conflict the repository can resolve rather
-- than a duplicate row that leaves two answers to the same question.
--
-- `address_key` holds the normalised form (see
-- RecipientAddress::suppression_key) rather than what the caller typed:
-- matching has to be done on the same value it was stored under, and doing
-- the normalising in one place beats hoping every query site remembers.
-- The full record, including the address as given, lives in `data`.
--
-- `channel` is part of the key rather than derived at read time, because
-- an address only belongs to one channel and two channels' addresses could
-- otherwise collide on the same string.

CREATE TABLE suppressions (
    tenant_id   TEXT NOT NULL,
    channel     TEXT NOT NULL,
    address_key TEXT NOT NULL,
    reason      TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    data        TEXT NOT NULL,
    PRIMARY KEY (tenant_id, channel, address_key)
);

-- Serves the per-tenant listing in the newest-first order it returns; the
-- primary key index can't, since it orders by channel and address.
CREATE INDEX idx_suppressions_tenant ON suppressions (tenant_id, created_at DESC);
