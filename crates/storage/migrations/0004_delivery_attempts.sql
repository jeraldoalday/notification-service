-- One row per delivery attempt, append-only: nothing rewrites an attempt
-- after it is recorded, which is what makes the sequence readable as
-- history rather than as a summary that happens to be stale.
--
-- This is expected to be the highest-volume table in the database -- a
-- notification with max_attempts = 5 that also gets rate limited can
-- produce a dozen rows on its own -- so it carries only the columns the
-- repository filters, orders or purges on, and the full record lives in
-- `data` like every other entity here.
--
-- There is deliberately no foreign key to `notifications`. The attempt log
-- is the record of what the service did, and it has to survive its
-- subject: purging delivered notifications after 30 days should not
-- silently take the evidence of how they were delivered with it.

CREATE TABLE delivery_attempts (
    id              TEXT PRIMARY KEY NOT NULL,
    notification_id TEXT NOT NULL,
    tenant_id       TEXT NOT NULL,
    result          TEXT NOT NULL,
    occurred_at     TEXT NOT NULL,
    data            TEXT NOT NULL
);

-- Serves the per-notification history in the chronological order it is
-- read in. This is the query the API and CLI both run.
CREATE INDEX idx_delivery_attempts_notification
    ON delivery_attempts (notification_id, occurred_at);

-- Serves both the per-tenant listing and the retention purge, which scans
-- by time. Tenant leads because every read is tenant-scoped; a purge with
-- no tenant filter walks the index rather than the table.
CREATE INDEX idx_delivery_attempts_tenant
    ON delivery_attempts (tenant_id, occurred_at DESC);
