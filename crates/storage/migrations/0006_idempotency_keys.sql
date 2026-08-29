-- A caller's own name for one submission, so a retry after a dropped
-- connection resolves to the notification the first attempt created
-- instead of sending a second message.
--
-- Broken out of `data` into its own column for the usual reason the other
-- columns here are: it is filtered on. The lookup runs on the submission
-- path, before anything is queued, so it has to be a point query rather
-- than a scan over a tenant's history.
--
-- The index is UNIQUE, and that is what actually enforces idempotency.
-- Checking for an existing key and then inserting is two statements with a
-- gap between them, and two of the client's own retries can arrive
-- concurrently -- that is the situation retries exist for. The unique
-- index closes the gap: the second insert fails, the repository reports it
-- as a duplicate, and the caller that lost the race reads back the
-- notification the winner created.
--
-- It is partial (`WHERE idempotency_key IS NOT NULL`) because most
-- notifications carry no key: SQLite treats NULLs as distinct in a unique
-- index anyway, but excluding them keeps the index to the rows that can
-- actually be looked up in it.
--
-- Keyed by (tenant_id, idempotency_key), never by the key alone. It is a
-- string one caller picked, and two tenants both submitting `order-1041`
-- have said nothing about each other.
ALTER TABLE notifications ADD COLUMN idempotency_key TEXT;

CREATE UNIQUE INDEX idx_notifications_idempotency
    ON notifications (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
