-- Index a tenant's notifications in the order they are listed in.
--
-- `list_by_tenant` is `WHERE tenant_id = ? ORDER BY created_at DESC`, and
-- the index it had covered only the `WHERE`. SQLite could find the
-- tenant's rows through it but then had to load and sort every one of them
-- to answer a fifty-row page -- work proportional to everything the tenant
-- has ever sent, on a query whose whole point is to look at the newest
-- handful. That is the shape every other listing here already avoids:
-- api_keys, suppressions and delivery_attempts all carry their sort column
-- in the index.
--
-- Two indexes rather than one, because the filtered and unfiltered
-- listings need different prefixes. An index on (tenant_id, status,
-- created_at) cannot produce created_at order for a query that doesn't
-- constrain status -- the rows come out grouped by status first -- so the
-- unfiltered listing would be back to sorting.
DROP INDEX idx_notifications_tenant;

CREATE INDEX idx_notifications_tenant
    ON notifications (tenant_id, created_at DESC);

CREATE INDEX idx_notifications_tenant_status
    ON notifications (tenant_id, status, created_at DESC);
