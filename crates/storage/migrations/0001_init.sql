-- Notifications and recipients are stored as full JSON documents in `data`,
-- so ns-storage doesn't need to keep a hand-written column-per-field mapping
-- in sync with ns-core. The columns alongside `data` are the ones the
-- repository actually needs to filter or order on (e.g. picking due jobs);
-- everything else is read back through deserialization.

CREATE TABLE recipients (
    id   TEXT PRIMARY KEY NOT NULL,
    data TEXT NOT NULL
);

CREATE TABLE notifications (
    id            TEXT PRIMARY KEY NOT NULL,
    tenant_id     TEXT NOT NULL,
    status        TEXT NOT NULL,
    priority      INTEGER NOT NULL,
    scheduled_for TEXT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    data          TEXT NOT NULL
);

CREATE INDEX idx_notifications_due
    ON notifications (status, scheduled_for, priority DESC, created_at);

CREATE INDEX idx_notifications_tenant
    ON notifications (tenant_id);
