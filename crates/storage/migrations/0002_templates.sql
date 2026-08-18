-- Templates follow the same shape as notifications and recipients: the full
-- entity lives in `data` as JSON, and only the columns the repository needs
-- to key or filter on are broken out.
--
-- The primary key is (tenant_id, id) rather than a surrogate id, because a
-- template's id is the caller-chosen slug that notifications reference via
-- `template_id`, and it only has to be unique within a tenant. That also
-- makes the PK index serve `list_templates`, which scans a single tenant's
-- prefix.

CREATE TABLE templates (
    tenant_id  TEXT NOT NULL,
    id         TEXT NOT NULL,
    channel    TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    data       TEXT NOT NULL,
    PRIMARY KEY (tenant_id, id)
);
