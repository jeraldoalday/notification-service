-- What each recipient has asked to be sent, per topic.
--
-- Keyed by (tenant_id, recipient_id, topic, channel) with no surrogate
-- id, the same shape as `suppressions` and `templates`: a preference has
-- no identity beyond the thing it is an opinion about, and a surrogate id
-- would only mean a caller had to look one up before it could change an
-- answer it already knows the subject of.
--
-- `channel` holds `*` for a preference covering every channel rather than
-- NULL. A NULL in a primary key is not equal to itself in SQLite, so a
-- NULL-channel row could be inserted twice and matched by neither the
-- upsert nor the delete. `*` cannot collide with a real channel, because
-- ChannelKind has no such spelling -- and it cannot collide with a topic
-- either, since Topic refuses a leading `*`.
--
-- `setting` is broken out of `data` because the delivery path reads it
-- and because an operator answering "who has opted out of this?" filters
-- on it. `updated_at` is out for the ordinary reason the other tables'
-- sort columns are: it is what the listing orders by.
--
-- The full record stays in `data`, so adding a field to `Preference`
-- doesn't need a migration.

CREATE TABLE preferences (
    tenant_id    TEXT NOT NULL,
    recipient_id TEXT NOT NULL,
    topic        TEXT NOT NULL,
    channel      TEXT NOT NULL,
    setting      TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    data         TEXT NOT NULL,
    PRIMARY KEY (tenant_id, recipient_id, topic, channel)
);

-- The delivery-path lookup is (tenant_id, recipient_id, topic), which is
-- a prefix of the primary key, so it is already a point query into that
-- index and needs nothing further.
--
-- This index serves the other question: everything one recipient has said,
-- in the order the listing returns it. The primary key cannot, since it
-- orders by topic rather than by time.
CREATE INDEX idx_preferences_recipient
    ON preferences (tenant_id, recipient_id, updated_at DESC);
