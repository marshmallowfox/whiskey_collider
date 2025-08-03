-- Indexes
CREATE INDEX IF NOT EXISTS idx_events_count
    ON events USING btree (id);

CREATE INDEX IF NOT EXISTS idx_events_user_timestamp
    ON events USING btree (user_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_events_timestamp_desc
    ON events USING btree (timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_events_type_timestamp
    ON events USING btree (type_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_events_stats
    ON events USING btree (user_id, (metadata->>'page'), type_id);

CREATE INDEX IF NOT EXISTS idx_events_covering
    ON events USING btree (user_id, type_id, timestamp DESC)
    INCLUDE (id, metadata);