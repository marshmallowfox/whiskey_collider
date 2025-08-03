CREATE UNLOGGED TABLE IF NOT EXISTS users (
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNLOGGED TABLE IF NOT EXISTS event_types (
    id   BIGSERIAL PRIMARY KEY,
    name TEXT       NOT NULL UNIQUE
);

CREATE UNLOGGED TABLE IF NOT EXISTS events (
    id         BIGSERIAL    PRIMARY KEY,
    user_id    BIGINT       NOT NULL,
    type_id    BIGINT       NOT NULL,
    timestamp  TIMESTAMPTZ  NOT NULL,
    metadata   JSONB
);
