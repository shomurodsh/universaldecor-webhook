CREATE TABLE IF NOT EXISTS amo_webhook_raw (
    id          BIGSERIAL PRIMARY KEY,
    received_at TIMESTAMPTZ DEFAULT NOW(),
    entity      VARCHAR(50),
    action      VARCHAR(50),
    payload     JSONB,
    processed   BOOLEAN DEFAULT FALSE,
    error       TEXT
);

CREATE INDEX IF NOT EXISTS idx_webhook_raw_processed ON amo_webhook_raw(processed);
CREATE INDEX IF NOT EXISTS idx_webhook_raw_received_at ON amo_webhook_raw(received_at);