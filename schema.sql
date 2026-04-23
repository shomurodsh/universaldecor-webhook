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
-- Нормализованные сделки
CREATE TABLE IF NOT EXISTS leads (
    amo_id                BIGINT PRIMARY KEY,
    name                  TEXT,
    pipeline_id           BIGINT,
    status_id             BIGINT,
    price                 NUMERIC DEFAULT 0,
    responsible_user_id   BIGINT,
    created_user_id       BIGINT,
    modified_user_id      BIGINT,
    created_at            TIMESTAMPTZ,
    updated_at            TIMESTAMPTZ,
    account_id            BIGINT,
    raw_payload           JSONB,
    synced_at             TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_leads_status     ON leads(status_id);
CREATE INDEX IF NOT EXISTS idx_leads_pipeline   ON leads(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_leads_user       ON leads(responsible_user_id);
CREATE INDEX IF NOT EXISTS idx_leads_created    ON leads(created_at);