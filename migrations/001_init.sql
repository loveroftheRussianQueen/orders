CREATE TABLE IF NOT EXISTS orders (
    id         BIGSERIAL PRIMARY KEY,
    user_id    BIGINT NOT NULL,
    amount     NUMERIC(10, 2) NOT NULL,
    status     VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox (
    id         BIGSERIAL PRIMARY KEY,
    topic      VARCHAR(100) NOT NULL,
    key        VARCHAR(100),
    payload    JSONB NOT NULL,
    sent       BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- partial index: only unsent rows, keeps the scan fast as table grows
CREATE INDEX IF NOT EXISTS idx_outbox_unsent ON outbox (id) WHERE sent = FALSE;

-- idempotency table: consumer writes event_id here before processing
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     VARCHAR(200) PRIMARY KEY,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
