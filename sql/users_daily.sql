CREATE TABLE IF NOT EXISTS users_daily (
    id INTEGER NOT NULL,
    name TEXT NOT NULL,
    dt DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ensure idempotency: one record per user per day
ALTER TABLE users_daily
ADD CONSTRAINT uniq_users_daily_id_dt UNIQUE (id, dt);

-- Improve performance for date-based deletes and queries
CREATE INDEX IF NOT EXISTS idx_users_daily_dt
ON users_daily (dt);
