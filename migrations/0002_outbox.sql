CREATE TABLE IF NOT EXISTS outbox_messages (
                          outbox_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                          booking_id    UUID NOT NULL,
                          topic         TEXT NOT NULL,
                          message_key   TEXT,
                          payload       JSONB NOT NULL,
                          scheduled_at  TIMESTAMPTZ NOT NULL,
                          sent_at       TIMESTAMPTZ,
                          attempts      INT NOT NULL DEFAULT 0,
                          last_error    TEXT
);

ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS booking_id UUID;

CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_messages (scheduled_at) WHERE sent_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_outbox_booking_pending ON outbox_messages (booking_id) WHERE sent_at IS NULL;

