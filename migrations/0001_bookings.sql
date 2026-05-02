CREATE TYPE booking_status AS ENUM ('confirmed', 'canceled');

CREATE TABLE bookings (
                          booking_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                          resource_id         UUID NOT NULL,
                          user_id             UUID NOT NULL,
                          resource_name       VARCHAR(255) NOT NULL,
                          resource_location   VARCHAR(255),
                          resource_type       TEXT NOT NULL,
                          starts_at           TIMESTAMPTZ NOT NULL,
                          ends_at             TIMESTAMPTZ NOT NULL,
                          status              booking_status NOT NULL DEFAULT 'confirmed',
                          cancel_reason       TEXT,
                          created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
                          updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

                          CONSTRAINT check_time_range CHECK (starts_at < ends_at)
);

CREATE INDEX idx_bookings_resource_time ON bookings (resource_id, starts_at, ends_at);
CREATE INDEX idx_bookings_user_time ON bookings (user_id, starts_at DESC);
CREATE INDEX idx_bookings_status ON bookings (status);

CREATE TABLE outbox_messages (
                          outbox_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                          booking_id    UUID NOT NULL,
                          topic         TEXT NOT NULL,
                          message_key   TEXT,
                          payload       JSONB NOT NULL,
                          scheduled_at  TIMESTAMPTZ NOT NULL,
                          sent_at       TIMESTAMPTZ,
                          attempts      INT NOT NULL DEFAULT 0,
                          last_error    TEXT,
                          CONSTRAINT fk_outbox_booking
                            FOREIGN KEY (booking_id) REFERENCES bookings(booking_id)
);

CREATE INDEX idx_outbox_pending ON outbox_messages (scheduled_at) WHERE sent_at IS NULL;
CREATE INDEX idx_outbox_booking_pending ON outbox_messages (booking_id) WHERE sent_at IS NULL;
