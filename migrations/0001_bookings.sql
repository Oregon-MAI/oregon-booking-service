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