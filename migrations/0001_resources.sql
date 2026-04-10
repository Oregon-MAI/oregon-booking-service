CREATE TYPE resource_type AS ENUM ('meeting_room', 'workspace', 'device');
CREATE TYPE resource_status AS ENUM ('available', 'occupied', 'maintenance', 'emergency');

CREATE TABLE resources (
                           uuid       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                           name       VARCHAR(255) NOT NULL,
                           type       resource_type NOT NULL,
                           location   VARCHAR(255),
                           status     resource_status NOT NULL DEFAULT 'available',
                           created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                           updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_resources_type ON resources(type);
CREATE INDEX idx_resources_status ON resources(status);
CREATE INDEX idx_resources_type_status ON resources(type, status);

CREATE TABLE meeting_rooms (
                               resource_uuid  UUID PRIMARY KEY REFERENCES resources(uuid) ON DELETE CASCADE,
                               capacity       SMALLINT NOT NULL CHECK (capacity > 0),
                               has_projector  BOOLEAN NOT NULL DEFAULT false,
                               has_whiteboard BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE workspaces (
                            resource_uuid UUID PRIMARY KEY REFERENCES resources(uuid) ON DELETE CASCADE,
                            has_monitor   BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE devices (
                         resource_uuid UUID PRIMARY KEY REFERENCES resources(uuid) ON DELETE CASCADE,
                         device_type   VARCHAR(100) NOT NULL,
                         serial_number VARCHAR(255) UNIQUE,
                         model         VARCHAR(100),
                         description   TEXT
);
