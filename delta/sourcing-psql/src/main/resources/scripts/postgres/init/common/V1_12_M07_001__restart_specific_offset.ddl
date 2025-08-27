-- Add the ability to restart from a given offset
ALTER TABLE projection_restarts ADD COLUMN IF NOT EXISTS from_offset bigint DEFAULT 0;