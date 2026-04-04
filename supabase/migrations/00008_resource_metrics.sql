-- Add resource_metrics JSONB column to track Docker build/test resource usage.
-- Stores: build_duration_s, image_size_bytes, test_duration_s, peak_memory_bytes.
-- Using JSONB for flexibility — new metrics can be added without migrations.

ALTER TABLE error_logs ADD COLUMN IF NOT EXISTS resource_metrics JSONB;
ALTER TABLE docker_contexts ADD COLUMN IF NOT EXISTS resource_metrics JSONB;
