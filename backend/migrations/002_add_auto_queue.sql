-- Migration: Add auto-queue functionality
-- Date: 2025-10-20

-- Add system settings table for queue state
CREATE TABLE IF NOT EXISTS system_settings (
    id SERIAL PRIMARY KEY,
    setting_key VARCHAR(100) UNIQUE NOT NULL,
    setting_value TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert default auto_queue_enabled setting (disabled by default)
INSERT INTO system_settings (setting_key, setting_value)
VALUES ('auto_queue_enabled', 'false')
ON CONFLICT (setting_key) DO NOTHING;

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_system_settings_key ON system_settings(setting_key);
