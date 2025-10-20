-- Migration: Add Theme Support to Thema Ads System
-- This migration adds theme tracking columns and creates theme configuration table

-- Add theme_name column to jobs table
ALTER TABLE thema_ads_jobs
ADD COLUMN IF NOT EXISTS theme_name VARCHAR(50) DEFAULT 'singles_day';

-- Add batch_size column to jobs table (if not exists)
ALTER TABLE thema_ads_jobs
ADD COLUMN IF NOT EXISTS batch_size INTEGER DEFAULT 7500;

-- Add theme_name column to job_items table
ALTER TABLE thema_ads_job_items
ADD COLUMN IF NOT EXISTS theme_name VARCHAR(50);

-- Add campaign_id and campaign_name columns to job_items (if not exists)
ALTER TABLE thema_ads_job_items
ADD COLUMN IF NOT EXISTS campaign_id VARCHAR(50),
ADD COLUMN IF NOT EXISTS campaign_name VARCHAR(500);

-- Add theme_name column to input_data table
ALTER TABLE thema_ads_input_data
ADD COLUMN IF NOT EXISTS theme_name VARCHAR(50);

-- Add campaign_id and campaign_name columns to input_data (if not exists)
ALTER TABLE thema_ads_input_data
ADD COLUMN IF NOT EXISTS campaign_id VARCHAR(50),
ADD COLUMN IF NOT EXISTS campaign_name VARCHAR(500);

-- Create theme_configs table for storing active theme per customer/account
CREATE TABLE IF NOT EXISTS theme_configs (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL UNIQUE,
    theme_name VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on theme_configs customer_id
CREATE INDEX IF NOT EXISTS idx_theme_configs_customer_id ON theme_configs(customer_id);

-- Update existing jobs to have 'singles_day' theme
UPDATE thema_ads_jobs
SET theme_name = 'singles_day'
WHERE theme_name IS NULL;

-- Add comment to document supported themes
COMMENT ON COLUMN thema_ads_jobs.theme_name IS 'Supported themes: singles_day, black_friday, cyber_monday, sinterklaas, kerstmis';
COMMENT ON TABLE theme_configs IS 'Stores the active theme configuration per customer account';
