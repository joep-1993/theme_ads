-- Thema Ads Processing Job Tracking Schema

-- Jobs table: tracks each processing job
CREATE TABLE IF NOT EXISTS thema_ads_jobs (
    id SERIAL PRIMARY KEY,
    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- pending, running, paused, completed, failed
    total_ad_groups INTEGER DEFAULT 0,
    processed_ad_groups INTEGER DEFAULT 0,
    successful_ad_groups INTEGER DEFAULT 0,
    failed_ad_groups INTEGER DEFAULT 0,
    input_file VARCHAR(255),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT,
    is_repair_job BOOLEAN DEFAULT FALSE
);

-- Job items: tracks each individual ad group being processed
CREATE TABLE IF NOT EXISTS thema_ads_job_items (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES thema_ads_jobs(id) ON DELETE CASCADE,
    customer_id VARCHAR(50) NOT NULL,
    ad_group_id VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- pending, successful, failed, skipped
    new_ad_resource VARCHAR(500),
    error_message TEXT,
    processed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Input data: stores uploaded CSV/Excel data
CREATE TABLE IF NOT EXISTS thema_ads_input_data (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES thema_ads_jobs(id) ON DELETE CASCADE,
    customer_id VARCHAR(50) NOT NULL,
    ad_group_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_job_items_job_id ON thema_ads_job_items(job_id);
CREATE INDEX IF NOT EXISTS idx_job_items_status ON thema_ads_job_items(status);
CREATE INDEX IF NOT EXISTS idx_input_data_job_id ON thema_ads_input_data(job_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON thema_ads_jobs(status);

-- Activation Plan: stores which theme should be active per customer
CREATE TABLE IF NOT EXISTS activation_plan (
    customer_id VARCHAR(50) PRIMARY KEY,
    theme_name VARCHAR(50) NOT NULL,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    plan_version VARCHAR(50) DEFAULT '1',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Activation Missing Ads: tracks ad groups missing required theme ads
CREATE TABLE IF NOT EXISTS activation_missing_ads (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    campaign_id VARCHAR(50),
    campaign_name TEXT,
    ad_group_id VARCHAR(50) NOT NULL,
    ad_group_name TEXT,
    required_theme VARCHAR(50) NOT NULL,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for activation tables
CREATE INDEX IF NOT EXISTS idx_activation_plan_customer ON activation_plan(customer_id);
CREATE INDEX IF NOT EXISTS idx_activation_missing_customer ON activation_missing_ads(customer_id);
CREATE INDEX IF NOT EXISTS idx_activation_missing_theme ON activation_missing_ads(required_theme);
