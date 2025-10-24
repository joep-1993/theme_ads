# PROJECT INDEX
_Technical reference for the project. Update when: architecture changes, new patterns emerge._

## Architecture

### System Overview
- **Backend**: FastAPI with async processing
- **Database**: PostgreSQL for job persistence
- **Google Ads**: API v28+ integration
- **Processing**: Batch operations with pause/resume
- **Quality Assurance**: Check-up function audits processed ad groups, verifies ad integrity, creates repair jobs

### Key Components
- `backend/main.py` - FastAPI API endpoints (CSV upload, Excel upload, auto-discovery, checkup, queue management)
  - `/api/thema-ads/upload` - CSV upload and job creation (legacy, defaults to singles_day theme)
  - `/api/thema-ads/upload-excel` - Excel upload with theme column support
  - `/api/thema-ads/discover` - Auto-discover ad groups from MCC (with theme parameter)
  - `/api/thema-ads/themes` - Get list of supported themes
  - `/api/thema-ads/checkup` - Audit processed ad groups, verify theme ads exist (multi-theme aware)
  - `/api/thema-ads/run-all-themes` - Discovery with theme selection (uses Query(None) for proper repeated param parsing)
  - `/api/thema-ads/queue/status` - Get auto-queue enabled state
  - `/api/thema-ads/queue/enable` - Enable automatic job queue
  - `/api/thema-ads/queue/disable` - Disable automatic job queue
- `backend/thema_ads_service.py` - Business logic and job processing
  - `discover_all_missing_themes()` - Discovery with batch queries and theme filtering (lines 947-1340)
  - Discovery job creation (lines 1302-1318): Assigns theme_name to each ad group item before creating jobs to ensure proper theme tracking; without this field, create_job() falls back to 'singles_day' default
  - `checkup_ad_groups()` - Database-driven multi-theme checkup: queries job_items for theme_name, checks theme-specific labels (THEME_BF, THEME_CM, etc.), creates repair jobs with correct theme
  - `get_next_pending_job()` - Returns oldest pending job ID (FIFO)
  - `_start_next_job_if_queue_enabled()` - Auto-queue logic: waits 30s, checks queue state, starts next job
- `backend/database.py` - Database connection management, auto-queue state persistence
  - `get_auto_queue_enabled()` - Retrieve queue toggle state from database
  - `set_auto_queue_enabled(bool)` - Persist queue toggle state
- `frontend/thema-ads.html` - Web UI with 3 tabs (Excel Upload, CSV Upload, Auto-Discover, Check-up) and auto-queue toggle
- `frontend/js/thema-ads.js` - Frontend logic including uploadExcel(), runCheckup(), theme loading, auto-queue toggle (loadQueueStatus(), toggleAutoQueue())
- `themes/` - Theme content directory (black_friday/, cyber_monday/, sinterklaas/, kerstmis/)
  - Each theme has headlines.txt and descriptions.txt files
- `thema_ads_optimized/themes.py` - Theme management module (load content, get labels, validate themes)
- `thema_ads_optimized/account ids` - Whitelist of 28 active customer IDs (discovery loads from this file)
- `thema_ads_optimized/` - CLI automation tools
- `thema_ads_optimized/operations/` - Google Ads API operations
  - `rsa_management.py` - Smart RSA slot management for 3-ad limit (not yet integrated)
- `thema_ads_optimized/processors/` - Data processing logic
- `thema_ads_optimized/models.py` - Data models (AdGroupInput with theme_name field)

## Technology Stack

### Backend
- **FastAPI** - Web framework with async support
- **PostgreSQL** - Job state and progress tracking
- **Google Ads API v28** - Ad operations
- **Python asyncio** - Parallel processing

### Infrastructure
- **Docker** - Containerization
- **docker-compose** - Multi-container orchestration

## Key Patterns

### Performance Optimizations
1. **Batch API Operations** - Reduce API calls by batching (100 ads per creation batch to prevent crawler overload)
2. **Async Processing** - Parallel customer processing with semaphore control (5 concurrent customers)
3. **Prefetch Strategy** - Load all data upfront to eliminate redundant API calls
4. **Direct Ad Query** - 74% fewer queries using cross-resource filtering
5. **Customer Account Whitelisting** - Use static file-based customer ID list instead of dynamic MCC query to avoid CANCELED accounts (eliminates permission errors, faster discovery)
6. **Automatic Job Chunking** - Large discoveries split into optimal-sized jobs (default 50k items/job, configurable 10k-100k)
7. **API Quota Optimization** - Reduced from 6 to 4 operations per ad group (33% savings):
   - Disabled THEMA_AD label on new ads
   - Disabled BF_2025 label on ad groups
   - Kept essential labels: SINGLES_DAY (new ad), THEMA_ORIGINAL (old ad), SD_DONE (ad group)
8. **Google Crawler Rate Limiting Prevention** - Small batches to prevent DESTINATION_NOT_WORKING errors:
   - Ad creation batch size: 100 (down from 10,000)
   - Batch delays: 5s between ad creation batches
   - Prevents CloudFront from blocking Google's policy crawler
9. **Rate Limiting** - Multi-layer approach to prevent 503 errors:
   - Query batch size: 5000 (reduced from 7500)
   - Customer delays: 30s between customers
   - Batch delays: 2s between API queries
   - Concurrency: 5 max concurrent customers (reduced from 10)
   - Job chunking: 50k items per job max
   - Operation reduction: 4 ops/ad group (from 6)
10. **Extended 503 Retry Logic** - Exponential backoff with long waits (60s, 180s, 540s, 1620s) for Service Unavailable errors
11. **CONCURRENT_MODIFICATION Retry Handling** - Jittered exponential backoff (5s→80s with ±20% variance)
   - Detects database_error: CONCURRENT_MODIFICATION specifically
   - Prevents thundering herd with random delays to avoid simultaneous retries
   - Eliminated 40/97 failures in Job 338 (41% failure rate → 0%)
   - Longer base delays (5s, 10s, 20s, 40s, 80s) vs standard (2s, 4s, 8s)
12. **Batch Query Discovery Optimization** - Eliminate N+1 queries in discovery (99.9% reduction)
   - Batch fetch ad group labels, ads, and ad labels using IN clauses (5000 per batch)
   - Use dictionary lookups for O(1) resource→ID mapping instead of O(n) linear search
   - Reduced 50,000 queries to ~30 queries in all-themes discovery
   - Discovery time: 8+ hours → 5-10 minutes for 10,000 ad groups (99x faster)

### Reliability
1. **Idempotent Processing** - SD_DONE labels prevent duplicate processing
2. **Quality Verification** - Multi-theme checkup function (database-driven)
   - Queries thema_ads_job_items for theme_name of each processed ad group
   - Checks for theme-specific labels (THEME_BF, THEME_CM, THEME_SK, THEME_KM, THEME_SD)
   - Creates repair jobs with correct theme_name for ad groups missing their theme ads
   - Supports all themes: Black Friday, Cyber Monday, Sinterklaas, Kerstmis, Singles Day
3. **Audit/Repair Workflow** - Checkup function for quality assurance
   - Pattern: Audit processed items → Verify theme labels → Create repair jobs for missing ads
   - Use case: Ensure all processed ad groups have their theme-specific ads (database-driven verification)
   - Implementation: Query database for theme per ad group, check theme labels in Google Ads API, create repair jobs
   - Repair Job Flag: Jobs created by checkup have `is_repair_job=True` to bypass SD_DONE skip logic
     - Database: `thema_ads_jobs.is_repair_job BOOLEAN DEFAULT FALSE`
     - Processor: `ThemaAdsProcessor(config, skip_sd_done_check=is_repair_job)`
     - Allows reprocessing of items that already have SD_DONE label
4. **Automatic Job Queue** - FIFO queue for unattended processing
   - Toggle state stored in `system_settings` table (survives restarts)
   - When enabled: Jobs auto-start after current job completes (30s delay)
   - Failed job handling: Queue continues even if jobs fail
   - Manual control: UI toggle switch in job management section
   - Use case: Queue multiple discoveries/uploads, let system process overnight
5. **State Persistence** - PostgreSQL tracks job and item status for resume capability
6. **Background Tasks** - FastAPI BackgroundTasks for long-running jobs
7. **Error Handling** - Distinguish between failed, skipped, and successful items

### API Integration
1. **Configurable Batch Size** - User-adjustable (1000-10000, default: 5000) for rate limiting or performance
2. **CSV Flexibility** - Support minimal or full CSV formats
3. **Excel Compatibility** - Handle scientific notation and encoding issues
4. **Ad Group Name Lookups** - Resolve IDs from names to avoid Excel precision loss

## Database Schema

### Multi-Theme Support
- `thema_ads_jobs.theme_name` VARCHAR(50) - Theme for job (NO DEFAULT VALUE - must be explicitly set during job creation; database default removed 2025-10-24 to prevent incorrect theme assignment via fallback; theme must be provided in create_job() call)
- `thema_ads_job_items.theme_name` VARCHAR(50) - Theme for specific ad group
- `thema_ads_input_data.theme_name` VARCHAR(50) - Theme from original upload
- `system_settings` table - Store system-wide configuration (auto-queue state)
  - `setting_key` VARCHAR(100) UNIQUE - Setting identifier (e.g., 'auto_queue_enabled')
  - `setting_value` TEXT - Setting value (e.g., 'true' or 'false')
  - `updated_at` TIMESTAMP - Last modification time
- `theme_configs` table - Store active theme configuration per customer (future use)
  - `customer_id` VARCHAR(50) UNIQUE - Customer account ID
  - `theme_name` VARCHAR(50) - Active theme for customer
  - `updated_at`, `created_at` TIMESTAMP - Tracking fields

### Supported Themes
- `black_friday` - Label: THEME_BF, Display: "Black Friday", Countdown: 2025-11-28
- `cyber_monday` - Label: THEME_CM, Display: "Cyber Monday", Countdown: 2025-12-01
- `sinterklaas` - Label: THEME_SK, Display: "Sinterklaas", Countdown: 2025-12-05
- `kerstmis` - Label: THEME_KM, Display: "Kerstmis", Countdown: 2025-12-25
- `singles_day` - Label: THEME_SD, Display: "Singles Day", Countdown: 2025-11-11 (legacy)

**Important - RSA Countdown Syntax:**
- RSA countdown format differs from standard Google Ads format
- **RSA Format**: `{COUNTDOWN(yyyy-MM-dd HH:mm:ss,daysBefore)}`
- **Example**: `{COUNTDOWN(2025-11-28 00:00:00,5)}`
- **NOT** standard format: `{=COUNTDOWN("yyyy/MM/dd HH:mm:ss","language")}` ❌
- Key differences:
  - No `=` sign after opening brace
  - Date uses dashes (ISO format), not slashes
  - No quotes around parameters
  - Uses `daysBefore` parameter (integer) instead of language code
- All theme template files use RSA format in headlines.txt and descriptions.txt

## Operations

### Database Operations

**Bulk Job Deletion** - Clear all jobs from database (testing/recovery)
```bash
# Use case: Clear failed jobs after fixing errors, start fresh
docker-compose up -d  # Ensure containers are running
docker-compose exec -T db psql -U postgres -d thema_ads -c "DELETE FROM thema_ads_jobs;"
# Cascades to thema_ads_job_items automatically
```
- **When to use**: After fixing critical bugs (e.g., countdown syntax), before fresh run
- **Example**: Session 2025-10-17 - Removed 13 failed jobs after countdown syntax fix
- **Note**: Deleting jobs does NOT affect Google Ads data; only clears local processing state

## Configuration

### Environment Variables
- `GOOGLE_DEVELOPER_TOKEN` - Google Ads API developer token
- `GOOGLE_REFRESH_TOKEN` - OAuth refresh token
- `GOOGLE_CLIENT_ID` - OAuth client ID
- `GOOGLE_CLIENT_SECRET` - OAuth client secret
- `GOOGLE_LOGIN_CUSTOMER_ID` - MCC account ID
- `MAX_CONCURRENT_CUSTOMERS` - Parallel customer processing limit (default: 5)
- `BATCH_SIZE` - Items per API query (default: 5000)
- `API_RETRY_ATTEMPTS` - Retry attempts for failed API calls (default: 5)
- `API_RETRY_DELAY` - Initial retry delay in seconds (default: 2.0)
- `API_BATCH_DELAY` - Delay between API batches in seconds (default: 2.0)
- `CUSTOMER_DELAY` - Delay between processing customers in seconds (default: 30.0)

### API Parameters (frontend-configurable)
- `batch_size` - Items per API query (default: 5000, range: 1000-10000)
- `job_chunk_size` - Max items per job for auto-discovery (default: 50000, range: 10000-100000)

### Performance Tuning
- **For speed**:
  - Increase `BATCH_SIZE` to 7500-10000
  - Reduce `CUSTOMER_DELAY` to 10-15s
  - Increase `job_chunk_size` to 80000-100000
- **For stability**:
  - Keep defaults (BATCH_SIZE=5000, CUSTOMER_DELAY=30s, job_chunk_size=50000)
- **For rate-limited scenarios**:
  - Reduce `BATCH_SIZE` to 1000-3000
  - Increase `CUSTOMER_DELAY` to 60s
  - Reduce `job_chunk_size` to 10000-20000

## External Dependencies

### APIs
- Google Ads API v28+
- PostgreSQL database

### Libraries
- fastapi
- google-ads-python
- psycopg2
- python-dotenv
- openpyxl (Excel file parsing)

## File Structure
```
theme_ads/
├── backend/
│   ├── main.py                     # API endpoints (Excel upload, CSV upload, discover, checkup, themes)
│   ├── thema_ads_service.py        # Business logic (includes checkup_ad_groups, multi-theme support)
│   ├── database.py                 # DB connection
│   ├── thema_ads_schema.sql        # DB schema (includes is_repair_job, theme_name columns)
│   └── migrations/
│       └── add_theme_support.sql   # Migration for multi-theme system
├── themes/                         # Theme content directory
│   ├── black_friday/
│   │   ├── headlines.txt           # 15 headlines with RSA COUNTDOWN syntax
│   │   └── descriptions.txt        # 4 descriptions with RSA COUNTDOWN syntax
│   ├── cyber_monday/               # Countdown: 2025-12-01 00:00:00
│   │   ├── headlines.txt
│   │   └── descriptions.txt
│   ├── sinterklaas/                # Countdown: 2025-12-05 00:00:00
│   │   ├── headlines.txt
│   │   └── descriptions.txt
│   └── kerstmis/                   # Countdown: 2025-12-25 00:00:00
│       ├── headlines.txt
│       └── descriptions.txt
│   # Note: All template files use RSA countdown format: {COUNTDOWN(yyyy-MM-dd HH:mm:ss,daysBefore)}
├── delete_sd_checked_labels.py     # Utility: Delete SD_CHECKED labels from all accounts
├── remove_singles_day_ads_batch.py # Utility: Remove SINGLES_DAY ads and SD_DONE labels
├── frontend/
│   ├── thema-ads.html              # Web UI (4 tabs: Excel Upload, CSV Upload, Auto-Discover, Check-up)
│   └── js/
│       └── thema-ads.js            # Frontend logic (uploadExcel, runCheckup, theme loading)
├── thema_ads_optimized/
│   ├── themes.py                   # Theme management (load content, get labels, validate)
│   ├── models.py                   # Data models (AdGroupInput with theme_name)
│   ├── account ids                 # Whitelist of active customer IDs (28 accounts, excludes 16 CANCELED)
│   ├── main_optimized.py           # CLI entry point with multi-theme support
│   ├── operations/                 # Google Ads operations
│   │   ├── ads.py                  # Ad creation (campaign_theme=1 parameter)
│   │   ├── labels.py               # Label operations
│   │   ├── prefetch.py             # Bulk data fetching
│   │   └── rsa_management.py       # RSA slot management (3-ad limit, not yet integrated)
│   ├── processors/                 # Data processing
│   │   └── data_loader.py          # CSV/input handling
│   ├── templates/                  # Ad templates
│   │   └── generators.py           # Theme-based template generation
│   └── utils/                      # Utilities
│       ├── cache.py                # Caching logic
│       └── retry.py                # Retry logic with 503 ServiceUnavailable handling
├── cc1/                            # CC1 documentation
│   ├── TASKS.md
│   ├── LEARNINGS.md
│   ├── BACKLOG.md
│   └── PROJECT_INDEX.md
└── README.md
```

---
_Last updated: 2025-10-17_
