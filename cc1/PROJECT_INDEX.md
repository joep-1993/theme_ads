# PROJECT INDEX
_Technical reference for the project. Update when: architecture changes, new patterns emerge._

## Architecture

### System Overview
- **Backend**: FastAPI with async processing
- **Database**: PostgreSQL for job persistence
- **Google Ads**: API v28+ integration
- **Processing**: Batch operations with pause/resume
- **Quality Assurance**: Check-up function audits processed ad groups by querying Google Ads API to verify theme ads exist (not just labels), distinguishes valid vs invalid DONE labels, creates repair jobs only for genuinely missing ads

### Key Components
- `backend/main.py` - FastAPI API endpoints (CSV upload, Excel upload, auto-discovery, checkup, activation, queue management)
  - `/api/thema-ads/upload` - CSV upload and job creation (legacy, defaults to singles_day theme)
  - `/api/thema-ads/upload-excel` - Excel upload with theme column support
  - `/api/thema-ads/discover` - Auto-discover ad groups from MCC (with theme parameter)
  - `/api/thema-ads/themes` - Get list of supported themes
  - `/api/thema-ads/checkup` - OPTIMIZED audit of processed ad groups with skip_audited parameter (default: true)
  - `/api/thema-ads/remove-checkup-labels` - Remove THEMES_CHECK_DONE labels for clean audit runs
  - `/api/thema-ads/cleanup-thema-original` - Remove THEMA_ORIGINAL labels from ads with theme labels; processes all 28 customers and 4 active themes; supports dry_run parameter (default: false); returns statistics (total_checked, total_fixed, total_failed); frontend: "Label Cleanup" tab between Check-up and Run All Themes; backend/main.py:1645
  - `/api/thema-ads/run-all-themes` - Discovery with theme selection (uses Query(None) for proper repeated param parsing)
  - `/api/thema-ads/activate-v2` - V2 AD-FIRST ultra-fast activation (10-100x faster); queries FROM ad_group_ad_label to directly target ads by label; two-step label resolution (get label.id by name, query ads by label.id); only 4 queries per customer vs thousands; parameters: customer_ids (optional), parallel_workers (default: 5), reset_labels (default: false)
  - `/api/thema-ads/activate-optimized` - OPTIMIZED activation with parallel processing (lines 1768-1824); processes customers in parallel (default: 5 workers), bulk queries for theme and original ads, batch status updates; parameters: customer_ids (optional), parallel_workers (default: 5), reset_labels (default: false)
  - `/api/thema-ads/queue/status` - Get auto-queue enabled state
  - `/api/thema-ads/queue/enable` - Enable automatic job queue
  - `/api/thema-ads/queue/disable` - Disable automatic job queue
- `backend/thema_ads_service.py` - Business logic and job processing
  - `discover_all_missing_themes()` - Discovery with batch queries and theme filtering (lines 947-1340)
  - Discovery job creation (lines 1302-1318): Assigns theme_name to each ad group item before creating jobs to ensure proper theme tracking; without this field, create_job() falls back to 'singles_day' default
  - `checkup_ad_groups()` - OPTIMIZED: Direct Google Ads audit (12-24x faster) with TRUE VALIDATION; queries ad_group_ad_label to verify theme ads actually exist; only repairs ad groups genuinely missing theme ads; adds THEMES_CHECK_DONE to validated ad groups; removes DONE labels only from invalid cases; customer pre-filtering, bulk theme queries, HS/ campaign filter, chunking (500 AG/1000 ads) (lines 668-1040)
  - `remove_checkup_labels()` - Remove THEMES_CHECK_DONE labels from all ad groups (lines 512-599)
  - `activate_ads_per_plan()` - Original activation function with per-ad-group processing (lines 1525-1906)
  - `activate_ads_per_plan_optimized()` - OPTIMIZED: 5-10x faster activation (lines 1908-2180); parallel customer processing with configurable workers (default: 5), bulk queries for theme-labeled ads and THEMA_ORIGINAL ads, batch status updates (enable theme ads, pause original ads); eliminates per-ad-group operations; uses asyncio.gather for concurrent execution and async locks for thread-safe stats
  - `activate_ads_per_plan_v2()` - V2 AD-FIRST: 10-100x faster activation (lines 2266-2512); revolutionary approach queries FROM ad_group_ad_label instead of ad groups; two-step label resolution gets label.id by name then queries ads directly by label.id; only 4 queries per customer (get theme label ID, query theme ads, get original label ID, query original ads); THEMA_ORIGINAL ads query batched (1000 ad groups per batch) to avoid FILTER_HAS_TOO_MANY_VALUES on large customers; parallel customer processing with configurable workers; batch mutations (5000 ads per chunk)
  - `get_next_pending_job()` - Returns oldest pending job ID (FIFO)
  - `_start_next_job_if_queue_enabled()` - Auto-queue logic: waits 30s, checks queue state, starts next job
- `backend/database.py` - Database connection management, auto-queue state persistence
  - `get_auto_queue_enabled()` - Retrieve queue toggle state from database
  - `set_auto_queue_enabled(bool)` - Persist queue toggle state
- `frontend/thema-ads.html` - Web UI with 6 tabs (Excel Upload, CSV Upload, Auto-Discover, Check-up [OPTIMIZED], Run All Themes, Activate Ads) and auto-queue toggle
- `frontend/js/thema-ads.js` - Frontend logic including uploadExcel(), runCheckup() with skip_audited parameter, removeCheckupLabels() with confirmation dialog, theme loading, auto-queue toggle (loadQueueStatus(), toggleAutoQueue())
- `themes/` - Theme content directory (black_friday/, cyber_monday/, sinterklaas/, kerstmis/)
  - Each theme has headlines.txt and descriptions.txt files
- `thema_ads_optimized/themes.py` - Theme management module (load content, get labels, validate themes)
- `thema_ads_optimized/account ids` - Whitelist of 28 active customer IDs (discovery loads from this file)
- `thema_ads_optimized/` - CLI automation tools
- `thema_ads_optimized/cleanup_thema_original_labels.py` - Label conflict cleanup script; removes THEMA_ORIGINAL labels from ads that have theme labels; processes 4 active themes (BF, CM, SK, KM) excluding Singles Day; supports dry-run mode; batch operations (1000 labels per API call); found ~85,000 conflicting ads across 28 customers; integrated into frontend via /api/thema-ads/cleanup-thema-original endpoint
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
1. **Ad-First Query Pattern** - Query ads directly by label instead of querying all ad groups (10-100x faster)
   - Query FROM ad_group_ad_label WHERE label.id = X
   - Two-step label resolution: get label.id by name, then query ads by ID
   - Only 4 queries per customer vs thousands in ad-group-first approach
   - Eliminates need to query all ad groups and filter client-side
   - Implementation: activate_ads_per_plan_v2() in backend/thema_ads_service.py:2266-2512
2. **Batch API Operations** - Reduce API calls by batching (100 ads per creation batch to prevent crawler overload)
3. **Async Processing** - Parallel customer processing with semaphore control (5 concurrent customers)
4. **Prefetch Strategy** - Load all data upfront to eliminate redundant API calls
5. **Direct Ad Query** - 74% fewer queries using cross-resource filtering
6. **Customer Account Whitelisting** - Use static file-based customer ID list instead of dynamic MCC query to avoid CANCELED accounts (eliminates permission errors, faster discovery)
7. **Automatic Job Chunking** - Large discoveries split into optimal-sized jobs (default 50k items/job, configurable 10k-100k)
8. **API Quota Optimization** - Reduced from 6 to 4 operations per ad group (33% savings):
   - Disabled THEMA_AD label on new ads
   - Disabled BF_2025 label on ad groups
   - Kept essential labels: SINGLES_DAY (new ad), THEMA_ORIGINAL (old ad), SD_DONE (ad group)
9. **Google Crawler Rate Limiting Prevention** - Small batches to prevent DESTINATION_NOT_WORKING errors:
   - Ad creation batch size: 100 (down from 10,000)
   - Batch delays: 5s between ad creation batches
   - Prevents CloudFront from blocking Google's policy crawler
10. **Rate Limiting** - Multi-layer approach to prevent 503 errors:
   - Query batch size: 5000 (reduced from 7500)
   - Customer delays: 30s between customers
   - Batch delays: 2s between API queries
   - Concurrency: 5 max concurrent customers (reduced from 10)
   - Job chunking: 50k items per job max
   - Operation reduction: 4 ops/ad group (from 6)
11. **Extended 503 Retry Logic** - Exponential backoff with long waits (60s, 180s, 540s, 1620s) for Service Unavailable errors
12. **CONCURRENT_MODIFICATION Retry Handling** - Jittered exponential backoff (5s→80s with ±20% variance)
   - Detects database_error: CONCURRENT_MODIFICATION specifically
   - Prevents thundering herd with random delays to avoid simultaneous retries
   - Eliminated 40/97 failures in Job 338 (41% failure rate → 0%)
   - Longer base delays (5s, 10s, 20s, 40s, 80s) vs standard (2s, 4s, 8s)
13. **Batch Query Discovery Optimization** - Eliminate N+1 queries in discovery (99.9% reduction)
   - Batch fetch ad group labels, ads, and ad labels using IN clauses (5000 per batch)
   - Use dictionary lookups for O(1) resource→ID mapping instead of O(n) linear search
   - Reduced 50,000 queries to ~30 queries in all-themes discovery
   - Discovery time: 8+ hours → 5-10 minutes for 10,000 ad groups (99x faster)
14. **Duplicate Ad Removal with Batch Optimization** - 50x performance improvement for duplicate detection
   - Content-based duplicate detection: sorted(headlines+descriptions) signature for uniqueness
   - Priority scoring: theme_label_count * 100 + has_any_theme * 10 + is_enabled * 1
   - Python stable sort for deterministic tie-breaking (older ads preferred)
   - Batch label fetching: 86,205 queries → ~18 queries using two-step GAQL approach
   - DUPLICATES_CHECKED label prevents reprocessing
   - Performance: 90 seconds vs 60+ minutes for 67,719 ads
   - Two-step GAQL: Fetch label resources with IN clause → Fetch label names → Map in code
15. **Parallel Duplicate Removal** - 60% time reduction using multi-processing
   - ProcessPoolExecutor with 3 concurrent workers (one per customer)
   - Each customer has separate Google Ads API rate limits (safe to parallelize)
   - Automatic resume from DUPLICATES_CHECKED labels after interruptions
   - Results: 58,771 duplicate ads removed across 28 customer accounts
   - Processing time: ~12 hours parallel vs ~30+ hours sequential
   - Rate limit safety: Each customer account has independent API quota

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
5. **RSA path1 Field** - Distinction between ad properties and URL parameters:
   - `path1` is an RSA ad property, NOT a URL query parameter
   - Set via: `rsa.path1 = theme_name` (display URL path extension)
   - Separate from URL query params: `?campaign_theme=1`
   - Common confusion: path1 appears in display URL but is not part of final_url

## Database Schema

### Core Tables
- `thema_ads_jobs` - Job tracking table
  - `theme_name` VARCHAR(50) - Theme for job (NO DEFAULT VALUE - must be explicitly set during job creation; database default removed 2025-10-24 to prevent incorrect theme assignment via fallback; theme must be provided in create_job() call)
  - `batch_size` INTEGER DEFAULT 7500 - API query batch size for this job
  - `skipped_ad_groups` INTEGER DEFAULT 0 - Count of ad groups skipped (already processed with SD_DONE label)
  - `is_repair_job` BOOLEAN DEFAULT FALSE - If true, bypasses SD_DONE check to reprocess items
- `thema_ads_job_items` - Individual ad group processing status
  - `theme_name` VARCHAR(50) - Theme for specific ad group
- `thema_ads_input_data` - Original upload data
  - `theme_name` VARCHAR(50) - Theme from original upload
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
- `ALL_THEMES_DONE` - Meta-label applied when ad group has all 4 main theme completion labels (THEME_BF_DONE, THEME_CM_DONE, THEME_SK_DONE, THEME_KM_DONE)
- `THEMES_CHECK_DONE` - Audit tracking label applied to ad groups after validation; used by checkup function to skip already-audited ad groups for faster subsequent runs

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
- `DATABASE_URL` - PostgreSQL connection string (format: postgresql://user:password@host:port/database)
- `GOOGLE_DEVELOPER_TOKEN` - Google Ads API developer token
- `GOOGLE_REFRESH_TOKEN` - OAuth refresh token
- `GOOGLE_CLIENT_ID` - OAuth client ID
- `GOOGLE_CLIENT_SECRET` - OAuth client secret
- `GOOGLE_LOGIN_CUSTOMER_ID` - MCC account ID
- `MAX_CONCURRENT_CUSTOMERS` - Parallel customer processing limit (default: 5, optimized: 10)
- `MAX_CONCURRENT_OPERATIONS` - Maximum concurrent operations (default: 50)
- `BATCH_SIZE` - Items per API query (code default: 5000; database stores per-job value, typically 7500)
- `API_RETRY_ATTEMPTS` - Retry attempts for failed API calls (default: 5)
- `API_RETRY_DELAY` - Initial retry delay in seconds (default: 2.0)
- `API_BATCH_DELAY` - Delay between API batches in seconds (default: 2.0)
- `CUSTOMER_DELAY` - Delay between processing customers in seconds (default: 30.0)
- `LOG_LEVEL` - Logging verbosity (default: INFO, options: DEBUG, INFO, WARNING, ERROR)
- `DRY_RUN` - Test mode without making actual changes (default: false)
- `INPUT_FILE` - Default input file path (default: input_data.xlsx)
- `ENABLE_CACHING` - Enable response caching (default: true)

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
├── remove_duplicate_ads.py         # Utility: Remove duplicate RSAs keeping theme-labeled ads (batch optimized, 50x speedup)
├── remove_all_duplicates.py        # Utility: Wrapper to process all 28 customers sequentially for duplicate removal
├── remove_all_duplicates_parallel.py # Utility: Parallel version with 3 workers (60% faster, 58,771 ads removed)
├── check_ad_groups.py              # Utility: Check ad group labels and theme ads
├── audit_theme_done_labels_optimized.py # Utility: OPTIMIZED audit script verifies themed ads exist for DONE labels, removes invalid labels, creates repair jobs; features customer pre-filtering, bulk theme processing, HS/ campaign filter, chunking, THEMES_CHECK_DONE tracking, parallel execution (5 workers)
├── fill_missing_themed_ads_parallel_v3.py # Utility: Gap-filler for ad groups with THEME_*_DONE labels but missing theme ads
│                                    # Features: batch ad creation, themed content from /themes/ files, proper labeling,
│                                    # ALL_THEMES_DONE label when all 4 themes present, progress persistence (fill_missing_progress_v3.json),
│                                    # parallel processing (3 workers), campaign_theme=1 URL parameter, path1=theme_name ad field
├── activate_ads_v2.py               # Utility: Test V2 activation function independently
├── pause_enabled_themed_ads_parallel.py # Utility: Pause all enabled theme ads across customers
├── fix_theme_labels_parallel.py     # Utility: Fix incorrect theme label assignments
├── create_black_friday_ads.py       # Utility: One-off script for Black Friday ad creation
├── investigate_ad_group.py          # Utility: Debug tool for inspecting specific ad groups
├── [20+ additional utility scripts] # Note: Root directory contains ad-hoc testing/maintenance scripts
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
_Last updated: 2025-11-01_
