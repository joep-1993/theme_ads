# PROJECT INDEX
_Technical reference for the project. Update when: architecture changes, new patterns emerge._

## Architecture

### System Overview
- **Backend**: FastAPI with async processing
- **Database**: PostgreSQL for job persistence
- **Google Ads**: API v28+ integration
- **Processing**: Batch operations with pause/resume

### Key Components
- `backend/main.py` - FastAPI API endpoints (includes auto-discovery from customer whitelist)
- `backend/thema_ads_service.py` - Business logic and job processing
- `backend/database.py` - Database connection management
- `thema_ads_optimized/account ids` - Whitelist of 28 active customer IDs (discovery loads from this file)
- `thema_ads_optimized/` - CLI automation tools
- `thema_ads_optimized/operations/` - Google Ads API operations
- `thema_ads_optimized/processors/` - Data processing logic

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
1. **Batch API Operations** - Reduce API calls by batching (up to 10K operations per request)
2. **Async Processing** - Parallel customer processing with semaphore control (5 concurrent customers)
3. **Prefetch Strategy** - Load all data upfront to eliminate redundant API calls
4. **Direct Ad Query** - 74% fewer queries using cross-resource filtering
5. **Customer Account Whitelisting** - Use static file-based customer ID list instead of dynamic MCC query to avoid CANCELED accounts (eliminates permission errors, faster discovery)
6. **Automatic Job Chunking** - Large discoveries split into optimal-sized jobs (default 50k items/job, configurable 10k-100k)
7. **API Quota Optimization** - Reduced from 6 to 4 operations per ad group (33% savings):
   - Disabled THEMA_AD label on new ads
   - Disabled BF_2025 label on ad groups
   - Kept essential labels: SINGLES_DAY (new ad), THEMA_ORIGINAL (old ad), SD_DONE (ad group)
8. **Rate Limiting** - Multi-layer approach to prevent 503 errors:
   - Batch size: 5000 (reduced from 7500)
   - Customer delays: 30s between customers
   - Batch delays: 2s between API calls
   - Concurrency: 5 max concurrent customers (reduced from 10)
   - Job chunking: 50k items per job max
   - Operation reduction: 4 ops/ad group (from 6)
9. **Extended 503 Retry Logic** - Exponential backoff with long waits (60s, 180s, 540s, 1620s) for Service Unavailable errors

### Reliability
1. **Idempotent Processing** - SD_DONE labels prevent duplicate processing
2. **State Persistence** - PostgreSQL tracks job and item status for resume capability
3. **Background Tasks** - FastAPI BackgroundTasks for long-running jobs
4. **Error Handling** - Distinguish between failed, skipped, and successful items

### API Integration
1. **Configurable Batch Size** - User-adjustable (1000-10000, default: 5000) for rate limiting or performance
2. **CSV Flexibility** - Support minimal or full CSV formats
3. **Excel Compatibility** - Handle scientific notation and encoding issues
4. **Ad Group Name Lookups** - Resolve IDs from names to avoid Excel precision loss

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

## File Structure
```
theme_ads/
├── backend/
│   ├── main.py                     # API endpoints
│   ├── thema_ads_service.py        # Business logic
│   ├── database.py                 # DB connection
│   └── thema_ads_schema.sql        # DB schema
├── thema_ads_optimized/
│   ├── account ids                 # Whitelist of active customer IDs (28 accounts, excludes 16 CANCELED)
│   ├── main_optimized.py           # CLI entry point
│   ├── operations/                 # Google Ads operations
│   │   ├── ads.py                  # Ad creation
│   │   ├── labels.py               # Label operations
│   │   └── prefetch.py             # Bulk data fetching
│   ├── processors/                 # Data processing
│   │   └── data_loader.py          # CSV/input handling
│   ├── templates/                  # Ad templates
│   │   └── generators.py           # Template generation
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
_Last updated: 2025-10-03_
