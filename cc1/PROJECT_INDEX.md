# PROJECT INDEX
_Technical reference for the project. Update when: architecture changes, new patterns emerge._

## Architecture

### System Overview
- **Backend**: FastAPI with async processing
- **Database**: PostgreSQL for job persistence
- **Google Ads**: API v28+ integration
- **Processing**: Batch operations with pause/resume

### Key Components
- `backend/main.py` - FastAPI API endpoints
- `backend/thema_ads_service.py` - Business logic and job processing
- `backend/database.py` - Database connection management
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
2. **Async Processing** - Parallel customer processing with semaphore control
3. **Prefetch Strategy** - Load all data upfront to eliminate redundant API calls
4. **Direct Ad Query** - 74% fewer queries using cross-resource filtering

### Reliability
1. **Idempotent Processing** - SD_DONE labels prevent duplicate processing
2. **State Persistence** - PostgreSQL tracks job and item status for resume capability
3. **Background Tasks** - FastAPI BackgroundTasks for long-running jobs
4. **Error Handling** - Distinguish between failed, skipped, and successful items

### API Integration
1. **Configurable Batch Size** - User-adjustable for rate limiting or performance
2. **CSV Flexibility** - Support minimal or full CSV formats
3. **Excel Compatibility** - Handle scientific notation and encoding issues
4. **Ad Group Name Lookups** - Resolve IDs from names to avoid Excel precision loss

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
│       └── retry.py                # Retry logic
├── cc1/                            # CC1 documentation
│   ├── TASKS.md
│   ├── LEARNINGS.md
│   ├── BACKLOG.md
│   └── PROJECT_INDEX.md
└── README.md
```

---
_Last updated: 2025-10-03_
