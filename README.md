# Theme Ads - Google Ads Automation

Automated Google Ads themed campaign management system with web interface and CLI tools.

## Features

- **CSV Upload**: Bulk process ad groups from CSV files
- **Auto-Discovery**: Automatically find ad groups from MCC account
- **Pause/Resume**: Jobs can be paused and resumed without data loss
- **Label Tracking**: Uses SD_DONE labels to prevent duplicate processing
- **Batch Processing**: Configurable batch sizes for optimal API performance
- **Web Interface**: Real-time job monitoring and management

## Quick Start

### Web Interface
```bash
./start-thema-ads.sh
# Access at http://localhost:8001
```

### CLI Automation
```bash
cd thema_ads_optimized
./docker-run.sh setup
./docker-run.sh build
./docker-run.sh run
```

## Project Structure

```
theme_ads/
├── backend/                    # FastAPI web service
│   ├── main.py                # API endpoints
│   ├── thema_ads_service.py   # Business logic
│   └── thema_ads_schema.sql   # Database schema
├── thema_ads_optimized/       # CLI automation scripts
│   ├── main_optimized.py      # Main processing script
│   ├── operations/            # Google Ads operations
│   ├── processors/            # Data processing
│   └── templates/             # Ad templates
└── start-thema-ads.sh         # Quick start script
```

## Configuration

1. Copy `.env.example` to `.env` in `thema_ads_optimized/`
2. Add your Google Ads credentials
3. Configure MCC account and other settings

## Documentation

- [Thema Ads Guide](THEMA_ADS_GUIDE.md) - Complete usage guide
- [CLAUDE.md](CLAUDE.md) - Project architecture and conventions

## Tech Stack

- FastAPI for web API
- PostgreSQL for job persistence
- Google Ads API v28+
- Docker for containerization
- Async processing for performance

---
_Automated Google Ads themed campaigns at scale_
