# CLAUDE.md

This project is **Theme Ads** - a Google Ads automation system for themed ad campaigns.

## Tech Stack
- **Backend**: FastAPI with async processing
- **Database**: PostgreSQL in Docker
- **Google Ads**: API v28+ integration
- **Processing**: Batch operations with pause/resume capability

## Project Structure
- `thema_ads_optimized/` - Main automation scripts
- `backend/` - FastAPI service for web interface
- `start-thema-ads.sh` - Quick start script

## Development Workflow
1. Run `./start-thema-ads.sh` to start web interface
2. Or use `thema_ads_optimized/` for CLI automation
3. Access web interface at http://localhost:8001

## Key Features
- **CSV Upload**: Bulk ad group processing from CSV
- **Auto-Discovery**: Find ad groups from MCC account automatically
- **Pause/Resume**: Jobs can be paused and resumed
- **Label Tracking**: Uses SD_DONE labels to prevent duplicates
- **Batch Processing**: Configurable batch sizes for API efficiency

## File Locations
- Web API: `backend/thema_ads_service.py`
- Database Schema: `backend/thema_ads_schema.sql`
- CLI Scripts: `thema_ads_optimized/`
- Configuration: `thema_ads_optimized/.env`

---
_Project: Theme Ads | Google Ads Automation_
