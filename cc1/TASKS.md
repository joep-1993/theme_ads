# TASKS
_Active task tracking. Update when: starting work, completing tasks, finding blockers._

## Current Sprint
_Active tasks for immediate work_

## In Progress
_Tasks currently being worked on_

## Completed
_Finished tasks (move here when done)_

- [x] Filter discovery to use only valid customer accounts from whitelist file (eliminates 16 CANCELED accounts causing PERMISSION_DENIED errors) #claude-session:2025-10-04
- [x] Separate theme_ads from content_top into independent repository #claude-session:2025-10-03
- [x] Create dedicated backend/main.py with Google Ads API endpoints only #claude-session:2025-10-03
- [x] Optimize auto-discovery with direct ad query (74% fewer API queries: 271→71 for 146k ad groups) #claude-session:2025-10-03
- [x] Merge thema_ads_project and thema_ads_optimized directories into single thema_ads_optimized/ structure #claude-session:2025-10-03
- [x] Increase discovery timeout from 2 to 10 minutes for large account discovery (100K+ ad groups) #claude-session:2025-10-03
- [x] Use configurable batch_size in discovery SD_DONE label checks instead of hardcoded 5000 #claude-session:2025-10-03
- [x] Fix Google Ads API 10K operation limit by chunking batch operations (ads, ad labels, ad group labels) #claude-session:2025-10-03
- [x] Remove theme label filtering from ad prefetch logic (find existing ads even if they already have BF_2025 label) #claude-session:2025-10-03
- [x] Add ad_group_name column to CSV export of failed/skipped items for better debugging #claude-session:2025-10-03
- [x] Add configurable batch_size input field to frontend (CSV and Auto-Discover tabs, default 7500, range 1000-10000) #claude-session:2025-10-02
- [x] Increase BATCH_SIZE from 5,000 to 7,500 for additional 33% performance improvement (fewer API calls for large customers) #claude-session:2025-10-02
- [x] Fix Excel precision loss by using ad_group_name lookups (added ad_group_name column, resolve correct IDs from Google Ads API) #claude-session:2025-10-02
- [x] Optimize auto-discover with batched label checking (5000x faster: 30 API calls instead of 146k) #claude-session:2025-10-02
- [x] Add limit parameter to auto-discover (prevents overwhelming system, allows testing) #claude-session:2025-10-02
- [x] Add auto-discover mode to find ad groups from Google Ads (MCC account, Beslist.nl accounts, HS/ campaigns, no SD_DONE label) #claude-session:2025-10-02
- [x] Increase batch size from 1,000 to 5,000 for 5x performance improvement on large customers #claude-session:2025-10-02
- [x] Fix timezone display issue (UTC timestamps now properly converted to local timezone) #claude-session:2025-10-02
- [x] Update jobs list table to show Success/Failed/Skipped counts #claude-session:2025-10-02
- [x] Include skipped items in downloadable CSV with clear reasons (e.g., "Ad group has 0 ads" or "Ad group has 'SD_DONE' label") #claude-session:2025-10-02
- [x] Count ad groups without existing ads as skipped instead of failed (no SD_DONE label applied) #claude-session:2025-10-02
- [x] Add skipped items tracking to frontend (separate column for already-processed ad groups with SD_DONE label) #claude-session:2025-10-02
- [x] Fix results mapping bug causing false failures (properly track processed vs failed vs skipped ad groups) #claude-session:2025-10-02
- [x] Fix comma decimal separator in scientific notation (handle 1,76256E+11 from European Excel locales) #claude-session:2025-10-02
- [x] Add automatic job start after CSV upload (removed manual start button requirement) #claude-session:2025-10-02
- [x] Fix scientific notation in CSV uploads (Excel converts large IDs to 1.76256E+11 format) #claude-session:2025-10-02
- [x] Add CSV download for failed items (export customer_id, ad_group_id, error_message) #claude-session:2025-10-02
- [x] Fix FILTER_HAS_TOO_MANY_VALUES error by batching prefetch queries (1000 ad groups per query) #claude-session:2025-10-02
- [x] Add SD_DONE label to processed ad groups and skip already-processed ad groups #claude-session:2025-10-02
- [x] Optimize large CSV file upload performance (batch inserts, dynamic timeouts, progress feedback) #claude-session:2025-10-02
- [x] Fix CSV encoding issues - support multiple encodings (UTF-8, Windows-1252, ISO-8859-1) #claude-session:2025-10-02
- [x] Extend CSV upload file size limit from 10MB to 30MB (updated frontend validation) #claude-session:2025-10-02
- [x] Refactor legacy thema_ads script for security (removed hardcoded secrets, environment variables, .env setup) #claude-session:2025-10-02
- [x] Set up Git repository and GitHub integration (Git init, SSH authentication, GitHub push, secret protection) #claude-session:2025-10-02
- [x] Add delete job functionality to Thema Ads (UI button, backend endpoint, cascade deletion) #claude-session:2025-10-02
- [x] Fix CSV upload and validation issues (empty row handling, customer_id formatting, optional campaign columns) #claude-session:2025-10-02
- [x] Add comprehensive error handling to Thema Ads frontend (CSV validation, timeouts, retries, network errors) #claude-session:2025-10-02
- [x] Build Thema Ads web interface with CSV upload, real-time progress tracking, and resume capability #claude-session:2025-10-02
- [x] Integrate Thema Ads frontend into Docker with volume mounts and dependency fixes #claude-session:2025-10-02
- [x] Build high-performance Google Ads automation with Docker (thema_ads_optimized) #claude-session:2025-10-02
- [x] Set up Docker with multi-stage builds, docker-compose, helper scripts #claude-session:2025-10-02
- [x] Tested and deployed themed ads (Singles Day) - 5 ads successfully created #claude-session:2025-10-02

## Blocked
_Tasks waiting on dependencies_

---

## Task Tags Guide
- `#priority:` high | medium | low
- `#estimate:` estimated time (5m, 1h, 2d)
- `#blocked-by:` what's blocking this task
- `#claude-session:` date when Claude worked on this
