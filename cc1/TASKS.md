# TASKS
_Active task tracking. Update when: starting work, completing tasks, finding blockers._

## Current Sprint
_Active tasks for immediate work_

## In Progress
_Tasks currently being worked on_

- [ ] Complete duplicate removal labeling phase for customer 4056770576 (ran duplicate removal script for 77 minutes; scanning and duplicate removal phases completed successfully; found 3,357 ad groups with duplicates; removed all duplicate ads; labeling phase stopped at 26% completion with 884/3,357 ad groups labeled; remaining 2,473 ad groups need DUPLICATES_CHECKED label to prevent reprocessing; log file: /tmp/fully_optimized_duplicate_removal_v2.log) #claude-session:2025-10-24

## Completed
_Finished tasks (move here when done)_

- [x] Fix checkup function to verify theme ads exist (Rewrote checkup_ad_groups() to actually verify theme ads exist before marking for repair; added batch queries for theme-labeled ads using ad_group_ad_label; distinguishes between ad groups with valid theme ads that get THEMES_CHECK_DONE label vs missing theme ads that get DONE label removed and repair job created; eliminates false positives from blindly reprocessing all ad groups with DONE labels; backend/thema_ads_service.py:946-1011) #claude-session:2025-10-31
- [x] Implement 3-5x performance optimizations for theme ad creation (Priority 1: Batch database updates using executemany() pattern reducing 100,000+ individual UPDATEs to ~50-100 batches with 1000-item buffers achieving 10-20x faster DB operations backend/thema_ads_service.py:271-324,447-483; Priority 2: BALANCED rate limiting reducing initial_delay 2.0s→1.0s and min_delay 1.0s→0.5s achieving 2-3x faster API calls thema_ads_optimized/operations/ads.py:15-21; Priority 3: Increased customer concurrency from 5→10 workers and reduced customer_delay 15.0s→5.0s achieving 2x throughput thema_ads_optimized/config.py:23,29; combined result: 5 hours→1-1.5 hours for 50K ad groups; Job 413 Sinterklaas completed in 3.5 hours for 50K without optimizations; applies to Auto-Discover and Run All Themes) #claude-session:2025-10-31
- [x] Implement V2 ad-first activation achieving 10-100x performance improvement (built activate_ads_per_plan_v2() using revolutionary ad-first query approach querying FROM ad_group_ad_label instead of ad groups; two-step label resolution gets label.id by name then queries ads directly by label.id; only 4 queries per customer vs thousands in ad-group-first approach; added /api/thema-ads/activate-v2 endpoint; integrated into frontend; discovered correct GAQL syntax through Google Ads API documentation research) #claude-session:2025-10-31
- [x] Create optimized activation function with parallel processing (built activate_ads_per_plan_optimized() achieving 5-10x performance improvement through parallel customer processing with 5 workers, bulk queries for theme-labeled ads and THEMA_ORIGINAL ads in single queries per customer, batch status updates for enabling theme ads and pausing original ads; eliminates per-ad-group operations by querying all ads by label; added /api/thema-ads/activate-optimized endpoint with parallel_workers parameter; implementation uses asyncio.gather for concurrent execution and async locks for thread-safe stats) #claude-session:2025-10-31
- [x] Execute DONE label removals across 28 customers (ran audit_theme_done_labels_optimized.py with --execute flag processing all 28 customers × 4 themes = 112 audit operations; script completed successfully with exit code 0; removed thousands of invalid DONE labels from ad groups that had THEME_*_DONE labels but no corresponding themed ads; unblocked ad groups for future auto-discovery; used customer pre-filtering, bulk theme processing, HS/ campaign filtering, chunking, and parallel execution with 5 workers; handled CONCURRENT_MODIFICATION errors with retry logic) #claude-session:2025-10-31
- [x] Optimize Check-up function with 12-24x performance improvement (replaced database-driven check-up with optimized Google Ads direct audit achieving 12-24x speedup through customer pre-filtering, bulk theme processing 4x faster, HS/ campaign filtering 2-3x faster, better chunking 1.5x faster, and THEMES_CHECK_DONE tracking label to skip already-audited ad groups; added remove_checkup_labels() function for clean runs; updated frontend UI with skip_audited checkbox and reset labels button; created audit script to remove invalid DONE labels across 28 customers) #claude-session:2025-10-30
- [x] Create duplicate ad removal system with batch optimization (built remove_duplicate_ads.py and remove_all_duplicates.py achieving 50x performance improvement: 90s vs 60+ min scanning for 67,719 ads; content-based duplicate detection using sorted headlines+descriptions signature; priority scoring keeps ads with theme labels over unlabeled duplicates; DUPLICATES_CHECKED label prevents reprocessing; batch queries reduce 86,205 individual label fetches to ~18 queries using two-step GAQL approach; fixed resource name format using ~ separator and INNER JOIN syntax limitation) #claude-session:2025-10-24
- [x] Fix singles_day jobs created despite theme not selected (two-part bug: FastAPI Query parameter parsing issue where repeated params not parsed into list causing themes=None→all themes; missing theme_name field in chunk_data causing create_job() fallback to 'singles_day' default; fixed with Query(None) import, theme_name assignment in job creation loop, database DEFAULT removal; all three fixes required to fully resolve issue) #claude-session:2025-10-24
- [x] Optimize discovery with batch queries achieving 99.9% query reduction (fixed N+1 query anti-pattern in discover_all_missing_themes() causing 8+ hour discovery times; reduced 50,000+ queries to ~30 queries using batch IN clauses with 5000 items per batch and dictionary lookups for O(1) mapping; performance improved from 3s per ad group to 5-10min total for 10,000 ad groups, 99x faster; eliminated per-item queries by fetching all ad group labels, ads, and ad labels upfront then processing in-memory) #claude-session:2025-10-23
- [x] Fix Job 338 CONCURRENT_MODIFICATION failures with 40/97 ad groups failing (41% failure rate; created repair job 340 achieving 100% success; improved retry logic in thema_ads_optimized/utils/retry.py with CONCURRENT_MODIFICATION detection, exponential backoff with jitter 5s→10s→20s→40s→80s, and ±20% random variance to prevent thundering herd problem; race condition caused by uniform short delays sending simultaneous retries to Google's backend) #claude-session:2025-10-23
- [x] Fix Cyber Monday and Singles Day headline length validation failures (discovered Google Ads API validates rendered output not literal syntax; modified themes/cyber_monday/headlines.txt line 9 from "Cyber Monday – Eindigt Over {COUNTDOWN(...)}" to "Eindigt Over {COUNTDOWN(...)}" reducing rendered length from 36→21 chars; updated thema_ads_optimized/themes.py lines 76-101 Singles Day theme to remove double quotes and shorten problematic headlines; all themes now validated ≤30 chars when rendered) #claude-session:2025-10-23
- [x] Investigate Kerstmis success vs Cyber Monday/Singles Day failure mystery (root cause: theme name length affects rendered headline length; Kerstmis uses "Kerst" (5 chars) staying under 30-char limit, Cyber Monday (12 chars) exceeds limit with identical COUNTDOWN syntax; Google Ads API validates rendered output not literal syntax shown in error messages) #claude-session:2025-10-23
- [x] Fix auto-queue async context issue (changed _start_next_job_if_queue_enabled to use asyncio.create_task instead of await, prevents delayed database updates for auto-queued jobs) #claude-session:2025-10-22
- [x] Update Check-up function for multi-theme support (rewrote checkup_ad_groups() to query database for theme_name per ad group, check theme-specific labels instead of text search, create repair jobs with correct theme; supports all themes: black_friday, cyber_monday, sinterklaas, kerstmis, singles_day) #claude-session:2025-10-09
- [x] Implement multi-theme system (Black Friday, Cyber Monday, Sinterklaas, Kerstmis; Excel upload with theme column; theme selection in Auto-Discover; theme-specific content loading; dynamic label management; per-ad-group theming; processing by customer with theme applied per ad group) #claude-session:2025-10-09
- [x] Remove SINGLES_DAY ads with batch deletion (batch removed 1,046 SINGLES_DAY ads and 20,312 SD_DONE label associations; created remove_singles_day_ads_batch.py script with proper error handling) #claude-session:2025-10-09
- [x] Add campaign_theme=1 query parameter to ad URLs (modified build_ad_data() to append tracking parameter to all created ad URLs) #claude-session:2025-10-09
- [x] Fix repair job SD_DONE skip logic (added is_repair_job flag to jobs table, updated get_job_status to return flag, modified processor to skip SD_DONE check for repair jobs) #claude-session:2025-10-08
- [x] Re-implement Check-up function with proper testing (audits ad groups with SD_DONE label, verifies SINGLES_DAY ads exist, creates repair jobs for missing ads; includes backend endpoint, frontend UI tab, tested with limit=10) #claude-session:2025-10-07
- [x] Reduce ad creation batch size to prevent Google crawler rate limiting (100 ads per batch, 5s delays, prevents DESTINATION_NOT_WORKING errors from CloudFront blocking) #claude-session:2025-10-05
- [x] Reduce API operations by disabling non-essential labels (removed THEMA_AD and BF_2025 labels, reduced from 6 to 4 operations per ad group, 33% savings) #claude-session:2025-10-05
- [x] Add automatic job chunking for large discoveries (splits into optimal-sized jobs, default 50k items per job, user-configurable 10k-100k) #claude-session:2025-10-04
- [x] Implement 503 error handling with extended retry logic (60s, 180s, 540s, 1620s exponential backoff for Service Unavailable errors) #claude-session:2025-10-04
- [x] Reduce default batch_size from 7500 to 5000 to avoid rate limits and 503 errors #claude-session:2025-10-04
- [x] Add customer processing delays (30s between customers) to prevent API rate limiting #claude-session:2025-10-04
- [x] Update frontend batch_size default from 7500 to 5000 in both CSV and Auto-Discover tabs #claude-session:2025-10-04
- [x] Fix REQUEST_TOO_LARGE error with automatic chunk size reduction (recursively halves chunk size from 10K down to 100 until success) #claude-session:2025-10-04
- [x] Fix error handling for complete chunk failures (track failures per ad group with specific error messages instead of generic "no resource returned") #claude-session:2025-10-04
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
