# LEARNINGS
_Capture mistakes, solutions, and patterns. Update when: errors occur, bugs are fixed, patterns emerge._

## Docker Commands
```bash
# Thema Ads Web Interface (Quick Start)
./start-thema-ads.sh           # Build, start, and initialize everything

# Thema Ads Optimized CLI
cd thema_ads_optimized/
./docker-run.sh setup          # Setup environment and directories
./docker-run.sh build          # Build Docker image
./docker-run.sh dry-run        # Test run (no changes)
./docker-run.sh run            # Production run
./docker-run.sh logs           # View logs
./docker-run.sh clean          # Cleanup Docker resources
```

## Common Issues & Solutions

### Google Ads API 503 Service Unavailable Errors
- **Error**: `503 The service is currently unavailable`
- **Cause**: Google Ads API rate limiting or temporary service unavailability when processing large volumes
- **Impact**: Jobs with 100k+ ad groups hitting multiple 503 errors over hours, causing customer processing failures
- **Symptoms**: Repeated failures across multiple customers (e.g., 8696777335, 5930401821, 3114657125, 5807833423, etc.)
- **Solution**: Multi-layered rate limiting and extended retry strategy
```python
# 1. Special handling for 503 errors in retry decorator
from google.api_core.exceptions import ServiceUnavailable

def async_retry(max_attempts: int = 5, delay: float = 2.0, backoff: float = 2.0):
    async def wrapper(*args, **kwargs):
        for attempt in range(1, max_attempts + 1):
            try:
                return await func(*args, **kwargs)
            except ServiceUnavailable as e:
                if attempt < max_attempts:
                    # Much longer delays for 503: 60s, 180s, 540s, 1620s
                    retry_delay = 60 * (3 ** (attempt - 1))
                    await asyncio.sleep(retry_delay)
                else:
                    raise

# 2. Reduce batch size to lower API load
batch_size = 5000  # Reduced from 7500

# 3. Add delays between customers
async def process_with_limit(customer_id, customer_inputs):
    result = await process_customer(customer_id, customer_inputs)
    await asyncio.sleep(30.0)  # 30s delay between customers
    return result

# 4. Reduce concurrent customer processing
max_concurrent_customers = 5  # Reduced from 10

# 5. Increase delays between batch operations
time.sleep(2.0)  # Increased from 0.5s between API batches
```
- **Configuration Changes**:
  - `BATCH_SIZE`: 7500 → 5000
  - `MAX_CONCURRENT_CUSTOMERS`: 10 → 5
  - `API_RETRY_ATTEMPTS`: 3 → 5
  - `API_RETRY_DELAY`: 1.0s → 2.0s
  - `API_BATCH_DELAY`: 0.5s → 2.0s
  - `CUSTOMER_DELAY`: New, 30.0s
- **Result**: Extended retry windows give Google's API time to recover, lower concurrency reduces load

### Google Ads CANCELED Accounts Cause PERMISSION_DENIED Errors
- **Error**: `StatusCode.PERMISSION_DENIED: The caller does not have permission` with `CUSTOMER_NOT_ENABLED: The customer account can't be accessed because it is not yet enabled or has been deactivated`
- **Cause**: MCC queries return all customer accounts including CANCELED/deactivated accounts
- **Impact**: Discovery fails for 16 CANCELED accounts (e.g., "Beslist.nl - Boeken", "Beslist.nl - CD's", "Beslist.nl - DVD's", etc.)
- **Solution**: Maintain whitelist of active customer IDs in separate file, load from file instead of querying MCC
```python
# Load customer IDs from file
account_ids_file = Path(__file__).parent.parent / "thema_ads_optimized" / "account ids"
with open(account_ids_file, 'r') as f:
    customer_ids = [line.strip() for line in f if line.strip()]

# Use only whitelisted accounts
beslist_customers = [{'id': cid} for cid in customer_ids]
```
- **File Format**: One customer ID per line (e.g., `4056770576`)
- **Maintenance**: Update file when accounts are added/removed from MCC

### Google Ads API Version Compatibility
- **Error**: `501 GRPC target method can't be resolved`
- **Cause**: Using outdated Google Ads API version (v16)
- **Solution**: Upgrade to google-ads>=25.1.0 (currently v28.0.0)

### Google Ads OAuth Credentials Mismatch
- **Error**: `unauthorized_client: Unauthorized`
- **Cause**: Refresh token must match the exact client_id/client_secret used to generate it
- **Solution**: Ensure client_id and client_secret match the ones used to create the refresh_token

### Google Ads API Parameter Changes
- **Error**: `mutate_ad_group_ads() got an unexpected keyword argument 'partial_failure'`
- **Cause**: Google Ads API v28+ removed 'partial_failure' parameter
- **Solution**: Remove partial_failure parameter from all mutate operations

### Empty List Conditional Bug
- **Error**: Operations silently skipped even though data exists
- **Cause**: Empty lists evaluate to False in Python conditionals (e.g., `if new_ads and label_ops:`)
- **Solution**: Check only the required condition (e.g., `if new_ads:`), not empty supporting lists

### Results Mapping Bug in Batch Processing
- **Error**: All items marked as failed with no error message when ad operations list is empty
- **Cause**: Using index-based success check `success=i < len(new_ad_resources)` fails when no operations were built
- **Impact**: Ad groups with no existing ads or no final URLs incorrectly marked as failed without error messages
- **Solution**: Separately track which inputs had operations built vs which failed pre-checks
```python
# Track separately
processed_inputs = []  # Had operations built
failed_inputs = []     # Failed pre-checks (no existing ad, no final URL)
skipped_ags = []       # Already processed (has SD_DONE label)

# Build operations
for inp, ag_resource in zip(inputs, ad_group_resources):
    if already_has_label:
        skipped_ags.append(inp)
    elif result := build_operations(inp):
        processed_inputs.append(inp)
        ad_operations.append(result)
    else:
        failed_inputs.append(inp)

# Map results correctly
for i, inp in enumerate(processed_inputs):
    results.append(ProcessingResult(success=True, new_ad_resource=new_ad_resources[i]))

for inp in failed_inputs:
    results.append(ProcessingResult(success=False, error="No existing ad found or no final URL available"))
```

### Module Import Errors in Docker
- **Error**: `ModuleNotFoundError: No module named 'database'`
- **Cause**: Relative imports don't work when running as module in Docker container
- **Solution**: Use absolute imports (e.g., `from backend.database import get_db_connection`)

### Empty CSV Rows Causing Job Failures
- **Error**: `Error in query: unexpected input 1.` when starting job
- **Cause**: CSV contained empty rows with blank customer_id or ad_group_id fields
- **Solution**: Skip rows during CSV parsing where customer_id or ad_group_id is empty
```python
if not customer_id or not ad_group_id:
    continue  # Skip empty rows
```

### Customer IDs with Dashes Breaking Google Ads API
- **Error**: `Error in query: unexpected input 1.` when querying ad groups
- **Cause**: Customer IDs formatted as "123-456-7890" instead of "1234567890"
- **Solution**: Automatically strip dashes from customer_id during CSV parsing
```python
customer_id = row['customer_id'].strip().replace('-', '')
```

### Excel Scientific Notation - Precision Loss Problem
- **Error**: `BAD_RESOURCE_ID` or "No existing ad found" even though ads exist
- **Root Cause**: Excel scientific notation stores **only 5-6 significant digits**, losing precision
- **Example**:
  - Original ID: `168066123456` (12 digits)
  - Excel converts: `1.68066E+11` (only 6 significant digits!)
  - Converted back: `168066000000` (last 6 digits become zeros)
- **Impact**: Ad group IDs corrupted, lookups fail, all items marked as "no existing ad"
- **Initial Solution (INSUFFICIENT)**: Convert scientific notation back
```python
def convert_scientific_notation(value: str) -> str:
    if 'E' in value.upper():
        value_normalized = value.replace(',', '.')
        return str(int(float(value_normalized)))  # Still loses precision!
```
- **Problem**: Precision already lost in Excel file (168066123456 → 168066000000)
- **Real Solution**: Use `ad_group_name` instead of `ad_group_id` for lookups
  1. Include `ad_group_name` column in CSV exports
  2. Look up correct `ad_group_id` from Google Ads API using name
  3. Use correct ID for all operations
```python
# Backend: Store ad_group_name from CSV
item['ad_group_name'] = row.get('ad_group_name')

# Processing: Resolve correct IDs from names
async def _resolve_ad_group_ids(customer_id, inputs):
    inputs_needing_lookup = [inp for inp in inputs if inp.ad_group_name]
    query = f"SELECT ad_group.id, ad_group.name FROM ad_group WHERE ad_group.name IN ({names})"
    name_to_id = {row.ad_group.name: str(row.ad_group.id) for row in response}
    # Update inputs with correct IDs from Google Ads
```
- **Prevention**: Always export with `ad_group_name` column; CSV must include both ID and name

### CSV Encoding Issues
- **Error**: `'utf-8' codec can't decode byte 0xe8 in position X: invalid continuation byte`
- **Cause**: CSV file exported from Excel or other tools using non-UTF-8 encoding (Windows-1252, ISO-8859-1)
- **Solution**: Try multiple encodings in fallback order
```python
encodings = ['utf-8', 'utf-8-sig', 'windows-1252', 'iso-8859-1', 'latin1']
for encoding in encodings:
    try:
        decoded = contents.decode(encoding)
        break
    except UnicodeDecodeError:
        continue
```

### Google Ads API Query Filter Limits - Configurable Batch Size
- **Error**: `FILTER_HAS_TOO_MANY_VALUES` - "Request contains an invalid argument"
- **Cause**: WHERE IN clause with too many values (e.g., 50,000+ ad group resources)
- **Solution**: Batch queries with user-configurable batch size (default: 7500)
```python
# Frontend: User selects batch size (1000-10000, default 7500)
batch_size = batchSizeInput.value || 7500

# Backend: Store batch_size in job
job_id = create_job(input_data, batch_size=batch_size)

# Processing: Use dynamic batch_size instead of hardcoded constant
async def prefetch_existing_ads_bulk(client, customer_id, ad_group_resources, batch_size=7500):
    for i in range(0, len(resources), batch_size):
        batch = resources[i:i + batch_size]
        resources_str = ", ".join(f"'{r}'" for r in batch)
        query = f"SELECT ... WHERE resource IN ({resources_str})"
        response = service.search(customer_id, query)
```
- **Impact**: Customers with 10k+ ad groups were failing completely before this fix
- **Performance Optimization**:
  - 1,000 → 5,000: ~5x speedup
  - 5,000 → 7,500: Additional ~33% speedup (fewer API calls)
  - Example: 54,968 ad groups = 11 batches @ 5k vs 8 batches @ 7.5k (27% fewer calls)
- **User Control**:
  - Smaller batches (1000-3000) for rate-limited scenarios
  - Default 7500 for optimal performance
  - Larger batches (up to 10000) for maximum speed
  - Stored per-job for consistency across pauses/resumes

### Large CSV Upload Timeouts
- **Error**: Connection timeout during upload, "Failed to load jobs list (request timed out)"
- **Cause**: Individual row-by-row database inserts extremely slow for large files (100k+ rows)
- **Solution**: Use batch inserts with executemany() and dynamic timeouts
```python
# Batch insert instead of loop
input_values = [(job_id, item['customer_id'], ...) for item in input_data]
cur.executemany("INSERT INTO table VALUES (%s, %s, ...)", input_values)

# Dynamic timeout on frontend based on file size
baseTimeout = 120000  # 2 minutes
extraTimeout = Math.floor(fileSize / (5 * 1024 * 1024)) * 30000  # +30s per 5MB
uploadTimeout = Math.min(baseTimeout + extraTimeout, 600000)  # Max 10 min
```
- **Performance**: Batch inserts are 100-1000x faster than individual inserts for large datasets

### GitHub Push Protection Blocking Secrets
- **Error**: `Push cannot contain secrets` - Google OAuth tokens, Azure secrets detected
- **Cause**: Hardcoded credentials in thema_ads script were committed to git history
- **Solution**:
  1. Remove files with secrets: `git rm --cached thema_ads_project/thema_ads`
  2. Add to .gitignore: `thema_ads_project/thema_ads` and `*.xlsx`
  3. Refactor script to use environment variables from .env file
  4. Amend commit to exclude sensitive files
```bash
# Remove from git tracking
git rm --cached path/to/secret-file

# Add to .gitignore
echo "path/to/secret-file" >> .gitignore

# Amend commit
git add .gitignore
git commit --amend --no-edit
```

## Git Commands
```bash
# Repository Setup
git init                                    # Initialize repository
git remote add origin git@github.com:user/repo.git
git branch -M main                          # Rename branch to main
git push -u origin main                     # Push to GitHub

# Configuration
git config user.name "username"
git config user.email "email@example.com"
```

## Project Patterns

### Check-up Function for Audit and Repair
- **Pattern**: Audit processed items and repair missing operations
- **Use Case**: Find ad groups marked as processed (SD_DONE label) but missing expected artifacts (SINGLES_DAY ads)
- **Benefit**: Catch failures from previous runs, fix incomplete processing, maintain data consistency
- **Implementation**:
```python
# Check-up workflow
for customer in customers:
    # 1. Find ad groups with SD_DONE label (already processed)
    processed_ag = query("SELECT ad_group WHERE has_label('SD_DONE')")

    # 2. Check which have verification label (already checked)
    checked_ag = query("SELECT ad_group WHERE has_label('SD_CHECKED')")

    # 3. Process only unchecked ad groups
    to_check = [ag for ag in processed_ag if ag not in checked_ag]

    for ag in to_check:
        # 4. Verify expected artifact exists
        has_singles_day_ad = query(f"SELECT ad WHERE ad_group={ag} AND has_label('SINGLES_DAY')")

        if has_singles_day_ad:
            # 5. Mark as verified
            apply_label(ag, 'SD_CHECKED')
        else:
            # 6. Queue for repair
            needs_repair.append(ag)

    # Stop when limit reached
    if len(needs_repair) >= limit:
        break

# Create jobs to fix missing items
create_repair_jobs(needs_repair)
```
- **Key Features**:
  - **SD_CHECKED label**: Prevents re-checking same ad groups on subsequent runs
  - **Incremental**: Only checks ad groups not yet verified
  - **Repair jobs**: Creates standard processing jobs for items needing fixes
  - **Limit-aware**: Respects user limits for testing/performance

### SD_CHECKED Label System for Check-up Tracking
- **Pattern**: Use verification label to track audit completion status
- **Purpose**: Prevent redundant checking of ad groups already verified in Check-up runs
- **Workflow**:
  1. **Check-up discovers** ad group with SD_DONE label (processed)
  2. **Verify artifact** exists (e.g., ad with SINGLES_DAY label)
  3. **If verified**: Apply SD_CHECKED label → skip in future Check-up runs
  4. **If missing**: Add to repair job → will get SD_CHECKED when fixed
- **Benefits**:
  - **Performance**: Skip verified ad groups (can reduce 662k checks to just new items)
  - **Progress tracking**: Clear audit trail of what's been checked
  - **Idempotent**: Safe to run Check-up multiple times
- **Label hierarchy**:
  - **SD_DONE**: Processing completed (ad created, labels applied)
  - **SD_CHECKED**: Verification completed (confirmed SINGLES_DAY ad exists)
- **Example**: First run checks 100k ad groups → applies SD_CHECKED to 95k verified. Second run only checks remaining 5k + any new items.

### Async/Parallel Processing for Performance
- **Pattern**: Use asyncio with semaphore-controlled concurrency
- **Benefit**: 20-50x speedup vs sequential processing
- **Example**: Process 10 customers in parallel, each with batched operations
```python
semaphore = asyncio.Semaphore(10)
tasks = [process_customer(cid, data) for cid, data in grouped]
results = await asyncio.gather(*tasks)
```

### Batch API Operations
- **Pattern**: Collect operations in memory, execute in single API call
- **Benefit**: Reduce API calls from 6 per item to 1 per 1000 items
- **Example**: Create 1000 ads in one mutate_ad_group_ads() call
- **Limit**: Google Ads API supports up to 10,000 operations per request

### Idempotent Processing with Label-Based Tracking
- **Pattern**: Label processed items and skip them on subsequent runs
- **Benefit**: Prevent duplicate processing, enable safe re-runs, resume after failures
- **Example**: Label ad groups with "SD_DONE" after processing, skip any with this label
```python
# Prefetch ad group labels
ag_labels_map = await prefetch_ad_group_labels(client, customer_id, ad_groups, "SD_DONE")

# Skip already processed
for inp, ag_resource in zip(inputs, ad_group_resources):
    if ag_labels_map.get(ag_resource, False):
        logger.info(f"Skipping {inp.ad_group_id} - already has SD_DONE label")
        continue
    # ... process ad group ...
    # After processing, label it
    await label_ad_groups_batch(client, customer_id, [(ag_resource, sd_done_label)])
```

### Prefetch Strategy for Bulk Operations
- **Pattern**: Load all required data upfront in 2-3 queries instead of N queries
- **Benefit**: Eliminate redundant API calls, enable better caching
- **Example**: Fetch all labels, all existing ads for customer before processing

### State Persistence for Resumable Jobs
- **Pattern**: Store job state in PostgreSQL with granular item tracking
- **Benefit**: Resume from exact point after crash or pause, zero data loss
- **Example**: Track job status + individual item status (pending/processing/completed/failed)
```sql
-- Job tracks overall progress
thema_ads_jobs: id, status, total, processed, successful, failed

-- Items track individual ad groups
thema_ads_job_items: id, job_id, customer_id, ad_group_id, status, error_message
```

### Flexible CSV Column Handling
- **Pattern**: Parse CSV by column names (not positions); make columns optional
- **Benefit**: Users can provide minimal CSV (2 cols) or full CSV (4+ cols); extra columns ignored; column order doesn't matter
- **Example**: Accept both minimal and full formats
```csv
# Minimal (fetches campaign info at runtime)
customer_id,ad_group_id
1234567890,9876543210

# Full (faster, no API calls needed)
customer_id,campaign_id,campaign_name,ad_group_id
1234567890,5555,My Campaign,9876543210

# Extra columns ignored (status, budget, etc.)
customer_id,campaign_id,campaign_name,ad_group_id,status,budget
```

### Defer Expensive Operations from Upload to Execution
- **Pattern**: Don't fetch external data during file upload; defer to job execution
- **Benefit**: Fast uploads (no timeouts), better error handling, users can upload large files quickly
- **Example**: Campaign info can be provided in CSV or fetched when job starts, not during upload
```python
# During upload: just parse and store
item = {
    'customer_id': customer_id,
    'ad_group_id': ad_group_id,
    'campaign_id': row.get('campaign_id'),  # Optional
    'campaign_name': row.get('campaign_name')  # Optional
}

# During job execution: fetch missing data if needed
if not item['campaign_id']:
    campaign_info = fetch_from_google_ads_api(customer_id, ad_group_id)
```

### Automatic Background Task Execution
- **Pattern**: Use FastAPI BackgroundTasks to auto-start long-running jobs after upload
- **Benefit**: Better UX (no manual start button), faster workflow, cleaner API
- **Example**: Auto-start job processing after CSV upload completes
```python
from fastapi import BackgroundTasks

@app.post("/api/thema-ads/upload")
async def upload_csv(file: UploadFile = File(...), background_tasks: BackgroundTasks = None):
    # Parse CSV and create job
    job_id = thema_ads_service.create_job(input_data)

    # Automatically start processing in background
    if background_tasks:
        background_tasks.add_task(thema_ads_service.process_job, job_id)
        logger.info(f"Job {job_id} queued for automatic processing")

    return {"job_id": job_id, "total_items": len(input_data), "status": "processing"}
```

### Skipped vs Failed Status Differentiation
- **Pattern**: Distinguish between actual failures and items that can't be processed
- **Benefit**: Clearer reporting, better troubleshooting, prevents false alarms
- **Example**: Mark items without existing ads as "skipped" instead of "failed"
```python
# Backend status logic
if result.success and "Already processed" in result.error:
    status = 'skipped'  # Has SD_DONE label, already processed
elif not result.success and "No existing ad" in result.error:
    status = 'skipped'  # No existing ads to work with (not a failure)
elif result.success:
    status = 'completed'
else:
    status = 'failed'  # Actual error (API failure, etc.)

# Frontend displays three categories
# Success: New ads created
# Skipped: Already processed OR no existing ads
# Failed: Actual errors that need attention
```

### Batched Discovery vs Per-Item Queries
- **Problem**: Checking properties for thousands of items with individual queries
- **Anti-pattern**: 1 API call per item to check a property (e.g., checking if 146k ad groups have a label = 146k API calls)
- **Solution**: Batch fetch all items, batch fetch all properties, filter in memory
```python
# ❌ BAD: 1 API call per ad group (146k calls!)
for ad_group in ad_groups:
    query = f"SELECT ... WHERE ad_group = '{ad_group.resource}' AND label = 'SD_DONE'"
    has_label = len(list(service.search(query))) > 0

# ✅ GOOD: Batch queries (2-3 calls total)
# 1. Get all ad groups (1 call)
ad_groups = list(service.search("SELECT ad_group FROM ad_group WHERE ..."))

# 2. Get all ad groups WITH the label (1-2 batched calls for 146k items)
BATCH_SIZE = 5000
for i in range(0, len(ad_groups), BATCH_SIZE):
    batch = ad_groups[i:i + BATCH_SIZE]
    resources_str = ", ".join(f"'{ag}'" for ag in batch)
    query = f"SELECT ad_group_label.ad_group WHERE ad_group IN ({resources_str}) AND label = 'SD_DONE'"
    # Process batch...

# 3. Filter in memory
processable_ad_groups = [ag for ag in ad_groups if ag not in labeled_set]
```
- **Performance**: Reduces 146k API calls to ~30 calls (5000x improvement)

### User-Configurable Performance Parameters
- **Pattern**: Allow users to adjust performance-critical parameters via UI
- **Benefit**: Optimize for different scenarios (rate limits, speed, API quotas) without code changes
- **Example**: Configurable batch size for API queries
```python
# Frontend: Input field with validation
<input type="number" id="batchSize" value="7500" min="1000" max="10000">

# JavaScript: Send to backend
const batchSize = parseInt(document.getElementById('batchSize').value) || 7500;
formData.append('batch_size', batchSize);

# Backend: Store with job
job_id = create_job(input_data, batch_size=batch_size)
cur.execute("INSERT INTO jobs (batch_size) VALUES (%s)", (batch_size,))

# Processing: Retrieve and use
job_details = get_job_status(job_id)
batch_size = job_details.get('batch_size', 7500)
processor = ThemaAdsProcessor(config, batch_size=batch_size)
```
- **Use Cases**:
  - API rate limiting: Lower batch size (1000-3000) to stay under quota
  - Maximum speed: Higher batch size (7500-10000) for fast processing
  - Testing: Small batch size (100) for quick validation

### Theme Label Filtering Issue - False Negatives
- **Problem**: Ad groups with RSAs incorrectly marked as "skipped - no ads" even when ads exist
- **Root Cause**: Prefetch query filtered out ads that already had theme labels (BF_2025, SINGLES_DAY)
- **Impact**: If ALL RSAs in an ad group had theme labels from previous runs, query returns 0 results
- **Solution**: Remove label filtering from ad prefetch, rely solely on SD_DONE ad group label
```python
# ❌ BAD: Filters out ads with theme labels (causes false negatives)
query = f"""
    SELECT ad_group_ad.* FROM ad_group_ad
    WHERE ad_group IN ({resources})
    AND ad.type = RESPONSIVE_SEARCH_AD
    AND status != REMOVED
    AND labels CONTAINS NONE ('{theme_label}')  # ← Causes problem
"""

# ✅ GOOD: No label filtering on ads (SD_DONE on ad group prevents reprocessing)
query = f"""
    SELECT ad_group_ad.* FROM ad_group_ad
    WHERE ad_group IN ({resources})
    AND ad.type = RESPONSIVE_SEARCH_AD
    AND status != REMOVED
"""
# Ad group-level SD_DONE label check prevents duplicate processing
```

### Google Ads API 10,000 Operations Per Request Limit
- **Problem**: Jobs with 20,000+ ad groups fail with "TOO_MANY_MUTATE_OPERATIONS" or "REQUEST_TOO_LARGE"
- **Root Cause**: Batch operations sent all items in single API request, exceeding 10K operation limit
- **Solution**: Chunk all batch operations into groups of 10,000 or less
```python
# ❌ BAD: Send all operations in one request
operations = [build_operation(item) for item in items]  # 20,000 operations
response = service.mutate(customer_id, operations)  # FAILS

# ✅ GOOD: Chunk operations into batches of 10K
BATCH_LIMIT = 10000
all_results = []

for chunk_start in range(0, len(items), BATCH_LIMIT):
    chunk = items[chunk_start:chunk_start + BATCH_LIMIT]
    operations = [build_operation(item) for item in chunk]
    response = service.mutate(customer_id, operations)
    all_results.extend(response.results)
```

### Google Ads API REQUEST_TOO_LARGE Error - Automatic Chunk Size Reduction
- **Problem**: Even with 10K limit enforced, some batches fail with REQUEST_TOO_LARGE (batch size in bytes, not operation count)
- **Root Cause**: Operations with large RSAs (15 headlines, 4 descriptions) can exceed Google's size limit before hitting 10K count
- **Impact**: Entire customer batches fail with "no resource returned", marking thousands of ad groups as failed
- **Solution**: Implement recursive chunk size reduction with automatic retry
```python
def _create_chunk_with_retry(service, chunk, chunk_size):
    """Create a chunk with automatic size reduction on REQUEST_TOO_LARGE."""
    operations = [build_operation(ad) for ad in chunk]

    try:
        response = service.mutate_ad_group_ads(customer_id=customer_id, operations=operations)
        return {"resources": [res.resource_name for res in response.results], "failures": []}

    except GoogleAdsException as e:
        error_msg = str(e)
        is_too_large = "REQUEST_TOO_LARGE" in error_msg or "too large" in error_msg.lower()

        if is_too_large and chunk_size > 100:
            # Retry with half the size
            new_chunk_size = chunk_size // 2
            logger.warning(f"REQUEST_TOO_LARGE, retrying with chunk size {new_chunk_size} (was {chunk_size})")

            # Split and retry recursively
            all_resources = []
            all_failures = []
            for sub_start in range(0, len(chunk), new_chunk_size):
                sub_chunk = chunk[sub_start:sub_start + new_chunk_size]
                result = _create_chunk_with_retry(service, sub_chunk, new_chunk_size)
                all_resources.extend(result["resources"])
                all_failures.extend(result["failures"])

            return {"resources": all_resources, "failures": all_failures}
        else:
            # Non-recoverable or chunk too small - mark all as failed with specific error
            failures = [{"ad_group_resource": ad["ad_group_resource"], "error": str(e)} for ad in chunk]
            return {"resources": [], "failures": failures}
```
- **Behavior**: Automatically halves chunk size (10000 → 5000 → 2500 → 1250 → ...) until success or minimum (100)
- **Result**: Recovers from size errors automatically, continues processing successful items

### Chunk Failure Error Handling - Per-Item Error Tracking
- **Problem**: When entire chunk fails, all items marked as "Ad creation failed (no resource returned)" with no details
- **Root Cause**: create_rsa_batch returned List[str] with only successful resources, no failure info
- **Impact**: Users couldn't diagnose why specific ad groups failed (policy violations, invalid data, etc.)
- **Solution**: Return dict with both successes and failures, track errors per ad group
```python
# ❌ OLD: Only return successful resources
async def create_rsa_batch(...) -> List[str]:
    # ... create ads ...
    return [res.resource_name for res in response.results]  # No failure info!

# ✅ NEW: Return both successes and failures
async def create_rsa_batch(...) -> dict:
    # ... create ads with retry logic ...
    return {
        "resources": [res.resource_name for res in response.results],
        "failures": [{"ad_group_resource": ag_res, "error": error_msg}, ...]
    }

# Caller: Map failures to specific ad groups
creation_result = await create_rsa_batch(client, customer_id, ad_operations)
new_ad_resources = creation_result["resources"]
creation_failures = creation_result["failures"]

# Build failure map
failure_map = {f["ad_group_resource"]: f["error"] for f in creation_failures}

# Build results with specific errors
for i, inp in enumerate(processed_inputs):
    ad_group_res = ad_operations[i]["ad_group_resource"]
    if ad_group_res in failure_map:
        results.append(ProcessingResult(
            success=False,
            error=f"Ad creation failed: {failure_map[ad_group_res]}"  # Specific error!
        ))
```
- **Benefit**: Users see exact Google Ads API error (policy violation, invalid URL, etc.) instead of generic message

### Customer Account Whitelisting for MCC Discovery
- **Pattern**: Store active customer IDs in external file instead of querying MCC for all accounts
- **Benefit**: Avoid CANCELED/disabled accounts, faster discovery, explicit control over which accounts to process
- **Use Case**: MCC accounts with mixed active/inactive customers (e.g., 28 active + 16 CANCELED)
- **Implementation**:
```python
# File: thema_ads_optimized/account ids (one ID per line)
4056770576
1496704472
4964513580
...

# Backend: Load from file
account_ids_file = Path(__file__).parent.parent / "thema_ads_optimized" / "account ids"
with open(account_ids_file, 'r') as f:
    customer_ids = [line.strip() for line in f if line.strip()]

# Process only whitelisted customers
for customer_id in customer_ids:
    process_customer(customer_id)
```
- **Alternative Approach**: Query MCC with status filter (requires additional API call and filtering logic)
- **Maintenance**: Update file when account status changes (new accounts added, old accounts deactivated)

### 503 Error Handling with Exponential Backoff Pattern
- **Pattern**: Separate retry logic for transient vs permanent errors
- **Benefit**: Recovers from temporary API unavailability without giving up too early
- **Implementation**:
```python
# Special case for 503 errors - much longer waits
except ServiceUnavailable as e:
    if attempt < max_attempts:
        # Exponential with base 3: 60s → 180s → 540s → 1620s (27min)
        retry_delay = 60 * (3 ** (attempt - 1))
        logger.warning(f"503 error, waiting {retry_delay}s before retry {attempt}/{max_attempts}")
        await asyncio.sleep(retry_delay)
    else:
        raise

# Regular errors - standard exponential backoff
except GoogleAdsException as e:
    if attempt < max_attempts:
        # Standard backoff: 2s → 4s → 8s → 16s
        await asyncio.sleep(current_delay)
        current_delay *= backoff
    else:
        raise
```
- **Key Insight**: 503 errors need minutes to recover (API capacity), not seconds (transient network)
- **Result**: Jobs that previously failed after 3 quick retries now succeed after waiting for API recovery

### Automatic Job Chunking for Large-Scale Processing
- **Pattern**: Split large discoveries into multiple smaller jobs automatically
- **Benefit**: Prevents 503 errors, improves reliability, enables parallel processing
- **Implementation**:
```python
# Backend: Split input data into chunks
job_chunk_size = 50000  # Default, user-configurable 10k-100k
num_chunks = (total_items + job_chunk_size - 1) // job_chunk_size

job_ids = []
for chunk_idx in range(num_chunks):
    start_idx = chunk_idx * job_chunk_size
    end_idx = min(start_idx + job_chunk_size, total_items)
    chunk_data = input_data[start_idx:end_idx]

    # Create separate job for each chunk
    job_id = create_job(chunk_data, batch_size=batch_size)
    job_ids.append(job_id)

    # Start processing in background
    background_tasks.add_task(process_job, job_id)

return {"job_ids": job_ids, "jobs_created": len(job_ids)}

# Frontend: Display multiple jobs
if (multipleJobs) {
    message = `${jobs_created} jobs created (split into chunks of ~${items_per_job} each)`;
}
```
- **Example**: 240k ad groups → 5 jobs of 48k each (instead of 1 massive 240k job)
- **Benefits**:
  - Smaller jobs less likely to hit API limits
  - If one job fails, others continue
  - Can process multiple jobs in parallel
  - Easier to monitor and troubleshoot
- **Configuration**: User-adjustable chunk size (10k for safety, 50k default, 100k for speed)

### Google Ads Policy Crawler Rate Limiting (DESTINATION_NOT_WORKING)
- **Problem**: When creating ads in large batches, Google's policy crawler validates all destination URLs simultaneously
- **Impact**: CloudFront/WAF detects burst of requests from Google's crawler as bot attack → blocks requests → ads rejected with DESTINATION_NOT_WORKING
- **Symptoms**: All ads fail with "DESTINATION_NOT_WORKING" even though URLs work fine in browsers and Google Search Console
- **Root Cause**: Creating 5,000-10,000 ads at once → Google crawler tries to validate all URLs immediately → CloudFront rate limits/blocks crawler
- **Solution**: Reduce ad creation batch size and add delays
```python
# ❌ BEFORE: Large batches overwhelm crawler
BATCH_LIMIT = 10000  # Create 10k ads at once
time.sleep(2.0)      # Only 2s between batches

# Result: Google crawler hits CloudFront with 10k URL checks instantly
# → CloudFront blocks/rate limits → all ads rejected

# ✅ AFTER: Small batches spread over time
BATCH_LIMIT = 100    # Create only 100 ads per batch
time.sleep(5.0)      # 5s delay between batches

# Result: Google crawler checks 100 URLs every 5 seconds
# → CloudFront allows requests → ads approved
```
- **Impact**: For 5,000 ads: 50 batches × 5s = 250 seconds (~4 minutes) vs instant failure
- **Key Insight**: Google performs policy validation (including URL crawling) BEFORE creating ads, even for PAUSED ads
- **Trade-off**: Slower processing but much higher success rate (0% → expected 90%+)
- **Note**: This is separate from Googlebot for Search - Google Ads uses different crawler (AdsBot-Google)

### Google Ads API Quota Management
- **Problem**: Google Ads API has strict daily operation limits (15,000/day for Basic access, ~1M/day for Standard access)
- **Impact**: Large-scale jobs (100k+ ad groups) can consume millions of operations and hit quota limits
- **Solution**: Reduce operations per ad group by disabling non-essential labels
```python
# ❌ BEFORE: 6 operations per ad group
# 1. Create RSA ad
# 2. Label new ad with SINGLES_DAY
# 3. Label new ad with THEMA_AD
# 4. Label old ad with THEMA_ORIGINAL
# 5. Label ad group with BF_2025
# 6. Label ad group with SD_DONE

# ✅ AFTER: 4 operations per ad group (33% reduction)
# 1. Create RSA ad
# 2. Label new ad with SINGLES_DAY
# 3. Label old ad with THEMA_ORIGINAL
# 4. Label ad group with SD_DONE (critical for preventing reprocessing)

# Disabled labels (not essential for tracking)
ag_labels = [
    # (ad_group_resource, labels["BF_2025"]),  # Disabled
    (ad_group_resource, labels["SD_DONE"])     # Keep - prevents reprocessing
]

new_label_ops = []
for ad_res in new_ad_resources:
    new_label_ops.append((ad_res, labels["SINGLES_DAY"]))
    # new_label_ops.append((ad_res, labels["THEMA_AD"]))  # Disabled
```
- **Savings Example**: 240k ad groups × 2 operations = 480k operations saved
- **Key Insight**: Only SD_DONE label on ad group is critical; other labels are for organization/reporting but not functionality
- **Trade-off**: Less granular filtering in Google Ads UI, but 33% faster processing and lower quota consumption

### Rate Limiting Strategy for Google Ads API
- **Pattern**: Multi-layer rate limiting to prevent 503 errors and quota exhaustion
- **Layers**:
  1. **Batch Size**: Limit items per query (5000 default, user-configurable)
  2. **Batch Delays**: Wait between API calls (2s between batches)
  3. **Customer Delays**: Wait between customers (30s)
  4. **Concurrency Limits**: Max parallel customers (5 concurrent)
  5. **Extended Retries**: Long waits for 503 errors (up to 27 minutes)
  6. **Job Chunking**: Split large jobs into smaller chunks (50k items per job default)
  7. **Operation Reduction**: Disable non-essential labels (4 ops/ad group instead of 6)
- **Configuration**:
```python
# config.py - Performance tuning
PerformanceConfig(
    max_concurrent_customers=5,      # Reduced from 10
    batch_size=5000,                  # Reduced from 7500
    api_retry_attempts=5,             # Increased from 3
    api_retry_delay=2.0,              # Increased from 1.0
    api_batch_delay=2.0,              # Increased from 0.5
    customer_delay=30.0               # New parameter
)
```
- **Trade-off**: Slower processing but much higher success rate on large jobs
- **Use Case**: Jobs with 100k+ ad groups that previously hit 503 errors repeatedly

### Check-up Limit Parameter Not Respected - Full Account Scan
- **Problem**: Check-up with limit=100 still takes 30+ minutes, scanning all 662k ad groups
- **Root Cause**: Initial implementation processed all customers and checked all ad groups before applying limit
- **Impact**: Even with small test limits (100), Check-up would scan entire MCC (28 customers, hundreds of thousands of ad groups)
- **Solution**: Process customers sequentially and stop immediately when limit reached
```python
# ❌ BAD: Process all customers, then apply limit at end
for customer in all_customers:  # Processes all 28
    ad_groups = get_all_with_sd_done(customer)  # Gets all ad groups
    for ag in ad_groups:
        check_singles_day_label(ag)  # Checks all
    # ... collect results ...
# Apply limit here (too late!)
return results[:limit]

# ✅ GOOD: Stop as soon as limit reached
for customer in all_customers:
    if len(results) >= limit:
        break  # Stop processing more customers

    ad_groups = get_all_with_sd_done(customer)
    for ag in ad_groups:
        check_singles_day_label(ag)
        results.append(ag)
        if len(results) >= limit:
            break  # Stop processing this customer
```
- **Result**: With limit=100, only processes 1-2 customers instead of all 28, completes in 1-2 minutes instead of 30+

### Discovery Optimization: Direct Ad Query vs Nested Queries
- **Problem**: Discovery was slow for large accounts (146k ad groups took ~271 API queries)
- **Old Approach**: Nested queries (customers → campaigns → ad groups → label checks)
- **New Approach**: Direct ad query with cross-resource filtering + deduplication
- **Performance Improvement**: 74% fewer queries (271 → 71 for 146k ad groups)
- **How It Works**:
```python
# ❌ OLD: Nested queries (slow)
for customer in customers:
    campaigns = query("SELECT campaign FROM campaign WHERE name LIKE 'HS/%'")  # 1 per customer
    for campaign in campaigns:
        ad_groups = query("SELECT ad_group FROM ad_group WHERE campaign = X")  # 1 per campaign
    # + batch label checks

# ✅ NEW: Direct ad query with cross-resource filter (fast)
ad_group_map = {}
for customer in customers:
    # Single query gets all data using campaign.name filter
    ads = query("""
        SELECT ad_group_ad.ad_group, ad_group.id, campaign.id, campaign.name
        FROM ad_group_ad
        WHERE campaign.name LIKE 'HS/%'
        AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
        AND ad_group_ad.status != REMOVED
    """)  # 1 per customer

    # Deduplicate in memory (ad group may have multiple ads)
    for row in ads:
        if row.ad_group not in ad_group_map:
            ad_group_map[row.ad_group] = {...}
# + batch label checks
```
- **Key Techniques**:
  - **Cross-resource filtering**: Use `campaign.name` in `ad_group_ad` query (Google Ads API supports this)
  - **In-memory deduplication**: Use dict/set to deduplicate ad groups (multiple ads per ad group)
  - **Customer-grouped label checks**: Still batch-check SD_DONE labels per customer for efficiency

---
_Last updated: 2025-10-03_
