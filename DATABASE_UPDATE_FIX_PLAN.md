# Database Update Issue - Root Cause Analysis & Fix Plan

**Date:** 2025-10-20
**Issue:** Job progress is not updated in database during processing
**Impact:** Frontend shows 0% progress even though jobs are actively processing

---

## Root Cause Analysis

### Problem Summary

Jobs process successfully (RSAs are created in Google Ads) but database status is never updated, causing:
1. Frontend shows 0% progress indefinitely
2. Users can't track job progress in real-time
3. Job statistics (successful/failed counts) remain at 0

### Root Causes Identified

#### 1. **Hot-Reload Kills Background Tasks**
**Location:** FastAPI Uvicorn watch mode

**Issue:**
- When code files are edited, Uvicorn auto-reloads the application
- Background async tasks (running jobs) are killed during reload
- Job status in database remains "running" but no actual processing happens
- New code changes don't apply to already-running jobs

**Evidence:**
```
WARNING:  WatchFiles detected changes in 'backend/thema_ads_service.py'. Reloading...
INFO:     Shutting down
INFO:     Waiting for application shutdown.
```

Jobs were restarted 3+ times during debugging, each time killing the background task.

---

#### 2. **Background Tasks Use In-Memory Code**
**Location:** `backend/thema_ads_service.py:process_job()`

**Issue:**
- When a job starts, it loads the Python code into memory
- The `_process_with_tracking()` method is bound to that specific instance
- Even after file edits and auto-reload, the running task uses OLD code
- New logging and fixes never execute because they're not in the running instance

**Evidence:**
- Added extensive logging (🔵 START, 🟢 FINISHED, etc.)
- Logs never appeared even though job was processing
- RSAs were being created (old code path) but new logging code never ran

---

#### 3. **Async/Sync Mismatch Potential**
**Location:** `backend/thema_ads_service.py:392-407`

**Issue:**
- `update_item_status()` is a synchronous method
- Called from async context (`_process_with_tracking`)
- While Python allows this, it may block the event loop
- Could cause slowdowns or prevent commits from executing

**Code:**
```python
async def process_with_tracking(...):
    # Async function
    self.update_item_status(...)  # Sync call - potential issue
```

---

#### 4. **Database Connection Management**
**Location:** `backend/database.py:get_db_connection()`

**Issue:**
- Each `update_item_status()` call creates a new database connection
- With high concurrency (5-10 customers × many ad groups), this creates many connections
- Connections may not be properly closed if exceptions occur
- Could exhaust database connection limit

**Current Implementation:**
```python
def update_item_status(...):
    conn = get_db_connection()  # New connection every time
    cur = conn.cursor()
    try:
        # Update queries
        conn.commit()
    finally:
        cur.close()
        conn.close()  # May not execute if exception
```

---

## Fix Plan

### Phase 1: Immediate Fixes (Highest Priority)

#### Fix 1.1: Make update_item_status Async
**File:** `backend/thema_ads_service.py`

**Problem:** Sync method called from async context

**Solution:**
```python
async def update_item_status_async(self, job_id: int, customer_id: str,
                                   ad_group_id: str, status: str,
                                   new_ad_resource: Optional[str] = None,
                                   error_message: Optional[str] = None):
    """Async version of update_item_status using run_in_executor."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        None,  # Uses default ThreadPoolExecutor
        self._update_item_status_sync,
        job_id, customer_id, ad_group_id, status,
        new_ad_resource, error_message
    )

def _update_item_status_sync(self, job_id: int, customer_id: str,
                             ad_group_id: str, status: str,
                             new_ad_resource: Optional[str] = None,
                             error_message: Optional[str] = None):
    """Synchronous database update (runs in thread pool)."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            cur.execute("""
                UPDATE thema_ads_job_items
                SET status = %s, new_ad_resource = %s,
                    error_message = %s, processed_at = CURRENT_TIMESTAMP
                WHERE job_id = %s AND customer_id = %s AND ad_group_id = %s
            """, (status, new_ad_resource, error_message, job_id, customer_id, ad_group_id))

            # Update job stats less frequently (every 10th update)
            if hash(ad_group_id) % 10 == 0:  # Only 10% of updates
                cur.execute("""
                    UPDATE thema_ads_jobs
                    SET processed_ad_groups = (
                            SELECT COUNT(*) FROM thema_ads_job_items
                            WHERE job_id = %s AND status IN ('successful', 'failed', 'skipped')
                        ),
                        successful_ad_groups = (
                            SELECT COUNT(*) FROM thema_ads_job_items
                            WHERE job_id = %s AND status = 'successful'
                        ),
                        failed_ad_groups = (
                            SELECT COUNT(*) FROM thema_ads_job_items
                            WHERE job_id = %s AND status = 'failed'
                        ),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (job_id, job_id, job_id, job_id))

            conn.commit()
            logger.debug(f"Updated item {ad_group_id}: {status}")

        finally:
            cur.close()
            conn.close()

    except Exception as e:
        logger.error(f"Failed to update item {ad_group_id}: {e}")
        # Don't raise - continue processing even if DB update fails
```

**Benefits:**
- Doesn't block event loop
- Proper async/await flow
- Reduces job stats query frequency (10x less)

---

#### Fix 1.2: Update _process_with_tracking to Use Async Updates
**File:** `backend/thema_ads_service.py`

**Change:**
```python
async def _process_with_tracking(self, processor, inputs, job_id):
    # ... existing code ...

    async def process_with_tracking(customer_id, customer_inputs):
        async with semaphore:
            results = await processor.process_customer(customer_id, customer_inputs)

            # Update database with results (NOW ASYNC)
            for result, inp in zip(results, customer_inputs):
                # Determine status
                if result.success and result.error and "Already processed" in result.error:
                    status = 'skipped'
                elif not result.success and result.error and "No existing ad" in result.error:
                    status = 'skipped'
                elif result.success:
                    status = 'successful'
                else:
                    status = 'failed'

                # CHANGED: Use async version
                await self.update_item_status_async(
                    job_id, customer_id, inp.ad_group_id, status,
                    result.new_ad_resource if result.success else None,
                    result.error
                )

            return results
```

---

#### Fix 1.3: Disable Auto-Reload in Production
**File:** `docker-compose.yml` or startup script

**Problem:** Auto-reload kills running jobs

**Solution:**
```yaml
# docker-compose.yml - app service
command: uvicorn backend.main:app --host 0.0.0.0 --port 8000 --no-reload

# OR set environment variable
environment:
  - UVICORN_RELOAD=false
```

**For Development:**
- Keep auto-reload enabled
- But pause all jobs before making code changes
- Resume after changes are applied

---

### Phase 2: Architectural Improvements (Medium Priority)

#### Fix 2.1: Separate Worker Process
**New Architecture:**

```
┌─────────────────┐
│   FastAPI App   │  (API only, no background tasks)
│   Port 8000     │
└────────┬────────┘
         │
         │ Updates job status via DB
         │
         ▼
┌─────────────────┐
│    Database     │  (Job queue)
│   PostgreSQL    │
└────────┬────────┘
         │
         │ Polls for jobs
         │
         ▼
┌─────────────────┐
│  Worker Process │  (Runs jobs independently)
│  Separate Python│  (Can restart without killing jobs)
│     Process     │
└─────────────────┘
```

**Implementation:**
```python
# workers/job_worker.py
import asyncio
import time
from backend.database import get_db_connection
from thema_ads_optimized.main_optimized import ThemaAdsProcessor

async def worker_loop():
    """Independent worker that processes jobs."""
    processor = ThemaAdsProcessor()

    while True:
        # Poll for pending/running jobs
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id FROM thema_ads_jobs
            WHERE status IN ('pending', 'running')
            ORDER BY created_at LIMIT 1
        """)
        job = cur.fetchone()
        cur.close()
        conn.close()

        if job:
            # Process job
            await process_job_worker(processor, job['id'])
        else:
            # No jobs, sleep
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(worker_loop())
```

**Benefits:**
- API and worker are independent
- Can restart API without killing jobs
- Can run multiple workers for parallelism
- Better error isolation

---

#### Fix 2.2: Batch Database Updates with Lock
**File:** `backend/thema_ads_service.py`

**Problem:** Individual updates are slow with high volume

**Solution:**
```python
import asyncio
from threading import Lock

class DatabaseUpdateBuffer:
    """Thread-safe buffer for batch database updates."""

    def __init__(self, buffer_size=100, flush_interval=10.0):
        self.buffer = []
        self.lock = Lock()
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        self.last_flush = time.time()

    async def add(self, job_id, customer_id, ad_group_id, status,
                  new_ad_resource=None, error_message=None):
        """Add item to buffer and flush if needed."""
        with self.lock:
            self.buffer.append((
                status, new_ad_resource, error_message,
                job_id, customer_id, ad_group_id
            ))

            # Flush if buffer full or time elapsed
            should_flush = (
                len(self.buffer) >= self.buffer_size or
                (time.time() - self.last_flush) > self.flush_interval
            )

        if should_flush:
            await self.flush()

    async def flush(self):
        """Flush buffer to database in batch."""
        with self.lock:
            if not self.buffer:
                return

            batch = self.buffer.copy()
            self.buffer.clear()
            self.last_flush = time.time()

        # Execute batch update in thread pool
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._flush_sync, batch)

    def _flush_sync(self, batch):
        """Synchronous batch flush."""
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.executemany("""
                UPDATE thema_ads_job_items
                SET status = %s, new_ad_resource = %s,
                    error_message = %s, processed_at = CURRENT_TIMESTAMP
                WHERE job_id = %s AND customer_id = %s AND ad_group_id = %s
            """, batch)
            conn.commit()
            logger.info(f"Flushed {len(batch)} updates to database")
        finally:
            cur.close()
            conn.close()
```

**Usage:**
```python
# Create buffer instance per job
buffer = DatabaseUpdateBuffer(buffer_size=100, flush_interval=10.0)

# In processing loop
await buffer.add(job_id, customer_id, ad_group_id, status, ...)

# Ensure final flush
await buffer.flush()
```

**Benefits:**
- 10-50x faster database writes
- Proper thread safety
- Time-based flushing ensures updates appear regularly

---

### Phase 3: Monitoring & Resilience (Lower Priority)

#### Fix 3.1: Job Heartbeat Monitoring
**Purpose:** Detect stuck jobs

```python
# Add to job processing loop
async def update_job_heartbeat(job_id):
    """Update job heartbeat to detect stuck jobs."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE thema_ads_jobs
        SET last_heartbeat = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (job_id,))
    conn.commit()
    cur.close()
    conn.close()

# Call every 30 seconds during processing
while processing:
    await update_job_heartbeat(job_id)
    await asyncio.sleep(30)
```

**Add database column:**
```sql
ALTER TABLE thema_ads_jobs
ADD COLUMN last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
```

**Monitoring query:**
```sql
-- Find stuck jobs (no heartbeat in 5 minutes)
SELECT id, status, started_at, last_heartbeat
FROM thema_ads_jobs
WHERE status = 'running'
  AND last_heartbeat < NOW() - INTERVAL '5 minutes';
```

---

#### Fix 3.2: Progress Estimation
**Purpose:** Show estimated progress even without database updates

```python
# Add to job record
estimated_items_per_minute: int
estimated_completion_time: datetime

# Calculate on job start
def calculate_estimates(total_items):
    # Based on historical data or conservative estimate
    items_per_minute = 800  # CONSERVATIVE setting
    duration_minutes = total_items / items_per_minute
    completion_time = datetime.now() + timedelta(minutes=duration_minutes)
    return items_per_minute, completion_time

# Frontend can show estimated progress
estimated_progress = min(
    100,
    (elapsed_minutes / total_estimated_minutes) * 100
)
```

---

#### Fix 3.3: Database Connection Pooling (Proper Implementation)
**File:** `backend/database.py`

**Problem:** Previous pooling attempt failed

**Solution using asyncpg (async-native):**
```python
import asyncpg
import asyncio

_pool = None

async def init_pool():
    """Initialize async connection pool."""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=os.getenv("DATABASE_URL"),
            min_size=2,
            max_size=20,
            command_timeout=60
        )
    return _pool

async def execute_query(query, *args):
    """Execute query using pool."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(query, *args)

async def execute_many(query, args_list):
    """Execute many queries in batch."""
    pool = await init_pool()
    async with pool.acquire() as conn:
        await conn.executemany(query, args_list)
```

**Requires:**
```bash
pip install asyncpg
```

**Benefits:**
- True async database operations
- Proper connection pooling
- Better performance under load

---

## Implementation Priority

### Immediate (Do First):
1. ✅ **Fix 1.1**: Make update_item_status async
2. ✅ **Fix 1.2**: Update _process_with_tracking
3. ✅ **Fix 1.3**: Disable auto-reload in production

**Expected Result:** Database updates work, jobs show progress

### Short Term (Next Week):
4. **Fix 2.2**: Implement batch updates with lock
5. **Fix 3.1**: Add heartbeat monitoring

**Expected Result:** Faster updates, better monitoring

### Medium Term (Next Month):
6. **Fix 2.1**: Separate worker process
7. **Fix 3.3**: Proper async connection pooling

**Expected Result:** Production-grade architecture

---

## Testing Plan

### Test 1: Single Job (1,000 items)
**Purpose:** Verify database updates work

**Steps:**
1. Implement Fix 1.1, 1.2, 1.3
2. Start fresh job with 1,000 items
3. Monitor frontend every 30 seconds
4. Verify progress updates from 0% → 100%

**Success Criteria:**
- Progress shows 1%, 2%, 3%... continuously
- Final status shows correct counts
- No stuck "running" jobs

---

### Test 2: Medium Job (10,000 items)
**Purpose:** Verify performance at scale

**Steps:**
1. Start job with 10,000 items
2. Monitor database query performance
3. Check for slowdowns or timeouts

**Success Criteria:**
- Progress updates smoothly
- API remains responsive
- Job completes in <15 minutes

---

### Test 3: Multiple Jobs (3 concurrent)
**Purpose:** Verify concurrency handling

**Steps:**
1. Start 3 jobs simultaneously
2. Monitor system resources
3. Verify all complete successfully

**Success Criteria:**
- All 3 jobs show progress
- No database deadlocks
- No connection exhaustion

---

## Rollback Plan

If fixes cause issues:

### Step 1: Revert Code Changes
```bash
cd /home/jschagen/theme_ads
git diff backend/thema_ads_service.py
git checkout backend/thema_ads_service.py
```

### Step 2: Restart Application
```bash
docker restart theme_ads-app-1
```

### Step 3: Verify System Stable
```bash
curl http://localhost:8002/api/thema-ads/jobs
```

---

## Estimated Timeline

| Phase | Tasks | Time | Dependencies |
|-------|-------|------|--------------|
| **Phase 1** | Fix 1.1-1.3 | 2-3 hours | None |
| **Testing** | Test 1-3 | 1-2 hours | Phase 1 |
| **Phase 2** | Fix 2.1-2.2 | 1-2 days | Phase 1 tested |
| **Phase 3** | Fix 3.1-3.3 | 3-5 days | Phase 2 tested |

**Total:** 1 week for full implementation and testing

---

## Success Metrics

### Before Fixes:
- ❌ Progress stuck at 0%
- ❌ No real-time updates
- ❌ API timeouts under load
- ❌ Jobs show "running" after completion

### After Fixes:
- ✅ Progress updates every 10-30 seconds
- ✅ Accurate success/failure counts
- ✅ API responsive during processing
- ✅ Jobs transition to "completed" properly
- ✅ Can restart app without killing jobs (Phase 2)

---

## Questions & Clarifications

Before implementing, confirm:

1. **Production vs Development:**
   - Is auto-reload needed in production?
   - Can we disable it for stability?

2. **Performance Requirements:**
   - Is 10-30 second update latency acceptable?
   - Or do you need real-time (< 1 second)?

3. **Infrastructure:**
   - Can we add a separate worker process?
   - Or must everything run in one container?

4. **Database:**
   - Can we add new columns (heartbeat)?
   - Can we install asyncpg library?

---

## Next Steps

**Recommend starting with Phase 1** (Fixes 1.1-1.3):
1. Implement async database updates
2. Disable auto-reload
3. Test with small job (1,000 items)
4. If successful, proceed to Phase 2

**Would you like me to:**
- Implement Phase 1 fixes now?
- Create a test job to validate the fixes?
- Answer any questions about the plan?
