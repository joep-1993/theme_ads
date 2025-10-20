# Job 213 Stall - Root Cause Analysis & Optimization Plan

**Date:** 2025-10-20
**Job:** 213
**Status:** Failed at 43.2% (21,595/50,000 items)

---

## Root Cause Analysis

### What Happened

Job 213 was processing normally and reached 43.2% completion (21,595/50,000 items) when it **suddenly stopped** at approximately 14:32 on 2025-10-20.

### Investigation Findings

1. **No Error Logs:** No API errors, exceptions, or failures in the application logs around the stall time
2. **Resource Usage:** Container was healthy (73MB RAM, 0.65% CPU)
3. **Silent Stop:** Processing just stopped with no completion or error message
4. **Database State:** Job remained marked as "running" despite no activity

### The Actual Root Cause

**Container Auto-Reload Killed the Job**

```
2025-10-20 14:30:13 - Last processing activity
WARNING:  WatchFiles detected changes in 'backend/database.py'. Reloading...
INFO:     Shutting down
INFO:     Waiting for background tasks to complete. (CTRL+C to quit)
ERROR:    Cancel 1 running task(s), timeout graceful shutdown exceeded
asyncio.exceptions.CancelledError: Task cancelled, timeout graceful shutdown exceeded
```

**Timeline:**
1. 14:03 - Job 213 started
2. 14:30 - Processing reached 43.2% (~21,595 items)
3. 14:30 - We added auto-queue feature and edited `backend/database.py`
4. 14:30 - Uvicorn's auto-reload detected file change
5. 14:30 - Container restarted, **killing the background job mid-process**
6. 14:30-16:00 - Job remained in "running" state but no actual processing
7. 16:00 - Investigation revealed the issue

---

## The Real Problem: Development Mode in Production

### Issue

The container is running with **Uvicorn's development mode** (`--reload` flag), which:
- Watches for file changes
- Auto-restarts when code changes
- **Kills all background tasks** including long-running jobs

### Why This is Bad

- ❌ Long-running jobs (1-48 hours) get killed on any code change
- ❌ Jobs remain in "running" state but aren't actually running
- ❌ No visibility that job was terminated
- ❌ No graceful shutdown or state persistence
- ❌ Production deployments should not auto-reload

---

## Optimization Plan

### Priority 1: Fix Auto-Reload Issue (CRITICAL)

**Problem:** Development mode auto-reload kills long-running jobs

**Solutions:**

#### Option A: Disable Auto-Reload (Recommended for Production)
```dockerfile
# In Dockerfile, change CMD to:
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
# Remove --reload flag
```

**Pros:**
- No more surprise job kills
- Production-ready
- More stable

**Cons:**
- Must manually restart container after code changes
- Slightly slower development iteration

#### Option B: Graceful Shutdown with Job State Persistence
```python
# In backend/thema_ads_service.py
import signal
import sys

def handle_shutdown(signum, frame):
    """Handle graceful shutdown on SIGTERM"""
    logger.warning("Shutdown signal received, pausing running jobs...")
    if self.current_job_id:
        self.pause_job(self.current_job_id)
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_shutdown)
```

**Pros:**
- Jobs auto-pause on container restart
- Can resume after restart
- Better development experience

**Cons:**
- More complex implementation
- Still interrupts processing

#### Option C: Separate Worker Process
```yaml
# docker-compose.yml - Run web and worker separately
services:
  app:  # Web API only
    command: uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload

  worker:  # Job processing only (no reload)
    command: python backend/worker.py  # Separate worker script
```

**Pros:**
- Web API can reload without killing jobs
- Production-grade architecture
- Scalable (multiple workers)

**Cons:**
- Requires refactoring
- More complex setup

---

### Priority 2: Job Health Monitoring

**Problem:** Job appeared "running" but was actually dead for 1.5 hours

**Solution:** Add heartbeat monitoring

```python
# In backend/thema_ads_service.py

async def process_job(self, job_id: int):
    # Start heartbeat task
    heartbeat_task = asyncio.create_task(self._job_heartbeat(job_id))

    try:
        # ... existing processing ...
    finally:
        heartbeat_task.cancel()

async def _job_heartbeat(self, job_id: int):
    """Update job heartbeat every 60 seconds"""
    while True:
        await asyncio.sleep(60)
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
```

**Database Migration:**
```sql
ALTER TABLE thema_ads_jobs ADD COLUMN last_heartbeat TIMESTAMP;
```

**Benefits:**
- Detect stalled jobs (no heartbeat > 5 minutes)
- Auto-mark as failed
- Alert on stuck jobs

---

### Priority 3: Job Resume on Container Restart

**Problem:** When container restarts, running jobs are lost

**Solution:** Auto-resume on startup

```python
# In backend/main.py

@app.on_event("startup")
async def resume_interrupted_jobs():
    """Resume jobs that were running when container stopped"""
    jobs = thema_ads_service.list_jobs(limit=100)
    for job in jobs:
        if job['status'] == 'running':
            # Check if job is actually running (heartbeat recent)
            if job.get('last_heartbeat'):
                time_since = datetime.now() - job['last_heartbeat']
                if time_since.seconds > 300:  # 5 minutes
                    # Job is stale, mark as failed
                    thema_ads_service.update_job_status(
                        job['id'],
                        'failed',
                        error_message='Job interrupted by container restart'
                    )
            else:
                # No heartbeat, definitely stale
                thema_ads_service.update_job_status(
                    job['id'],
                    'failed',
                    error_message='Job interrupted by container restart'
                )
```

**Benefits:**
- Clean up zombie "running" jobs on startup
- Visibility into interrupted jobs
- Prevent false "running" states

---

### Priority 4: Better Error Reporting

**Problem:** Job silently died with no notification

**Solution:** Add comprehensive logging and alerts

```python
# Log every major stage
logger.info(f"Job {job_id} started: {len(inputs)} items")
logger.info(f"Job {job_id} progress: {processed}/{total} ({pct}%)")
logger.info(f"Job {job_id} completed: {success}/{total} successful")

# Add exception handlers
try:
    results = await self._process_with_tracking(processor, inputs, job_id)
except asyncio.CancelledError:
    logger.error(f"Job {job_id} CANCELLED - container restart detected")
    self.update_job_status(job_id, 'failed',
                          error_message='Job cancelled due to container restart')
    raise
except Exception as e:
    logger.error(f"Job {job_id} FAILED: {e}", exc_info=True)
    self.update_job_status(job_id, 'failed', error_message=str(e))
    raise
```

---

### Priority 5: Rate Limiting Improvements

**Observation:** Many policy violation errors observed:
```
FaultMessage: The resource has been disapproved since the policy summary includes policy topics of type PROHIBITED
```

**Recommendations:**

1. **Pre-validate Ad Content:** Check ad content before submission to catch policy violations early
2. **Better Error Handling:** Categorize failures (policy vs technical vs temporary)
3. **Automatic Retry:** Retry temporary failures, skip policy violations
4. **Error Reporting:** Surface common policy violation patterns to user

---

## Implementation Priority

### Immediate (Must Fix)
1. ✅ Mark job 213 as failed (DONE)
2. ✅ **Disable Auto-Reload** - Switch to production mode (DONE)
3. ✅ **Add Startup Job Cleanup** - Mark stale "running" jobs as failed on startup (DONE)

### Short Term (This Week)
4. **Add Heartbeat Monitoring** - Detect stalled jobs
5. **Improve Error Logging** - Catch CancelledError explicitly
6. **Better Exception Handling** - Handle container restarts gracefully

### Medium Term (Next Sprint)
7. **Separate Worker Process** - Decouple web API from job processing
8. **Job Resume Logic** - Auto-resume interrupted jobs
9. **Enhanced Monitoring** - Dashboard showing job health, heartbeats, errors

---

## Recommended Configuration Changes

### Dockerfile
```dockerfile
# Current (Development)
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# Recommended (Production)
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
```

### docker-compose.yml
```yaml
# Add environment variable to control reload
environment:
  - RELOAD=${RELOAD:-false}  # Default to false (production mode)

# Use in Dockerfile CMD:
# CMD uvicorn backend.main:app --host 0.0.0.0 --port 8000 ${RELOAD:+--reload}
```

---

## Testing Plan

### Test 1: Verify No Auto-Reload
1. Start container with production mode
2. Start a job
3. Edit a Python file
4. Verify job continues running

### Test 2: Graceful Shutdown
1. Start a job
2. Restart container
3. Verify job is marked as failed
4. Verify error message explains what happened

### Test 3: Heartbeat Detection
1. Start a job
2. Simulate job hang (pause processing)
3. Wait 5 minutes
4. Verify system detects stale job and marks as failed

---

## Lessons Learned

1. **Development vs Production:** Auto-reload is great for dev but deadly for long-running jobs
2. **Background Tasks:** Uvicorn doesn't wait for background tasks during reload
3. **State Management:** Jobs need heartbeat/liveness checks
4. **Graceful Degradation:** System should detect and handle interrupted jobs
5. **Monitoring:** Need visibility into job health beyond just "status" field

---

## Summary

**Root Cause:** Container auto-reload killed job mid-process due to code changes during development

**Impact:** Job appeared running but was dead for 1.5 hours, causing confusion and wasted time

**Fix:** Disable auto-reload in production, add heartbeat monitoring, handle restarts gracefully

**Prevention:** Implement proper job lifecycle management and monitoring

---

## Implementation Status

### Priority 1 Fixes - COMPLETED (2025-10-20)

#### 1. Disabled Auto-Reload
**Files Modified:**
- `Dockerfile` - Removed `--reload` flag from CMD
- `docker-compose.yml` - Removed `--reload` flag from command override

**Changes:**
```yaml
# docker-compose.yml (line 15)
# Before:
command: uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload --timeout-keep-alive 600 --timeout-graceful-shutdown 60

# After:
command: uvicorn backend.main:app --host 0.0.0.0 --port 8000 --timeout-keep-alive 600 --timeout-graceful-shutdown 60
```

**Verification:**
- ✅ Container logs show "Started server process" (single process, no reloader)
- ✅ No "Will watch for changes" message
- ✅ File changes no longer trigger container restart
- ✅ Tested by editing `backend/database.py` - no reload occurred

#### 2. Added Startup Job Cleanup
**Files Modified:**
- `backend/main.py` - Added `@app.on_event("startup")` handler

**Implementation:**
```python
@app.on_event("startup")
async def cleanup_stale_jobs():
    """Clean up stale 'running' jobs on startup (jobs interrupted by container restart)."""
    logger.info("Checking for stale running jobs...")
    jobs = thema_ads_service.list_jobs(limit=100)

    stale_count = 0
    for job in jobs:
        if job['status'] == 'running':
            logger.warning(f"Found stale running job {job['id']}, marking as failed")
            thema_ads_service.update_job_status(
                job['id'],
                'failed',
                error_message='Job interrupted by container restart'
            )
            stale_count += 1

    if stale_count > 0:
        logger.info(f"Cleaned up {stale_count} stale running jobs")
```

**Benefits:**
- Automatically detects jobs left in "running" state after container restart
- Marks them as failed with descriptive error message
- Prevents false "running" status in job list
- Runs on every container startup

**Testing:**
- Container restart performed - cleanup executed successfully
- No stale "running" jobs found (all jobs were already in correct state)

### Impact

**Before:**
- Any code change would kill running jobs mid-process
- Jobs remained in "running" state but weren't actually running
- No visibility that jobs were terminated
- Container restarts caused silent job failures

**After:**
- Code changes require manual container restart (prevents accidental kills)
- Stale "running" jobs automatically cleaned up on startup
- Production-ready configuration
- Clear error messages for interrupted jobs

### Next Steps

The Priority 1 fixes are complete. Consider implementing Priority 2-5 items from the optimization plan:
- **Priority 2:** Heartbeat monitoring, better error logging
- **Priority 3:** Separate worker process, job resume logic
- **Priority 4:** Enhanced monitoring dashboard

---

_Investigation completed: 2025-10-20 16:00_
_Priority 1 fixes implemented: 2025-10-20 17:30_
