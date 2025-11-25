# Auto-Queue Feature Implementation

**Date:** 2025-10-20
**Status:** ✅ Implemented and Tested

---

## Overview

Implemented an automatic job queue system that automatically starts the next pending job when the current job completes or fails.

## Features

### Core Functionality
- **FIFO Queue**: Jobs process in order of creation (oldest first)
- **30-Second Delay**: Waits 30 seconds between job completion and next job start
- **Failed Job Handling**: Queue continues even if a job fails
- **Persistent State**: Toggle state survives container restarts
- **Manual Control**: Toggle switch in UI to enable/disable queue

### User Interface
- Toggle switch in job management section
- Visual status indicator showing queue state
- Real-time status updates every 10 seconds
- Color-coded feedback (green=enabled, gray=disabled)

---

## Implementation Details

### 1. Database Changes
**File:** `backend/database.py`
- Added `system_settings` table to store queue state
- Added `get_auto_queue_enabled()` function
- Added `set_auto_queue_enabled(enabled)` function
- Integrated into `init_db()` initialization

### 2. Backend Service
**File:** `backend/thema_ads_service.py`
- Added `get_next_pending_job()` - Returns oldest pending job ID (FIFO)
- Added `_start_next_job_if_queue_enabled()` - Checks queue state and starts next job
- Modified `process_job()` to call queue checker after completion/failure
- 30-second delay implemented with `await asyncio.sleep(30)`

### 3. API Endpoints
**File:** `backend/main.py`
- `GET /api/thema-ads/queue/status` - Returns queue enabled state
- `POST /api/thema-ads/queue/enable` - Enable auto-queue
- `POST /api/thema-ads/queue/disable` - Disable auto-queue

### 4. Frontend UI
**File:** `frontend/thema-ads.html`
- Added toggle switch in job list header
- Added status text display area
- Styled with Bootstrap form-switch component

**File:** `frontend/js/thema-ads.js`
- Added `loadQueueStatus()` - Fetches and displays queue state
- Added `toggleAutoQueue()` - Handles toggle changes
- Auto-refresh queue status every 10 seconds
- Error handling with toggle reversion on failure

---

## Usage

### Enabling Auto-Queue

1. **Via UI:**
   - Navigate to http://localhost:8002/static/thema-ads.html
   - Find the "Auto-Queue" toggle in the job list header
   - Click to enable

2. **Via API:**
   ```bash
   curl -X POST http://localhost:8002/api/thema-ads/queue/enable
   ```

### Disabling Auto-Queue

1. **Via UI:**
   - Click the toggle again to disable

2. **Via API:**
   ```bash
   curl -X POST http://localhost:8002/api/thema-ads/queue/disable
   ```

### Checking Queue Status

```bash
curl http://localhost:8002/api/thema-ads/queue/status
```

Response:
```json
{
  "auto_queue_enabled": true
}
```

---

## Behavior

### When Queue is Enabled

1. Job completes or fails
2. System waits 30 seconds
3. Checks if queue is still enabled
4. Gets oldest pending job (FIFO)
5. Starts the job automatically
6. Repeats for each subsequent job

### When Queue is Disabled

- Jobs must be started manually via UI or API
- No automatic processing occurs
- Current running job continues until completion

---

## Testing Results

✅ **Database Migration:** Successfully created system_settings table
✅ **API Endpoints:** All 3 endpoints responding correctly
✅ **Toggle Functionality:** UI toggle updates state in real-time
✅ **State Persistence:** Queue state survives container restarts
✅ **FIFO Ordering:** Verified oldest jobs are selected first

### Current Status
- Job 213: Running (37.6% complete)
- Jobs 219-223: Pending (will auto-start after job 213 if queue enabled)
- Auto-queue: Currently enabled

---

## Files Modified

### New Files
1. `backend/migrations/002_add_auto_queue.sql` - Database migration

### Modified Files
1. `backend/database.py` - Queue state management functions
2. `backend/thema_ads_service.py` - Auto-queue logic
3. `backend/main.py` - Queue API endpoints
4. `frontend/thema-ads.html` - Toggle UI
5. `frontend/js/thema-ads.js` - Toggle functionality

---

## Configuration

### Timing
- **Inter-job delay:** 30 seconds (hardcoded in `_start_next_job_if_queue_enabled()`)
- **UI refresh rate:** 10 seconds for queue status, 5 seconds for jobs

### To Change Delay

Edit `backend/thema_ads_service.py` line 788:
```python
await asyncio.sleep(30)  # Change 30 to desired seconds
```

---

## Troubleshooting

### Queue Not Starting Next Job

1. **Check queue is enabled:**
   ```bash
   curl http://localhost:8002/api/thema-ads/queue/status
   ```

2. **Check for pending jobs:**
   ```bash
   curl http://localhost:8002/api/thema-ads/jobs
   ```

3. **Check container logs:**
   ```bash
   docker logs theme_ads-app-1 --tail 100 | grep -i "queue\|next job"
   ```

### Toggle Not Working

1. **Check browser console** for JavaScript errors
2. **Verify API connectivity:**
   ```bash
   curl http://localhost:8002/api/thema-ads/queue/status
   ```

3. **Clear browser cache** and reload page

---

## Future Enhancements

Potential improvements:
- Configurable delay via UI/settings
- Priority queue support (vs strict FIFO)
- Job scheduling by time/date
- Email/webhook notifications on job completion
- Queue analytics dashboard

---

## Monitoring

### Check Queue Activity in Logs

```bash
docker logs theme_ads-app-1 -f | grep -E "Auto-queue|next job|Queue"
```

Expected output when queue triggers:
```
Waiting 30 seconds before checking for next job...
Auto-queue: Starting next pending job 221
```

---

**Implementation Complete! The auto-queue system is now active and will automatically process pending jobs when enabled.**
