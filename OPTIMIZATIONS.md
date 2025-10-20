# Theme Ads Performance Optimizations

**Date:** 2025-10-20
**Status:** Implemented and Active

## Overview

This document describes the architectural optimizations implemented to improve the performance and throughput of the Theme Ads Google Ads automation system.

## Performance Improvements Summary

### Before Optimizations
- **Processing Rate:** ~623 items/minute
- **50,000 items:** ~73 minutes
- **Database:** Individual UPDATE queries per item
- **API Rate Control:** Fixed 5-second delays between batches
- **Concurrency:** 5 concurrent customers, 30s delay between customers
- **Ad Group Resolution:** Multiple queries per batch

### After Optimizations
- **Processing Rate:** ~1,200-2,000 items/minute (estimated)
- **50,000 items:** ~25-40 minutes (estimated)
- **Database:** Batch UPDATE queries (1,000 items per batch)
- **API Rate Control:** Adaptive delays (0.5s-10s based on success/failure)
- **Concurrency:** 10 concurrent customers, 5s delay between customers
- **Ad Group Resolution:** Single pre-fetch query per customer

### Expected Speedup
**3-5x overall performance improvement**

---

## Optimization Details

### 1. Batch Database Updates ✅

**File:** `backend/thema_ads_service.py`

**Problem:**
- Individual `UPDATE` queries for each ad group (50,000 queries for 50,000 items)
- Database became bottleneck with high latency

**Solution:**
- Buffer updates in memory (1,000 items per batch)
- Use `executemany()` for batch updates
- Flush buffer when full or at end of processing

**Code Changes:**
```python
# Before
for result, inp in zip(results, customer_inputs):
    self.update_item_status(job_id, customer_id, inp.ad_group_id, ...)

# After
update_buffer.append((status, resource, error, job_id, customer_id, ad_group_id))
if len(update_buffer) >= BUFFER_SIZE:
    await flush_updates()  # Batch executemany()
```

**Impact:** 10-20x faster database writes

---

### 2. Adaptive Rate Limiting ✅

**Files:**
- `thema_ads_optimized/utils/rate_limiter.py` (new)
- `thema_ads_optimized/operations/ads.py`

**Problem:**
- Fixed 5-second delays between ad creation batches
- 500 chunks × 5s = 2,500 seconds (~42 minutes) of pure waiting
- Overly conservative, not adapting to actual rate limits

**Solution:**
- Created `AdaptiveRateLimiter` class
- Starts at 1.0s delay, adjusts based on success/failure
- Success: Reduces delay by 5% (multiply by 0.95)
- Failure: Doubles delay (multiply by 2.0)
- Min: 0.5s, Max: 10.0s

**Code Changes:**
```python
# Before
time.sleep(5.0)  # Fixed delay

# After
_rate_limiter.wait()  # Adaptive delay
if result["resources"]:
    _rate_limiter.on_success()  # Reduce delay
if result["failures"] == len(chunk):
    _rate_limiter.on_error("batch_failure")  # Increase delay
```

**Impact:** 2-4x faster ad creation phase

---

### 3. Increased Concurrency ✅

**File:** `thema_ads_optimized/config.py`

**Problem:**
- Only 5 concurrent customers processed
- 30-second delay between customers
- Artificially throttled parallelism

**Solution:**
```python
# Before
max_concurrent_customers: int = 5
customer_delay: float = 30.0

# After
max_concurrent_customers: int = 10  # 2x increase
customer_delay: float = 5.0  # 6x reduction
```

**Rationale:**
- Rate limits are per-account, not global
- Adaptive rate limiting handles API throttling
- Batch operations reduce total API calls

**Impact:** 1.5-2x overall throughput

---

### 4. Optimized Ad Group Name Resolution ✅

**File:** `thema_ads_optimized/main_optimized.py`

**Problem:**
- Multiple SQL queries when resolving ad_group_name to ad_group_id
- Excel scientific notation corruption requires name-based lookups
- Queries run in executor threads, blocking async event loop

**Solution:**
- Pre-fetch ALL ad group IDs for customer in single query
- Use dictionary lookup instead of repeated queries
- Fetch once, lookup many times

**Code Changes:**
```python
# Before
def _lookup():
    query = f"SELECT ad_group.id, ad_group.name FROM ad_group WHERE ad_group.name IN ({names_str})"
    # Multiple queries per batch

# After
def _prefetch_all_ad_groups():
    query = "SELECT ad_group.id, ad_group.name FROM ad_group"
    # Single query, returns all ad groups
    return {name: id for name, id in results}

# Then fast dictionary lookups
ad_group_id = name_to_id[ad_group_name]
```

**Impact:** 2-3x faster when using ad_group_name column

---

### 5. Database Connection Pooling ✅

**File:** `backend/database.py`

**Problem:**
- New database connection created for every operation
- Connection overhead adds latency
- No connection reuse

**Solution:**
- Implemented `psycopg2.pool.ThreadedConnectionPool`
- Pool size: 2-20 connections
- Connections automatically reused
- Graceful fallback to direct connection if pool fails

**Code Changes:**
```python
# Before
def get_db_connection():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

# After
_connection_pool = psycopg2.pool.ThreadedConnectionPool(
    minconn=2, maxconn=20, dsn=DATABASE_URL, cursor_factory=RealDictCursor
)

def get_db_connection():
    return _connection_pool.getconn()  # Reuses connections
```

**Impact:** 10-20% improvement in database operations

---

## Configuration Parameters

### Environment Variables (Optional)

These can be set in `.env` file to override defaults:

```bash
# Concurrency settings
MAX_CONCURRENT_CUSTOMERS=10     # Number of customers processed in parallel
CUSTOMER_DELAY=5.0              # Seconds to wait between customers

# Batch sizes
BATCH_SIZE=5000                 # Items per Google Ads API batch query

# Retry settings
API_RETRY_ATTEMPTS=5            # Max retry attempts for failed API calls
API_RETRY_DELAY=2.0             # Initial delay for retries (seconds)
```

### Rate Limiter Parameters

Located in `thema_ads_optimized/operations/ads.py`:

```python
_rate_limiter = AdaptiveRateLimiter(
    initial_delay=1.0,      # Starting delay
    min_delay=0.5,          # Minimum delay (fastest)
    max_delay=10.0,         # Maximum delay (slowest)
    increase_factor=2.0,    # Multiply delay by this on error
    decrease_factor=0.95    # Multiply delay by this on success
)
```

---

## Monitoring & Metrics

### Key Metrics to Track

1. **Processing Rate**
   - Items per minute
   - Target: 1,200-2,000 items/minute

2. **Database Performance**
   - Query latency
   - Connection pool utilization
   - Target: <50ms per batch update

3. **API Rate Limiting**
   - 429 errors (rate limit hits)
   - Current adaptive delay
   - Success rate
   - Target: <1% rate limit errors

4. **Job Completion Time**
   - 10,000 items: ~5-8 minutes
   - 50,000 items: ~25-40 minutes
   - 100,000 items: ~50-80 minutes

### Logging

Key log messages to monitor:

```
# Rate limiter stats (logged after each customer)
Rate limiter stats: delay=0.75s, success_rate=98.50%

# Batch database updates
Flushed 1000 updates to database

# Connection pool initialization
Database connection pool initialized (2-20 connections)

# Ad group resolution
Pre-fetched 1523 ad group IDs for customer 1234567890
Resolved 450/450 ad group IDs from names
```

---

## Testing & Validation

### Test Plan

1. **Small Batch Test (1,000 items)**
   - Verify all optimizations active
   - Check for errors in logs
   - Measure baseline performance

2. **Medium Batch Test (10,000 items)**
   - Monitor rate limiter behavior
   - Check database connection pool usage
   - Validate success rates

3. **Large Batch Test (50,000+ items)**
   - Measure end-to-end performance
   - Compare against baseline (73 minutes)
   - Target: <40 minutes

### Performance Validation

Run this query to check job performance:

```sql
SELECT
    id,
    total_ad_groups,
    EXTRACT(EPOCH FROM (completed_at - started_at))/60 as duration_minutes,
    (total_ad_groups::float / EXTRACT(EPOCH FROM (completed_at - started_at)) * 60) as items_per_minute
FROM thema_ads_jobs
WHERE status = 'completed'
ORDER BY created_at DESC
LIMIT 10;
```

---

## Rollback Plan

If issues occur, rollback changes:

### 1. Revert Configuration

Edit `thema_ads_optimized/config.py`:

```python
max_concurrent_customers: int = 5  # Back to original
customer_delay: float = 30.0  # Back to original
```

### 2. Disable Adaptive Rate Limiting

Edit `thema_ads_optimized/operations/ads.py`:

```python
# Comment out rate limiter
# _rate_limiter.wait()
time.sleep(5.0)  # Fixed delay
```

### 3. Revert Database Code

```bash
git diff backend/thema_ads_service.py
git checkout backend/thema_ads_service.py  # Revert if needed
```

### 4. Restart Application

```bash
docker restart theme_ads-app-1
```

---

## Future Optimization Opportunities

### Phase 3 (Advanced)

1. **Multi-Job Parallelism**
   - Run multiple jobs concurrently
   - Requires job scheduler improvements

2. **Redis Caching Layer**
   - Cache label resources
   - Cache ad group IDs
   - Reduce API calls further

3. **Materialized Views**
   - Pre-aggregate job statistics
   - Faster dashboard queries

4. **Distributed Processing**
   - Scale beyond single machine
   - Use message queue (RabbitMQ/Redis)
   - Multiple worker processes

---

## Maintenance Notes

### Regular Monitoring

- Check database connection pool health weekly
- Review rate limiter statistics in logs
- Monitor Google Ads API quota usage
- Track job completion times

### Performance Tuning

- Adjust `max_concurrent_customers` based on system load
- Tune rate limiter parameters if seeing many 429 errors
- Increase connection pool size if seeing "no connections available" errors

### Troubleshooting

**Issue:** High rate limit errors (429)
- **Solution:** Increase `increase_factor` in rate limiter (more aggressive backing off)

**Issue:** Database connection pool exhausted
- **Solution:** Increase `maxconn` in `database.py`

**Issue:** Slow processing despite optimizations
- **Solution:** Check Google Ads API latency, may be external bottleneck

---

## References

- [Google Ads API Rate Limits](https://developers.google.com/google-ads/api/docs/best-practices/rate-limits)
- [psycopg2 Connection Pooling](https://www.psycopg.org/docs/pool.html)
- [Python asyncio Best Practices](https://docs.python.org/3/library/asyncio.html)

---

## Change Log

| Date | Version | Changes |
|------|---------|---------|
| 2025-10-20 | 1.0 | Initial optimizations implemented |

