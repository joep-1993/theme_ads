# Google Ads API Rate Limiting - Configuration Options

## Current Issue Analysis

Based on the logs, the system is experiencing:
1. ~~Database connection pool exhaustion~~ (FIXED)
2. Need for more conservative API rate limiting

## Rate Limiting Strategy Options

### Option 1: CONSERVATIVE (Recommended for stability)

**Best for:** High volume processing where reliability > speed

**Configuration in `config.py`:**
```python
max_concurrent_customers: int = 5      # Reduce from 10 to 5
customer_delay: float = 15.0           # Increase from 5.0 to 15.0
batch_size: int = 5000                 # Keep at 5000
```

**Rate Limiter in `operations/ads.py`:**
```python
_rate_limiter = AdaptiveRateLimiter(
    initial_delay=2.0,      # Start slower (was 1.0)
    min_delay=1.0,          # Higher minimum (was 0.5)
    max_delay=15.0,         # Higher maximum (was 10.0)
    increase_factor=2.5,    # More aggressive backoff (was 2.0)
    decrease_factor=0.98    # Slower reduction (was 0.95)
)
```

**Expected Performance:**
- 50,000 items: ~45-60 minutes
- Rate limit errors: <0.5%
- More stable, fewer failures

---

### Option 2: BALANCED (Current - after connection pool fix)

**Best for:** Balanced speed and reliability

**Configuration in `config.py`:**
```python
max_concurrent_customers: int = 10     # Current setting
customer_delay: float = 5.0            # Current setting
batch_size: int = 5000                 # Current setting
```

**Rate Limiter in `operations/ads.py`:**
```python
_rate_limiter = AdaptiveRateLimiter(
    initial_delay=1.0,      # Current setting
    min_delay=0.5,          # Current setting
    max_delay=10.0,         # Current setting
    increase_factor=2.0,    # Current setting
    decrease_factor=0.95    # Current setting
)
```

**Expected Performance:**
- 50,000 items: ~30-40 minutes
- Rate limit errors: ~1-2%
- Good balance

---

### Option 3: AGGRESSIVE (Use with caution)

**Best for:** Small batches (<10,000 items) where speed is critical

**Configuration in `config.py`:**
```python
max_concurrent_customers: int = 15     # More parallelism
customer_delay: float = 2.0            # Minimal delay
batch_size: int = 7500                 # Larger batches
```

**Rate Limiter in `operations/ads.py`:**
```python
_rate_limiter = AdaptiveRateLimiter(
    initial_delay=0.5,      # Start fast
    min_delay=0.3,          # Very fast minimum
    max_delay=10.0,         # Same max
    increase_factor=2.0,    # Normal backoff
    decrease_factor=0.9     # Aggressive reduction
)
```

**Expected Performance:**
- 50,000 items: ~20-30 minutes
- Rate limit errors: ~3-5%
- Higher risk of throttling

---

## Google Ads API Rate Limits

**Official Limits:**
- **Basic access tier:** 1,000 requests per day
- **Standard access tier:** 15,000 requests per day
- **Peak requests:** Maximum bursts allowed temporarily

**Our Operation Costs:**
- Prefetch (per customer): ~3 API calls
- Ad creation (per 100 RSAs): ~1 API call
- Label operations: ~2-3 API calls per batch

**Example for 50,000 items:**
- ~500 batches × 1 call = 500 ad creation calls
- ~100 customers × 3 calls = 300 prefetch calls
- ~500 label operations = 500 calls
- **Total: ~1,300 API calls**

---

## Implementation Guide

### Step 1: Choose Your Strategy

Based on your needs:
- **Stability over speed?** → Choose Option 1 (Conservative)
- **Good balance?** → Keep Option 2 (Balanced - current)
- **Speed critical?** → Choose Option 3 (Aggressive)

### Step 2: Update Configuration

Edit `/home/jschagen/theme_ads/thema_ads_optimized/config.py`:

```python
@dataclass
class PerformanceConfig:
    """Performance tuning settings."""
    # CONSERVATIVE (Option 1)
    max_concurrent_customers: int = 5
    customer_delay: float = 15.0

    # OR BALANCED (Option 2) - Current
    # max_concurrent_customers: int = 10
    # customer_delay: float = 5.0

    # OR AGGRESSIVE (Option 3)
    # max_concurrent_customers: int = 15
    # customer_delay: float = 2.0
```

### Step 3: Update Rate Limiter

Edit `/home/jschagen/theme_ads/thema_ads_optimized/operations/ads.py`:

```python
# Find this section (around line 14)
_rate_limiter = AdaptiveRateLimiter(
    initial_delay=2.0,      # Choose from options above
    min_delay=1.0,          # Choose from options above
    max_delay=15.0,         # Choose from options above
    increase_factor=2.5,    # Choose from options above
    decrease_factor=0.98    # Choose from options above
)
```

### Step 4: Restart Application

```bash
docker restart theme_ads-app-1
```

---

## Monitoring Rate Limits

### Check for Rate Limit Errors

```bash
# Check recent logs for 429 errors
docker logs theme_ads-app-1 --tail 500 | grep -E "(429|RATE_LIMIT|quota)"

# Check rate limiter statistics
docker logs theme_ads-app-1 --tail 500 | grep "Rate limiter stats"
```

### Typical Log Output

**Good (no rate limiting):**
```
Rate limiter stats: delay=0.75s, success_rate=99.20%
Created 100 RSAs in chunk 42/150
```

**Rate limiting detected:**
```
Rate limiter: Error (batch_failure), delay: 2.00s -> 5.00s
Failed to create 100 RSAs in chunk 43/150
Rate limiter stats: delay=5.00s, success_rate=95.50%
```

---

## Advanced: Dynamic Rate Limiting Based on Time of Day

Google Ads API may have lower traffic during certain hours. Consider:

### Peak Hours (Higher Risk)
- **8 AM - 6 PM (your timezone)**
- Use **CONSERVATIVE** settings

### Off-Peak Hours (Lower Risk)
- **6 PM - 8 AM**
- Use **BALANCED** or **AGGRESSIVE** settings

### Implementation

Add time-based logic in `operations/ads.py`:

```python
from datetime import datetime

def get_rate_limiter_for_time():
    """Return rate limiter based on current time."""
    hour = datetime.now().hour

    if 8 <= hour < 18:  # Peak hours
        return AdaptiveRateLimiter(
            initial_delay=2.0, min_delay=1.0, max_delay=15.0,
            increase_factor=2.5, decrease_factor=0.98
        )
    else:  # Off-peak
        return AdaptiveRateLimiter(
            initial_delay=1.0, min_delay=0.5, max_delay=10.0,
            increase_factor=2.0, decrease_factor=0.95
        )

_rate_limiter = get_rate_limiter_for_time()
```

---

## Recommended Approach for Your Current Situation

Based on Job 172's performance (181 items/minute, many failures), I recommend:

### Immediate Action: Switch to CONSERVATIVE

1. **Edit `config.py`:**
```python
max_concurrent_customers: int = 5      # Reduce from 10
customer_delay: float = 15.0           # Increase from 5.0
```

2. **Edit `operations/ads.py`:**
```python
_rate_limiter = AdaptiveRateLimiter(
    initial_delay=2.0,      # Increase from 1.0
    min_delay=1.0,          # Increase from 0.5
    max_delay=15.0,         # Increase from 10.0
    increase_factor=2.5,    # Increase from 2.0
    decrease_factor=0.98    # Increase from 0.95
)
```

3. **Restart:**
```bash
docker restart theme_ads-app-1
```

### Expected Results

After switching to CONSERVATIVE:
- **More stable processing**
- **Fewer "Failed to create" errors**
- **Slower but more reliable** (~45-60 minutes for 50,000 items)
- **Better success rate** (>98%)

---

## Troubleshooting

### Issue: Still seeing many failures after changes

**Check:**
1. Are failures due to rate limits or other issues?
```bash
docker logs theme_ads-app-1 --tail 100 | grep "Failed to create" | head -5
```

2. Look for specific error messages:
   - `RATE_LIMIT_ERROR` → Need more conservative settings
   - `DESTINATION_NOT_WORKING` → CloudFront/website issue, not rate limit
   - `INVALID_AD_CUSTOMIZER` → Ad template issue, not rate limit

### Issue: Too slow, want to speed up

**Gradually increase:**
1. Start with CONSERVATIVE
2. Monitor for 24 hours
3. If success rate >98%, increase to BALANCED
4. Monitor again before going AGGRESSIVE

---

## Comparison Table

| Setting | Conservative | Balanced (Current) | Aggressive |
|---------|-------------|-------------------|------------|
| Concurrent Customers | 5 | 10 | 15 |
| Customer Delay | 15s | 5s | 2s |
| Initial Delay | 2.0s | 1.0s | 0.5s |
| Min Delay | 1.0s | 0.5s | 0.3s |
| Max Delay | 15.0s | 10.0s | 10.0s |
| **50K Items (est)** | 45-60 min | 30-40 min | 20-30 min |
| **Success Rate (est)** | 98-99% | 95-97% | 90-95% |
| **Use Case** | Large jobs | Medium jobs | Small jobs |

---

## Additional Recommendations

### 1. Batch Size Tuning

The current batch size of 100 RSAs per chunk is conservative. Options:

- **More conservative:** 50 RSAs per chunk
  - Fewer items fail together
  - More API calls (slower)

- **Current:** 100 RSAs per chunk (RECOMMENDED)
  - Good balance

- **Aggressive:** 200 RSAs per chunk
  - Fewer API calls (faster)
  - More items fail together if error occurs

### 2. Exponential Backoff on Specific Errors

Consider adding error-specific handling:

```python
if "RATE_LIMIT" in error_msg:
    _rate_limiter.on_error("rate_limit")
    time.sleep(30)  # Extra delay for rate limits
elif "DESTINATION_NOT_WORKING" in error_msg:
    # Don't increase delay, this is website issue
    pass
else:
    _rate_limiter.on_error("other")
```

### 3. Health Monitoring Dashboard

Create a simple monitoring endpoint:

```python
@app.get("/api/health/rate-limiter")
def get_rate_limiter_health():
    stats = _rate_limiter.get_stats()
    return {
        "current_delay": stats['current_delay'],
        "success_rate": stats['success_rate'],
        "health": "good" if stats['success_rate'] > 0.95 else "degraded"
    }
```

---

## Quick Reference Commands

```bash
# Check current job status
curl -s http://localhost:8002/api/thema-ads/jobs/172 | python3 -m json.tool

# Monitor rate limiter in real-time
docker logs theme_ads-app-1 --follow | grep -E "(Rate limiter|Failed|Created)"

# Count failures in last 1000 log lines
docker logs theme_ads-app-1 --tail 1000 | grep "Failed to create" | wc -l

# Check API error types
docker logs theme_ads-app-1 --tail 1000 | grep "FaultMessage" | sort | uniq -c
```

---

## Questions to Consider

Before choosing a strategy, ask:

1. **What's more important: speed or reliability?**
   - Reliability → CONSERVATIVE
   - Balanced → BALANCED (current)
   - Speed → AGGRESSIVE

2. **What's your Google Ads API tier?**
   - Basic (1K requests/day) → CONSERVATIVE only
   - Standard (15K requests/day) → Any option

3. **How large are your typical jobs?**
   - <10K items → AGGRESSIVE or BALANCED
   - 10K-50K items → BALANCED or CONSERVATIVE
   - >50K items → CONSERVATIVE

4. **Can you run jobs during off-peak hours?**
   - Yes → Use time-based dynamic settings
   - No → Use CONSERVATIVE

---

## Implementation Priority

For your current situation, implement in this order:

1. ✅ **DONE:** Fix connection pool exhaustion
2. **NEXT:** Switch to CONSERVATIVE settings (5 minutes)
3. **MONITOR:** Run for 2-4 hours, check success rate
4. **OPTIMIZE:** Adjust based on results

Would you like me to implement the CONSERVATIVE settings now?
