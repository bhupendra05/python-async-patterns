# python-async-patterns

> asyncio cookbook — concurrent HTTP, queues, retry with backoff, circuit breaker, background tasks, TaskGroup, benchmarks.

![Python](https://img.shields.io/badge/python-3.11+-blue) ![asyncio](https://img.shields.io/badge/asyncio-built--in-lightgrey) ![License](https://img.shields.io/badge/license-MIT-green)

```
Benchmark: 20 tasks, each with 0.1s I/O delay
Expected sequential time: ~2.0s
Expected concurrent time: ~0.1s

  Sequential (sync):        2.013s  (20 results)
  Threaded:                 0.112s  (20 results)
  Async (gather):           0.101s  (20 results)

  Speedup over sync:
    Threaded: 18.0x faster
    Async:    19.9x faster
```

## Examples

| File | Patterns |
|------|----------|
| `01_concurrent_http.py` | `gather()`, semaphore rate limiting, `as_completed()` streaming |
| `02_queues.py` | Producer/consumer, worker pool, priority queue, bounded backpressure |
| `03_retry_backoff.py` | Exponential backoff + jitter decorator, circuit breaker |
| `04_async_context_managers.py` | `@asynccontextmanager`, `__aenter__`/`__aexit__`, connection pool |
| `05_background_tasks.py` | Fire-and-forget, periodic tasks, task registry, `TaskGroup` |
| `benchmarks/sync_vs_async.py` | Sequential vs threaded vs async comparison |

## Quick Start

```bash
git clone https://github.com/bhupendra05/python-async-patterns.git
cd python-async-patterns
pip install -r requirements.txt

# Run any example
python examples/01_concurrent_http.py
python examples/02_queues.py
python examples/03_retry_backoff.py
python examples/04_async_context_managers.py
python examples/05_background_tasks.py

# See the speedup
python benchmarks/sync_vs_async.py
```

## Key Patterns

### Retry with exponential backoff + jitter

```python
@retry(max_attempts=4, base_delay=1.0, jitter=True)
async def call_api() -> dict:
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.json()
```

### Semaphore rate limiting

```python
semaphore = asyncio.Semaphore(10)  # max 10 concurrent

async def fetch_limited(url):
    async with semaphore:
        return await fetch(session, url)

results = await asyncio.gather(*[fetch_limited(u) for u in urls])
```

### Circuit breaker

```python
breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30)
try:
    result = await breaker.call(unreliable_service)
except CircuitBreakerOpen:
    return cached_response  # fast fallback
```

### Safe fire-and-forget

```python
# Bad: silently drops exceptions
asyncio.create_task(send_email(user))

# Good: logs errors from background tasks
fire_and_forget(send_email(user), name="welcome-email")
```

### TaskGroup (Python 3.11+)

```python
async with asyncio.TaskGroup() as tg:
    t1 = tg.create_task(fetch_user(user_id))
    t2 = tg.create_task(fetch_orders(user_id))
    t3 = tg.create_task(fetch_stats(user_id))
# All done here — one failure cancels siblings
```

## License

MIT © bhupendra05
