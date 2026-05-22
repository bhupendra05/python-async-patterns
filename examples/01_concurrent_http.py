"""
01_concurrent_http.py — Concurrent HTTP requests with aiohttp

Patterns:
  - asyncio.gather() for fan-out
  - Semaphore for rate limiting
  - Session reuse (single TCPConnector)
  - Timeout per request
"""
import asyncio
import time
from typing import Any

import aiohttp

URLS = [
    "https://httpbin.org/get?id=1",
    "https://httpbin.org/get?id=2",
    "https://httpbin.org/get?id=3",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/status/404",
    "https://httpbin.org/status/500",
]


# ── Pattern 1: Simple gather ───────────────────────────────────────────────────

async def fetch(session: aiohttp.ClientSession, url: str) -> dict[str, Any]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            return {"url": url, "status": resp.status, "ok": resp.ok}
    except asyncio.TimeoutError:
        return {"url": url, "status": None, "error": "timeout"}
    except aiohttp.ClientError as e:
        return {"url": url, "status": None, "error": str(e)}


async def fetch_all_simple(urls: list[str]) -> list[dict]:
    """Fetch all URLs concurrently — no rate limiting."""
    connector = aiohttp.TCPConnector(limit=100)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch(session, url) for url in urls]
        return await asyncio.gather(*tasks)


# ── Pattern 2: Rate-limited with semaphore ────────────────────────────────────

async def fetch_limited(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
) -> dict[str, Any]:
    async with semaphore:  # at most N concurrent requests
        return await fetch(session, url)


async def fetch_all_limited(urls: list[str], max_concurrent: int = 3) -> list[dict]:
    """Fetch all URLs with max concurrency limit."""
    semaphore = asyncio.Semaphore(max_concurrent)
    connector = aiohttp.TCPConnector(limit=max_concurrent)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_limited(session, url, semaphore) for url in urls]
        return await asyncio.gather(*tasks)


# ── Pattern 3: Streaming results as they complete ─────────────────────────────

async def fetch_as_completed(urls: list[str]) -> None:
    """Process results as each request finishes (not waiting for all)."""
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = {asyncio.create_task(fetch(session, url)): url for url in urls}
        for coro in asyncio.as_completed(tasks.keys()):
            result = await coro
            print(f"  → {result['url'][:40]:<40} status={result['status']}")


async def main() -> None:
    print("=== Pattern 1: Simple gather ===")
    t = time.perf_counter()
    results = await fetch_all_simple(URLS)
    elapsed = time.perf_counter() - t
    for r in results:
        print(f"  {r['status']} {r['url'][:50]}")
    print(f"  Total: {elapsed:.2f}s for {len(URLS)} requests\n")

    print("=== Pattern 2: Rate-limited (max 2 concurrent) ===")
    t = time.perf_counter()
    results = await fetch_all_limited(URLS, max_concurrent=2)
    elapsed = time.perf_counter() - t
    for r in results:
        print(f"  {r['status']} {r['url'][:50]}")
    print(f"  Total: {elapsed:.2f}s\n")

    print("=== Pattern 3: As-completed streaming ===")
    await fetch_as_completed(URLS)


if __name__ == "__main__":
    asyncio.run(main())
