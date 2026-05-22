"""
04_async_context_managers.py — Async context managers

Patterns:
  - @asynccontextmanager decorator
  - __aenter__ / __aexit__ class-based
  - Resource pooling
  - Timed context manager
  - Async generator as context manager
"""
import asyncio
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator


# ── Pattern 1: @asynccontextmanager ──────────────────────────────────────────

@asynccontextmanager
async def managed_connection(host: str, port: int) -> AsyncIterator[dict]:
    """Simulate a managed DB/HTTP connection."""
    print(f"  [conn] Opening connection to {host}:{port}")
    connection = {"host": host, "port": port, "id": id(host)}
    try:
        await asyncio.sleep(0.01)  # simulate connect
        yield connection
    except Exception as exc:
        print(f"  [conn] Error in connection: {exc}")
        raise
    finally:
        print(f"  [conn] Closing connection to {host}:{port}")
        await asyncio.sleep(0.005)  # simulate close


@asynccontextmanager
async def timer(label: str) -> AsyncIterator[None]:
    """Measure async block execution time."""
    start = time.perf_counter()
    print(f"  [timer:{label}] start")
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        print(f"  [timer:{label}] done — {elapsed*1000:.1f}ms")


# ── Pattern 2: Class-based async context manager ──────────────────────────────

class DatabasePool:
    """Simple async connection pool."""

    def __init__(self, host: str, max_connections: int = 5) -> None:
        self.host = host
        self.max_connections = max_connections
        self._semaphore: asyncio.Semaphore | None = None
        self._connection_count = 0

    async def __aenter__(self) -> "DatabasePool":
        self._semaphore = asyncio.Semaphore(self.max_connections)
        print(f"  [pool] Initialized (max={self.max_connections})")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        print(f"  [pool] Closing (used {self._connection_count} connections total)")
        return False  # don't suppress exceptions

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[dict]:
        """Acquire a connection from the pool."""
        assert self._semaphore is not None, "Pool not initialized"
        async with self._semaphore:
            self._connection_count += 1
            conn_id = self._connection_count
            print(f"  [pool] Acquired connection #{conn_id}")
            try:
                yield {"id": conn_id, "host": self.host}
            finally:
                print(f"  [pool] Released connection #{conn_id}")


# ── Pattern 3: Async generator context manager ────────────────────────────────

@asynccontextmanager
async def rate_limiter(calls_per_second: float) -> AsyncIterator[None]:
    """Limit execution rate of async code."""
    min_interval = 1.0 / calls_per_second
    last_call_time = 0.0

    async def throttle() -> None:
        nonlocal last_call_time
        now = time.monotonic()
        wait = min_interval - (now - last_call_time)
        if wait > 0:
            await asyncio.sleep(wait)
        last_call_time = time.monotonic()

    yield throttle  # type: ignore


# ── Demo ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    print("=== @asynccontextmanager connection ===")
    async with managed_connection("db.local", 5432) as conn:
        print(f"  Using connection: {conn}")
    print()

    print("=== Timer context manager ===")
    async with timer("concurrent fetch"):
        await asyncio.gather(
            asyncio.sleep(0.1),
            asyncio.sleep(0.15),
            asyncio.sleep(0.05),
        )
    print()

    print("=== Class-based connection pool ===")
    async with DatabasePool("postgres://localhost", max_connections=3) as pool:
        async def use_connection(task_id: int) -> None:
            async with pool.acquire() as conn:
                await asyncio.sleep(0.05)  # simulate query
                print(f"  Task {task_id} used connection #{conn['id']}")

        await asyncio.gather(*[use_connection(i) for i in range(5)])
    print()

    print("=== Nested context managers ===")
    async with timer("nested"):
        async with managed_connection("api.service", 443) as conn:
            async with managed_connection("cache.local", 6379) as cache:
                print(f"  DB conn: {conn['id']}, Cache conn: {cache['id']}")
                await asyncio.sleep(0.02)


if __name__ == "__main__":
    asyncio.run(main())
