"""
sync_vs_async.py — Benchmark sync vs async for I/O-bound work

Shows the speedup from concurrency for I/O-bound tasks.
"""
import asyncio
import time
import threading
from concurrent.futures import ThreadPoolExecutor


# Simulate I/O delay
IO_DELAY = 0.1
NUM_TASKS = 20


# ── Synchronous (sequential) ─────────────────────────────────────────────────

def sync_task(n: int) -> int:
    time.sleep(IO_DELAY)  # blocking I/O
    return n * 2


def run_sync() -> list[int]:
    return [sync_task(i) for i in range(NUM_TASKS)]


# ── Threaded ──────────────────────────────────────────────────────────────────

def run_threaded() -> list[int]:
    with ThreadPoolExecutor(max_workers=NUM_TASKS) as executor:
        return list(executor.map(sync_task, range(NUM_TASKS)))


# ── Async (concurrent, non-blocking) ─────────────────────────────────────────

async def async_task(n: int) -> int:
    await asyncio.sleep(IO_DELAY)  # non-blocking I/O
    return n * 2


async def run_async() -> list[int]:
    return await asyncio.gather(*[async_task(i) for i in range(NUM_TASKS)])


# ── Benchmark runner ──────────────────────────────────────────────────────────

def benchmark(label: str, func, *args) -> float:
    start = time.perf_counter()
    if asyncio.iscoroutinefunction(func):
        result = asyncio.run(func(*args))
    else:
        result = func(*args)
    elapsed = time.perf_counter() - start
    print(f"  {label:<25} {elapsed:.3f}s  ({len(result)} results)")
    return elapsed


def main() -> None:
    print(f"Benchmark: {NUM_TASKS} tasks, each with {IO_DELAY}s I/O delay")
    print(f"Expected sequential time: ~{NUM_TASKS * IO_DELAY:.1f}s")
    print(f"Expected concurrent time: ~{IO_DELAY:.1f}s\n")

    t_sync     = benchmark("Sequential (sync):", run_sync)
    t_threaded = benchmark("Threaded:          ", run_threaded)
    t_async    = benchmark("Async (gather):    ", run_async)

    print(f"\n  Speedup over sync:")
    print(f"    Threaded: {t_sync/t_threaded:.1f}x faster")
    print(f"    Async:    {t_sync/t_async:.1f}x faster")


if __name__ == "__main__":
    main()
