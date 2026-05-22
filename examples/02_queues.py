"""
02_queues.py — Producer/consumer patterns with asyncio.Queue

Patterns:
  - Single producer, multiple consumers
  - Backpressure via bounded queue
  - Poison pill shutdown pattern
  - Priority queue
  - Work queue with results collection
"""
import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Any


# ── Pattern 1: Basic producer/consumer ───────────────────────────────────────

async def producer(queue: asyncio.Queue, items: list, name: str = "producer") -> None:
    for item in items:
        await queue.put(item)
        print(f"  [{name}] produced: {item}")
        await asyncio.sleep(random.uniform(0.05, 0.15))
    await queue.put(None)  # poison pill to signal completion


async def consumer(queue: asyncio.Queue, name: str) -> list:
    results = []
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break
        # Simulate processing
        await asyncio.sleep(random.uniform(0.05, 0.2))
        result = item * 2
        results.append(result)
        print(f"  [{name}] processed: {item} → {result}")
        queue.task_done()
    return results


async def demo_basic_queue() -> None:
    print("=== Basic Producer/Consumer ===")
    # Bounded queue provides backpressure
    queue: asyncio.Queue = asyncio.Queue(maxsize=3)
    items = list(range(1, 8))

    prod = asyncio.create_task(producer(queue, items))
    cons = asyncio.create_task(consumer(queue, "consumer-1"))

    await asyncio.gather(prod)
    results = await cons
    print(f"  Results: {results}\n")


# ── Pattern 2: Multiple consumers (worker pool) ───────────────────────────────

async def worker(worker_id: int, queue: asyncio.Queue, results: list) -> None:
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            # Re-queue poison pill for other workers
            await queue.put(None)
            break
        await asyncio.sleep(0.1)  # simulate work
        result = f"worker-{worker_id}:{item}"
        results.append(result)
        queue.task_done()


async def demo_worker_pool() -> None:
    print("=== Worker Pool (3 consumers) ===")
    queue: asyncio.Queue = asyncio.Queue(maxsize=5)
    results: list = []
    NUM_WORKERS = 3
    WORK_ITEMS = 10

    # Start workers
    workers = [
        asyncio.create_task(worker(i, queue, results))
        for i in range(NUM_WORKERS)
    ]

    # Feed work
    for i in range(WORK_ITEMS):
        await queue.put(f"task-{i}")

    # Signal shutdown
    await queue.put(None)

    await asyncio.gather(*workers)
    await queue.join()
    print(f"  Processed {len(results)} items with {NUM_WORKERS} workers")
    print(f"  Sample: {results[:5]}\n")


# ── Pattern 3: Priority queue ─────────────────────────────────────────────────

@dataclass(order=True)
class PrioritizedTask:
    priority: int
    data: Any = field(compare=False)


async def demo_priority_queue() -> None:
    print("=== Priority Queue ===")
    pq: asyncio.PriorityQueue = asyncio.PriorityQueue()

    # Add tasks with different priorities (lower number = higher priority)
    tasks = [
        PrioritizedTask(priority=3, data="low-priority"),
        PrioritizedTask(priority=1, data="URGENT"),
        PrioritizedTask(priority=2, data="normal"),
        PrioritizedTask(priority=1, data="ALSO-URGENT"),
        PrioritizedTask(priority=3, data="low-priority-2"),
    ]

    for t in tasks:
        await pq.put(t)

    print("  Processing in priority order:")
    while not pq.empty():
        task = await pq.get()
        print(f"    priority={task.priority} → {task.data}")
    print()


async def main() -> None:
    await demo_basic_queue()
    await demo_worker_pool()
    await demo_priority_queue()


if __name__ == "__main__":
    asyncio.run(main())
