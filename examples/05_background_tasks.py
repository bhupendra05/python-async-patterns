"""
05_background_tasks.py — Background task patterns

Patterns:
  - Fire-and-forget with error handling
  - Periodic background tasks
  - Task registry / lifecycle management
  - Graceful shutdown
  - asyncio.TaskGroup (Python 3.11+)
"""
import asyncio
import logging
import signal
import time
from typing import Callable, Coroutine, Any

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


# ── Pattern 1: Safe fire-and-forget ──────────────────────────────────────────

def fire_and_forget(coro: Coroutine, *, name: str | None = None) -> asyncio.Task:
    """Schedule a coroutine without awaiting it.
    Logs exceptions instead of silently swallowing them.
    """
    task = asyncio.create_task(coro, name=name)

    def _on_done(t: asyncio.Task) -> None:
        if not t.cancelled() and t.exception() is not None:
            logger.error(f"Background task {t.get_name()!r} failed: {t.exception()}")

    task.add_done_callback(_on_done)
    return task


async def send_email(to: str, subject: str) -> None:
    await asyncio.sleep(0.5)  # simulate email send
    logger.info(f"  [email] Sent to {to}: {subject}")


async def update_cache(key: str) -> None:
    await asyncio.sleep(0.3)
    logger.info(f"  [cache] Updated key: {key}")


# ── Pattern 2: Periodic task ──────────────────────────────────────────────────

class PeriodicTask:
    """Run a coroutine repeatedly at a fixed interval."""

    def __init__(
        self,
        coro_func: Callable[[], Coroutine],
        interval: float,
        name: str = "periodic",
    ) -> None:
        self.coro_func = coro_func
        self.interval = interval
        self.name = name
        self._task: asyncio.Task | None = None
        self._running = False

    def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run(), name=self.name)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        while self._running:
            try:
                await self.coro_func()
            except Exception as exc:
                logger.error(f"  [{self.name}] error: {exc}")
            await asyncio.sleep(self.interval)


# ── Pattern 3: Task registry ─────────────────────────────────────────────────

class TaskRegistry:
    """Manage a set of background tasks with graceful shutdown."""

    def __init__(self) -> None:
        self._tasks: set[asyncio.Task] = set()

    def spawn(self, coro: Coroutine, name: str | None = None) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def shutdown(self, timeout: float = 5.0) -> None:
        if not self._tasks:
            return
        logger.info(f"  [registry] Cancelling {len(self._tasks)} task(s)...")
        for task in list(self._tasks):
            task.cancel()
        done, pending = await asyncio.wait(self._tasks, timeout=timeout)
        if pending:
            logger.warning(f"  [registry] {len(pending)} task(s) timed out")
        logger.info("  [registry] Shutdown complete")

    @property
    def active_count(self) -> int:
        return len(self._tasks)


# ── Pattern 4: TaskGroup (Python 3.11+) ──────────────────────────────────────

async def demo_task_group() -> None:
    """TaskGroup cancels all sibling tasks if one fails."""
    print("=== TaskGroup (structured concurrency) ===")

    async def task_a() -> str:
        await asyncio.sleep(0.1)
        return "task_a done"

    async def task_b() -> str:
        await asyncio.sleep(0.15)
        return "task_b done"

    async def task_c() -> str:
        await asyncio.sleep(0.05)
        return "task_c done"

    try:
        async with asyncio.TaskGroup() as tg:
            t_a = tg.create_task(task_a())
            t_b = tg.create_task(task_b())
            t_c = tg.create_task(task_c())

        # All tasks completed — results available
        for t in [t_a, t_b, t_c]:
            print(f"  {t.result()}")
    except* Exception as eg:
        for exc in eg.exceptions:
            print(f"  Task failed: {exc}")
    print()


# ── Demo ──────────────────────────────────────────────────────────────────────

HEARTBEAT_COUNT = 0

async def heartbeat() -> None:
    global HEARTBEAT_COUNT
    HEARTBEAT_COUNT += 1
    logger.info(f"  [heartbeat] ping #{HEARTBEAT_COUNT}")


async def main() -> None:
    print("=== Fire-and-forget ===")
    fire_and_forget(send_email("user@example.com", "Welcome!"), name="welcome-email")
    fire_and_forget(update_cache("user:123"), name="cache-update")
    await asyncio.sleep(0.6)
    print()

    print("=== Periodic task (runs 3 times) ===")
    pt = PeriodicTask(heartbeat, interval=0.1, name="heartbeat")
    pt.start()
    await asyncio.sleep(0.35)
    await pt.stop()
    print()

    print("=== Task registry with shutdown ===")
    registry = TaskRegistry()

    async def long_worker(n: int) -> None:
        logger.info(f"  [worker-{n}] starting")
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            logger.info(f"  [worker-{n}] cancelled gracefully")
            raise

    for i in range(3):
        registry.spawn(long_worker(i), name=f"worker-{i}")

    await asyncio.sleep(0.1)
    logger.info(f"  Active tasks: {registry.active_count}")
    await registry.shutdown(timeout=1.0)
    print()

    await demo_task_group()


if __name__ == "__main__":
    asyncio.run(main())
