"""
03_retry_backoff.py — Retry with exponential backoff

Patterns:
  - Exponential backoff with jitter
  - Retry decorator for async functions
  - Circuit breaker pattern
  - Timeout + retry combination
"""
import asyncio
import functools
import logging
import math
import random
import time
from collections.abc import Callable
from typing import Any, Type

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


# ── Pattern 1: Retry decorator ───────────────────────────────────────────────

def retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    exceptions: tuple[Type[Exception], ...] = (Exception,),
) -> Callable:
    """Async retry decorator with exponential backoff + jitter."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception: Exception | None = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as exc:
                    last_exception = exc
                    if attempt == max_attempts:
                        break

                    # Exponential backoff: base_delay * 2^attempt
                    delay = min(base_delay * (exponential_base ** (attempt - 1)), max_delay)

                    # Full jitter: random in [0, delay]
                    if jitter:
                        delay = random.uniform(0, delay)

                    logger.info(
                        f"  [{func.__name__}] attempt {attempt}/{max_attempts} failed: "
                        f"{exc}. Retrying in {delay:.2f}s..."
                    )
                    await asyncio.sleep(delay)

            raise last_exception  # type: ignore

        return wrapper
    return decorator


# ── Pattern 2: Circuit breaker ────────────────────────────────────────────────

class CircuitBreakerOpen(Exception):
    pass


class CircuitBreaker:
    """Prevents cascading failures by stopping calls to failing services.

    States:
      CLOSED   → calls pass through (normal operation)
      OPEN     → calls blocked immediately (service is down)
      HALF-OPEN → one test call allowed (checking recovery)
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        success_threshold: int = 2,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold

        self._state = "CLOSED"
        self._failures = 0
        self._successes = 0
        self._last_failure_time: float | None = None

    @property
    def state(self) -> str:
        if self._state == "OPEN":
            if self._last_failure_time and \
               time.monotonic() - self._last_failure_time > self.recovery_timeout:
                self._state = "HALF-OPEN"
                logger.info("  [circuit] → HALF-OPEN (testing recovery)")
        return self._state

    async def call(self, coro_func: Callable, *args: Any, **kwargs: Any) -> Any:
        state = self.state

        if state == "OPEN":
            raise CircuitBreakerOpen(f"Circuit is OPEN — service unavailable")

        try:
            result = await coro_func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as exc:
            self._on_failure()
            raise

    def _on_success(self) -> None:
        if self._state == "HALF-OPEN":
            self._successes += 1
            if self._successes >= self.success_threshold:
                self._state = "CLOSED"
                self._failures = 0
                self._successes = 0
                logger.info("  [circuit] → CLOSED (service recovered)")
        else:
            self._failures = 0

    def _on_failure(self) -> None:
        self._failures += 1
        self._last_failure_time = time.monotonic()
        if self._failures >= self.failure_threshold:
            self._state = "OPEN"
            logger.info(f"  [circuit] → OPEN after {self._failures} failures")


# ── Demo ──────────────────────────────────────────────────────────────────────

CALL_COUNT = 0

@retry(max_attempts=4, base_delay=0.1, jitter=True)
async def flaky_api_call() -> str:
    global CALL_COUNT
    CALL_COUNT += 1
    if CALL_COUNT < 4:
        raise ConnectionError(f"Service unavailable (attempt {CALL_COUNT})")
    return "✓ API call succeeded"


async def unreliable_service() -> str:
    if random.random() < 0.7:
        raise ConnectionError("Service down")
    return "OK"


async def main() -> None:
    print("=== Retry with exponential backoff ===")
    try:
        result = await flaky_api_call()
        print(f"  Result: {result} (took {CALL_COUNT} attempts)\n")
    except Exception as e:
        print(f"  Failed after retries: {e}\n")

    print("=== Circuit Breaker ===")
    breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=1.0)

    for i in range(8):
        try:
            result = await breaker.call(unreliable_service)
            print(f"  Call {i+1}: OK (state={breaker.state})")
        except CircuitBreakerOpen:
            print(f"  Call {i+1}: BLOCKED (circuit open)")
        except ConnectionError as e:
            print(f"  Call {i+1}: FAILED → {e} (state={breaker.state})")
        await asyncio.sleep(0.1)

    print(f"\n  Final circuit state: {breaker.state}")


if __name__ == "__main__":
    asyncio.run(main())
