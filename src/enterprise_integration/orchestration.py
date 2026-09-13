"""
Enterprise Integration Platform — API Orchestration Engine
===========================================================
Multi-step API composition, retry logic, circuit breaking,
and middleware processing.

MIT License. Copyright 2026 Prem Kumar.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)


@dataclass
class APICall:
    """Represents a single API invocation."""

    endpoint: str
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    body: dict[str, Any] | None = None
    timeout_ms: int = 30_000
    retry_count: int = 0
    retries_max: int = 3
    call_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class APIResult:
    """Result of an API call."""

    success: bool
    status_code: int = 0
    body: Any = None
    error: str | None = None
    duration_ms: float = 0.0
    attempt: int = 1
    call_id: str = ""


class CircuitBreaker:
    """Prevents cascade failures by opening after N consecutive errors."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half-open"

    def __init__(self, threshold: int = 5, reset_timeout: float = 30.0) -> None:
        self.threshold = threshold
        self.reset_timeout = reset_timeout
        self._failures = 0
        self._last_failure: float | None = None
        self._state = self.CLOSED

    @property
    def is_open(self) -> bool:
        if self._state == self.OPEN:
            if self._last_failure and (time.monotonic() - self._last_failure) > self.reset_timeout:
                self._state = self.HALF_OPEN
                return False
            return True
        return False

    @property
    def state(self) -> str:
        return self._state

    def record_success(self) -> None:
        self._failures = 0
        self._state = self.CLOSED

    def record_failure(self) -> None:
        self._failures += 1
        self._last_failure = time.monotonic()
        if self._failures >= self.threshold:
            self._state = self.OPEN

    def reset(self) -> None:
        self._failures = 0
        self._state = self.CLOSED
        self._last_failure = None


class RateLimiter:
    """Token-bucket rate limiter."""

    def __init__(self, rate: int = 100, burst: int = 10) -> None:
        self.rate = rate
        self.burst = burst
        self._tokens = float(burst)
        self._last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
        self._last_refill = now

    async def acquire(self) -> bool:
        self._refill()
        if self._tokens >= 1:
            self._tokens -= 1
            return True
        return False

    @property
    def available_tokens(self) -> float:
        self._refill()
        return self._tokens


class APIOrchestrator:
    """Orchestrates multi-step API calls with retries, circuit breaking, and composition."""

    def __init__(self) -> None:
        self._circuit_breakers: dict[str, CircuitBreaker] = {}
        self._middleware: list[Callable[[APICall], APICall | None]] = []
        self._rate_limiter = RateLimiter()
        self._call_log: list[dict[str, Any]] = []

    def add_middleware(self, mw: Callable[[APICall], APICall | None]) -> None:
        self._middleware.append(mw)

    def get_circuit_breaker(self, endpoint: str) -> CircuitBreaker:
        if endpoint not in self._circuit_breakers:
            self._circuit_breakers[endpoint] = CircuitBreaker()
        return self._circuit_breakers[endpoint]

    def set_rate_limit(self, rate: int, burst: int) -> None:
        self._rate_limiter = RateLimiter(rate=rate, burst=burst)

    async def execute(self, call: APICall) -> APIResult:
        """Execute a single API call with middleware, retry, and circuit breaker."""
        # Run middleware chain
        processed_call = call
        for mw in self._middleware:
            processed_call = mw(processed_call)
            if processed_call is None:
                return APIResult(success=False, error="Rejected by middleware.", call_id=call.call_id)

        # Check rate limit
        if not await self._rate_limiter.acquire():
            return APIResult(success=False, error="Rate limit exceeded.", call_id=call.call_id)

        cb = self.get_circuit_breaker(call.endpoint)
        if cb.is_open:
            return APIResult(success=False, error="Circuit breaker is open.", call_id=call.call_id)

        last_error: str | None = None
        for attempt in range(1, processed_call.retries_max + 1):
            try:
                start = time.monotonic()
                result = await self._invoke(processed_call)
                result.duration_ms = (time.monotonic() - start) * 1000
                result.attempt = attempt
                result.call_id = call.call_id
                cb.record_success()
                self._log_call(processed_call, result)
                return result
            except Exception as exc:
                last_error = str(exc)
                cb.record_failure()
                if attempt < processed_call.retries_max:
                    backoff = 0.01 * (2 ** (attempt - 1))
                    await asyncio.sleep(backoff)

        result = APIResult(
            success=False,
            error=last_error or "Unknown error",
            attempt=processed_call.retries_max,
            call_id=call.call_id,
        )
        self._log_call(processed_call, result)
        return result

    async def execute_batch(self, calls: list[APICall]) -> list[APIResult]:
        """Execute multiple API calls concurrently."""
        return await asyncio.gather(*(self.execute(c) for c in calls))

    async def execute_sequence(self, calls: list[APICall]) -> list[APIResult]:
        """Execute API calls sequentially, stopping on first failure."""
        results: list[APIResult] = []
        for c in calls:
            r = await self.execute(c)
            results.append(r)
            if not r.success:
                break
        return results

    async def execute_parallel_with_fallback(
        self, calls: list[APICall], fallback: Callable[[], Awaitable[APIResult]] | None = None
    ) -> APIResult:
        """Execute calls in parallel, return first successful result."""
        tasks = [asyncio.create_task(self.execute(c)) for c in calls]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            result = task.result()
            if result.success:
                for t in pending:
                    t.cancel()
                return result
        for t in pending:
            t.cancel()
        if fallback:
            return await fallback()
        return APIResult(success=False, error="All parallel calls failed.")

    def _log_call(self, call: APICall, result: APIResult) -> None:
        self._call_log.append({
            "call_id": call.call_id,
            "endpoint": call.endpoint,
            "method": call.method,
            "success": result.success,
            "duration_ms": result.duration_ms,
            "attempt": result.attempt,
        })

    @property
    def call_log(self) -> list[dict[str, Any]]:
        return list(self._call_log)

    @property
    def circuit_breaker_states(self) -> dict[str, str]:
        return {k: v.state for k, v in self._circuit_breakers.items()}

    async def _invoke(self, call: APICall) -> APIResult:
        """Simulated API invocation — replace with real HTTP client in production."""
        await asyncio.sleep(0.001)

        if "fail" in call.endpoint:
            raise RuntimeError(f"Simulated failure for {call.endpoint}")
        if "timeout" in call.endpoint:
            raise TimeoutError(f"Simulated timeout for {call.endpoint}")

        return APIResult(
            success=True,
            status_code=200,
            body={
                "endpoint": call.endpoint,
                "method": call.method,
                "received_body": call.body,
            },
        )
