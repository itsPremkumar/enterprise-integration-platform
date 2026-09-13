"""
Tests for Enterprise Integration Platform — Orchestration
===========================================================
"""

import asyncio
import pytest

from enterprise_integration.orchestration import (
    APICall,
    APIResult,
    APIOrchestrator,
    CircuitBreaker,
    RateLimiter,
)


# --- CircuitBreaker ---

class TestCircuitBreaker:
    def test_init(self):
        cb = CircuitBreaker(threshold=3, reset_timeout=10.0)
        assert cb.threshold == 3
        assert cb.reset_timeout == 10.0
        assert cb.is_open is False
        assert cb.state == "closed"

    def test_opens_after_threshold(self):
        cb = CircuitBreaker(threshold=3)
        cb.record_failure()
        cb.record_failure()
        assert cb.is_open is False
        cb.record_failure()
        assert cb.is_open is True
        assert cb.state == "open"

    def test_closes_on_success(self):
        cb = CircuitBreaker(threshold=2)
        cb.record_failure()
        cb.record_failure()
        assert cb.is_open is True
        cb.record_success()
        assert cb.is_open is False
        assert cb.state == "closed"

    def test_reset(self):
        cb = CircuitBreaker(threshold=1)
        cb.record_failure()
        assert cb.is_open is True
        cb.reset()
        assert cb.is_open is False
        assert cb.state == "closed"

    def test_half_open_after_timeout(self):
        cb = CircuitBreaker(threshold=1, reset_timeout=0.0)
        cb.record_failure()
        assert cb.is_open is True
        import time
        time.sleep(0.01)
        assert cb.is_open is False
        assert cb.state == "half-open"


# --- RateLimiter ---

class TestRateLimiter:
    def test_init(self):
        rl = RateLimiter(rate=10, burst=5)
        assert rl.rate == 10
        assert rl.burst == 5
        assert rl.available_tokens == 5

    @pytest.mark.asyncio
    async def test_acquire_within_burst(self):
        rl = RateLimiter(rate=10, burst=5)
        for _ in range(5):
            assert await rl.acquire() is True
        assert await rl.acquire() is False

    @pytest.mark.asyncio
    async def test_tokens_refill(self):
        rl = RateLimiter(rate=100, burst=2)
        await rl.acquire()
        await rl.acquire()
        assert await rl.acquire() is False
        await asyncio.sleep(0.05)
        assert rl.available_tokens > 0


# --- APIOrchestrator ---

class TestAPIOrchestrator:
    def test_init(self):
        orch = APIOrchestrator()
        assert orch.call_log == []
        assert orch.circuit_breaker_states == {}

    @pytest.mark.asyncio
    async def test_execute_success(self):
        orch = APIOrchestrator()
        call = APICall(endpoint="https://api.example.com/data", method="GET")
        result = await orch.execute(call)
        assert result.success is True
        assert result.status_code == 200
        assert result.body["endpoint"] == "https://api.example.com/data"

    @pytest.mark.asyncio
    async def test_execute_failure(self):
        orch = APIOrchestrator()
        call = APICall(endpoint="https://api.example.com/fail", retries_max=1)
        result = await orch.execute(call)
        assert result.success is False
        assert "Simulated failure" in result.error

    @pytest.mark.asyncio
    async def test_execute_timeout(self):
        orch = APIOrchestrator()
        call = APICall(endpoint="https://api.example.com/timeout", retries_max=1)
        result = await orch.execute(call)
        assert result.success is False
        assert "timeout" in result.error.lower() or "Timeout" in result.error

    @pytest.mark.asyncio
    async def test_execute_with_retry(self):
        orch = APIOrchestrator()
        call = APICall(endpoint="https://api.example.com/fail", retries_max=3)
        result = await orch.execute(call)
        assert result.success is False
        assert result.attempt == 3

    @pytest.mark.asyncio
    async def test_execute_batch(self):
        orch = APIOrchestrator()
        calls = [
            APICall(endpoint="https://api.example.com/a"),
            APICall(endpoint="https://api.example.com/b"),
        ]
        results = await orch.execute_batch(calls)
        assert len(results) == 2
        assert all(r.success for r in results)

    @pytest.mark.asyncio
    async def test_execute_sequence_stops_on_failure(self):
        orch = APIOrchestrator()
        calls = [
            APICall(endpoint="https://api.example.com/ok"),
            APICall(endpoint="https://api.example.com/fail", retries_max=1),
            APICall(endpoint="https://api.example.com/never-reached"),
        ]
        results = await orch.execute_sequence(calls)
        assert len(results) == 2
        assert results[0].success is True
        assert results[1].success is False

    @pytest.mark.asyncio
    async def test_middleware_rejects(self):
        orch = APIOrchestrator()
        orch.add_middleware(lambda call: None)
        call = APICall(endpoint="https://api.example.com/data")
        result = await orch.execute(call)
        assert result.success is False
        assert "Rejected by middleware" in result.error

    @pytest.mark.asyncio
    async def test_middleware_modifies(self):
        orch = APIOrchestrator()
        orch.add_middleware(lambda call: APICall(endpoint="https://api.example.com/data"))
        call = APICall(endpoint="https://api.example.com/other")
        result = await orch.execute(call)
        assert result.success is True

    @pytest.mark.asyncio
    async def test_circuit_breaker_opens(self):
        orch = APIOrchestrator()
        cb = orch.get_circuit_breaker("https://api.example.com/fail")
        for _ in range(5):
            cb.record_failure()
        call = APICall(endpoint="https://api.example.com/fail")
        result = await orch.execute(call)
        assert result.success is False
        assert "Circuit breaker is open" in result.error

    @pytest.mark.asyncio
    async def test_call_log(self):
        orch = APIOrchestrator()
        call = APICall(endpoint="https://api.example.com/data")
        await orch.execute(call)
        assert len(orch.call_log) == 1
        assert orch.call_log[0]["endpoint"] == "https://api.example.com/data"

    @pytest.mark.asyncio
    async def test_rate_limit_rejects(self):
        orch = APIOrchestrator()
        orch.set_rate_limit(rate=1, burst=1)
        await orch.execute(APICall(endpoint="https://api.example.com/first"))
        result = await orch.execute(APICall(endpoint="https://api.example.com/second"))
        assert result.success is False
        assert "Rate limit" in result.error

    @pytest.mark.asyncio
    async def test_parallel_with_fallback(self):
        orch = APIOrchestrator()
        calls = [
            APICall(endpoint="https://api.example.com/fail", retries_max=1),
            APICall(endpoint="https://api.example.com/ok"),
        ]
        result = await orch.execute_parallel_with_fallback(calls)
        assert result.success is True

    @pytest.mark.asyncio
    async def test_parallel_all_fail_with_fallback(self):
        orch = APIOrchestrator()
        calls = [
            APICall(endpoint="https://api.example.com/fail", retries_max=1),
        ]
        result = await orch.execute_parallel_with_fallback(calls)
        assert result.success is False
