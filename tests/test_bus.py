"""
Tests for Enterprise Integration Platform — Event Bus & Middleware
===================================================================
"""

import asyncio
import pytest

from enterprise_integration.bus import EventBus, MiddlewareManager


# --- EventBus ---

class TestEventBus:
    def test_init(self):
        bus = EventBus()
        assert bus.topic_count == 0
        assert bus.subscriber_count == 0

    def test_subscribe(self):
        bus = EventBus()
        bus.subscribe("topic.a", lambda e: None)
        assert bus.topic_count == 1
        assert bus.subscriber_count == 1

    def test_unsubscribe(self):
        bus = EventBus()
        handler = lambda e: None
        bus.subscribe("topic.a", handler)
        bus.unsubscribe("topic.a", handler)
        assert bus.subscriber_count == 0

    @pytest.mark.asyncio
    async def test_publish(self):
        bus = EventBus()
        received = []

        async def handler(event):
            received.append(event)

        bus.subscribe("orders.new", handler)
        event = type("Event", (), {"topic": "orders.new"})()
        count = await bus.publish(event)
        assert count == 1
        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_publish_no_subscribers(self):
        bus = EventBus()
        event = type("Event", (), {"topic": "unknown"})()
        count = await bus.publish(event)
        assert count == 0

    @pytest.mark.asyncio
    async def test_wildcard_subscribe(self):
        bus = EventBus()
        received = []

        async def handler(event):
            received.append(event)

        bus.subscribe("*", handler)
        event = type("Event", (), {"topic": "anything"})()
        await bus.publish(event)
        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_history(self):
        bus = EventBus(max_history=10)
        for i in range(5):
            event = type("Event", (), {"topic": f"t.{i}"})()
            await bus.publish(event)
        assert len(bus.history) == 5

    @pytest.mark.asyncio
    async def test_history_max(self):
        bus = EventBus(max_history=3)
        for i in range(5):
            event = type("Event", (), {"topic": f"t.{i}"})()
            await bus.publish(event)
        assert len(bus.history) == 3

    def test_topics(self):
        bus = EventBus()
        bus.subscribe("a", lambda e: None)
        bus.subscribe("b", lambda e: None)
        assert set(bus.topics()) == {"a", "b"}

    def test_clear(self):
        bus = EventBus()
        bus.subscribe("a", lambda e: None)
        bus.clear()
        assert bus.topic_count == 0
        assert len(bus.history) == 0

    @pytest.mark.asyncio
    async def test_handler_exception_doesnt_break(self):
        bus = EventBus()
        results = []

        async def bad_handler(event):
            raise RuntimeError("boom")

        async def good_handler(event):
            results.append("ok")

        bus.subscribe("t", bad_handler)
        bus.subscribe("t", good_handler)
        event = type("Event", (), {"topic": "t"})()
        await bus.publish(event)
        assert "ok" in results


# --- MiddlewareManager ---

class TestMiddlewareManager:
    def test_init(self):
        mm = MiddlewareManager()
        assert mm.before_count == 0
        assert mm.after_count == 0

    def test_before(self):
        mm = MiddlewareManager()
        mm.before(lambda e: e)
        assert mm.before_count == 1

    def test_after(self):
        mm = MiddlewareManager()
        mm.after(lambda e, r: None)
        assert mm.after_count == 1

    def test_process_in_passes(self):
        mm = MiddlewareManager()
        mm.before(lambda e: e)
        event = type("E", (), {"topic": "t"})()
        result = mm.process_in(event)
        assert result is event

    def test_process_in_rejects(self):
        mm = MiddlewareManager()
        mm.before(lambda e: None)
        event = type("E", (), {"topic": "t"})()
        result = mm.process_in(event)
        assert result is None

    def test_process_in_chain(self):
        mm = MiddlewareManager()
        mm.before(lambda e: e)
        mm.before(lambda e: e)
        event = type("E", (), {"topic": "t"})()
        result = mm.process_in(event)
        assert result is event

    def test_process_out(self):
        mm = MiddlewareManager()
        captured = []
        mm.after(lambda e, r: captured.append((e, r)))
        event = "event"
        result = "result"
        mm.process_out(event, result)
        assert captured == [("event", "result")]
