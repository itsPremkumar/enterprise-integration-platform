"""
Enterprise Integration Platform — Event Bus & Middleware
=========================================================
In-memory pub/sub event bus with topic routing and middleware chain.

MIT License. Copyright 2026 Prem Kumar.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)


class EventBus:
    """In-memory pub/sub event bus with topic routing and wildcard support."""

    def __init__(self, max_history: int = 1000) -> None:
        self._subscribers: dict[str, list[Callable]] = defaultdict(list)
        self._history: list[Any] = []
        self._max_history = max_history

    def subscribe(self, topic: str, handler: Callable) -> None:
        """Subscribe a handler to a topic. Use '*' for wildcard."""
        self._subscribers[topic].append(handler)

    def unsubscribe(self, topic: str, handler: Callable) -> None:
        """Remove a handler from a topic."""
        if handler in self._subscribers.get(topic, []):
            self._subscribers[topic].remove(handler)

    async def publish(self, event: Any) -> int:
        """Publish an event to all matching subscribers. Returns count of notified handlers."""
        self._history.append(event)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history :]

        handlers = list(self._subscribers.get(event.topic, []))
        handlers += self._subscribers.get("*", [])

        if handlers:
            await asyncio.gather(
                *(self._safe_call(h, event) for h in handlers),
                return_exceptions=True,
            )
        return len(handlers)

    async def _safe_call(self, handler: Callable, event: Any) -> None:
        try:
            result = handler(event)
            if asyncio.iscoroutine(result):
                await result
        except Exception as exc:
            logger.warning("Handler error for topic '%s': %s", event.topic, exc)

    @property
    def topic_count(self) -> int:
        return len(self._subscribers)

    @property
    def subscriber_count(self) -> int:
        return sum(len(v) for v in self._subscribers.values())

    @property
    def history(self) -> list[Any]:
        return list(self._history)

    def topics(self) -> list[str]:
        return list(self._subscribers.keys())

    def clear(self) -> None:
        self._subscribers.clear()
        self._history.clear()


class MiddlewareManager:
    """Manages middleware chain for event processing."""

    def __init__(self) -> None:
        self._before: list[Callable[[Any], Any | None]] = []
        self._after: list[Callable[[Any, Any], None]] = []

    def before(self, fn: Callable[[Any], Any | None]) -> None:
        """Register a pre-processing middleware. Return None to reject event."""
        self._before.append(fn)

    def after(self, fn: Callable[[Any, Any], None]) -> None:
        """Register a post-processing middleware."""
        self._after.append(fn)

    def process_in(self, event: Any) -> Any | None:
        """Run event through 'before' middleware chain."""
        current = event
        for fn in self._before:
            current = fn(current)
            if current is None:
                return None
        return current

    def process_out(self, event: Any, result: Any) -> None:
        """Run event/result through 'after' middleware chain."""
        for fn in self._after:
            fn(event, result)

    @property
    def before_count(self) -> int:
        return len(self._before)

    @property
    def after_count(self) -> int:
        return len(self._after)
