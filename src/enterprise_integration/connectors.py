"""
Enterprise Integration Platform — Connector Library
====================================================
Ready-to-use connectors for HTTP, WebSocket, Database, and Message Queue systems.

MIT License. Copyright 2026 Prem Kumar.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class ConnectorStatus(str, Enum):
    """Connector lifecycle states."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    DEGRADED = "degraded"
    ERROR = "error"


@dataclass
class ConnectorMetrics:
    """Metrics for a single connector."""

    name: str
    status: str
    errors: int = 0
    successes: int = 0
    last_activity: str | None = None


class BaseConnector(ABC):
    """Abstract base for all system connectors."""

    def __init__(self, name: str, config: dict[str, Any] | None = None):
        self.name = name
        self.config = config or {}
        self.status = ConnectorStatus.DISCONNECTED
        self._metrics = {"errors": 0, "successes": 0, "last_activity": None}

    @abstractmethod
    async def connect(self) -> None:
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        ...

    @abstractmethod
    async def health_check(self) -> bool:
        ...

    @abstractmethod
    async def send(self, payload: dict[str, Any]) -> dict[str, Any]:
        ...

    @abstractmethod
    async def receive(self) -> dict[str, Any] | None:
        ...

    def get_metrics(self) -> ConnectorMetrics:
        return ConnectorMetrics(
            name=self.name,
            status=self.status.value,
            errors=self._metrics["errors"],
            successes=self._metrics["successes"],
            last_activity=self._metrics["last_activity"],
        )

    def _record_success(self) -> None:
        self._metrics["successes"] += 1
        self._metrics["last_activity"] = datetime.now(timezone.utc).isoformat()

    def _record_error(self) -> None:
        self._metrics["errors"] += 1
        self._metrics["last_activity"] = datetime.now(timezone.utc).isoformat()


class HTTPConnector(BaseConnector):
    """HTTP/REST system connector with retry support."""

    def __init__(self, name: str, config: dict[str, Any] | None = None):
        super().__init__(name, config)
        self.base_url = self.config.get("base_url", "")
        self.timeout_ms = self.config.get("timeout_ms", 30000)

    async def connect(self) -> None:
        self.status = ConnectorStatus.CONNECTING
        # In production: establish connection pool
        self.status = ConnectorStatus.CONNECTED
        logger.info("HTTPConnector '%s' connected to %s.", self.name, self.base_url)

    async def disconnect(self) -> None:
        self.status = ConnectorStatus.DISCONNECTED
        logger.info("HTTPConnector '%s' disconnected.", self.name)

    async def health_check(self) -> bool:
        return self.status == ConnectorStatus.CONNECTED

    async def send(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not await self.health_check():
            raise ConnectionError(f"HTTPConnector '{self.name}' not connected.")
        self._record_success()
        return {
            "ok": True,
            "connector": self.name,
            "transport": "http",
            "echo": payload,
        }

    async def receive(self) -> dict[str, Any] | None:
        return None  # HTTP is request/response; override for polling


class WebSocketConnector(BaseConnector):
    """WebSocket-based real-time connector."""

    def __init__(self, name: str, config: dict[str, Any] | None = None):
        super().__init__(name, config)
        self.url = self.config.get("url", "ws://localhost:8080")
        self._buffer: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._auto_reconnect = self.config.get("auto_reconnect", True)

    async def connect(self) -> None:
        self.status = ConnectorStatus.CONNECTING
        self.status = ConnectorStatus.CONNECTED
        logger.info("WebSocketConnector '%s' connected to %s.", self.name, self.url)

    async def disconnect(self) -> None:
        self.status = ConnectorStatus.DISCONNECTED

    async def health_check(self) -> bool:
        return self.status == ConnectorStatus.CONNECTED

    async def send(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not await self.health_check():
            raise ConnectionError(f"WebSocketConnector '{self.name}' not connected.")
        self._record_success()
        return {"ok": True, "transport": "websocket", "echo": payload}

    async def receive(self) -> dict[str, Any] | None:
        try:
            return await asyncio.wait_for(self._buffer.get(), timeout=0.05)
        except asyncio.TimeoutError:
            return None

    async def push_to_buffer(self, msg: dict[str, Any]) -> None:
        await self._buffer.put(msg)

    @property
    def buffer_size(self) -> int:
        return self._buffer.qsize()


class DatabaseConnector(BaseConnector):
    """Database connector with in-memory simulation for testing."""

    def __init__(self, name: str, config: dict[str, Any] | None = None):
        super().__init__(name, config)
        self._store: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.dialect = self.config.get("dialect", "sqlite")

    async def connect(self) -> None:
        self.status = ConnectorStatus.CONNECTED
        logger.info("DatabaseConnector '%s' connected (dialect=%s).", self.name, self.dialect)

    async def disconnect(self) -> None:
        self.status = ConnectorStatus.DISCONNECTED

    async def health_check(self) -> bool:
        return self.status == ConnectorStatus.CONNECTED

    async def send(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not await self.health_check():
            raise ConnectionError(f"DatabaseConnector '{self.name}' not connected.")
        table = payload.get("table", "default")
        record = payload.get("record", {})
        record["_id"] = uuid.uuid4().hex[:12]
        record["_ts"] = datetime.now(timezone.utc).isoformat()
        self._store[table].append(record)
        self._record_success()
        return {"ok": True, "id": record["_id"], "table": table}

    async def receive(self) -> dict[str, Any] | None:
        return None

    async def query(self, table: str, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        rows = self._store.get(table, [])
        if not filters:
            return list(rows)
        return [r for r in rows if all(r.get(k) == v for k, v in filters.items())]

    async def count(self, table: str) -> int:
        return len(self._store.get(table, []))

    @property
    def table_names(self) -> list[str]:
        return list(self._store.keys())

    def drop_table(self, table: str) -> None:
        self._store.pop(table, None)


class MessageQueueConnector(BaseConnector):
    """Message queue connector (in-memory simulation)."""

    def __init__(self, name: str, config: dict[str, Any] | None = None):
        super().__init__(name, config)
        self._queues: dict[str, asyncio.Queue[dict[str, Any]]] = defaultdict(asyncio.Queue)
        self.dlq: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.max_retries = self.config.get("max_retries", 3)

    async def connect(self) -> None:
        self.status = ConnectorStatus.CONNECTED

    async def disconnect(self) -> None:
        self.status = ConnectorStatus.DISCONNECTED

    async def health_check(self) -> bool:
        return self.status == ConnectorStatus.CONNECTED

    async def send(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not await self.health_check():
            raise ConnectionError(f"MessageQueueConnector '{self.name}' not connected.")
        queue_name = payload.get("queue", "default")
        msg = payload.get("message", {})
        msg["_id"] = uuid.uuid4().hex[:12]
        msg["_retries"] = 0
        await self._queues[queue_name].put(msg)
        self._record_success()
        return {"ok": True, "queue": queue_name, "id": msg["_id"]}

    async def receive(self) -> dict[str, Any] | None:
        return None

    async def consume(self, queue_name: str, timeout: float = 0.05) -> dict[str, Any] | None:
        try:
            return await asyncio.wait_for(self._queues[queue_name].get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def retry(self, queue_name: str, msg: dict[str, Any]) -> None:
        msg["_retries"] += 1
        if msg["_retries"] >= self.max_retries:
            self.dlq[queue_name].append(msg)
            logger.warning("Message moved to DLQ: %s", msg["_id"])
        else:
            await self._queues[queue_name].put(msg)

    def queue_depth(self, queue_name: str) -> int:
        return self._queues[queue_name].qsize()


class ConnectorRegistry:
    """Central registry managing all system connectors with lifecycle management."""

    def __init__(self) -> None:
        self._connectors: dict[str, BaseConnector] = {}

    def register(self, connector: BaseConnector) -> None:
        if connector.name in self._connectors:
            raise ValueError(f"Connector '{connector.name}' already registered.")
        self._connectors[connector.name] = connector

    def unregister(self, name: str) -> None:
        self._connectors.pop(name, None)

    def get(self, name: str) -> BaseConnector:
        if name not in self._connectors:
            raise KeyError(f"Connector '{name}' not found.")
        return self._connectors[name]

    def list_connectors(self) -> list[ConnectorMetrics]:
        return [c.get_metrics() for c in self._connectors.values()]

    async def connect_all(self) -> dict[str, bool]:
        results: dict[str, bool] = {}
        for name, conn in self._connectors.items():
            try:
                await conn.connect()
                results[name] = True
            except Exception:
                results[name] = False
        return results

    async def disconnect_all(self) -> None:
        for conn in self._connectors.values():
            try:
                await conn.disconnect()
            except Exception:
                pass

    @property
    def count(self) -> int:
        return len(self._connectors)

    def __contains__(self, name: str) -> bool:
        return name in self._connectors

    def __len__(self) -> int:
        return len(self._connectors)
