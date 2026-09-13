"""
Tests for Enterprise Integration Platform — Connectors
========================================================
"""

import asyncio
import pytest

from enterprise_integration.connectors import (
    BaseConnector,
    ConnectorStatus,
    HTTPConnector,
    WebSocketConnector,
    DatabaseConnector,
    MessageQueueConnector,
    ConnectorRegistry,
)


# --- HTTPConnector ---

class TestHTTPConnector:
    def test_init_defaults(self):
        c = HTTPConnector("test")
        assert c.name == "test"
        assert c.status == ConnectorStatus.DISCONNECTED
        assert c.base_url == ""

    def test_init_config(self):
        c = HTTPConnector("api", {"base_url": "https://api.example.com", "timeout_ms": 5000})
        assert c.base_url == "https://api.example.com"
        assert c.timeout_ms == 5000

    @pytest.mark.asyncio
    async def test_connect_disconnect(self):
        c = HTTPConnector("test")
        await c.connect()
        assert c.status == ConnectorStatus.CONNECTED
        await c.disconnect()
        assert c.status == ConnectorStatus.DISCONNECTED

    @pytest.mark.asyncio
    async def test_health_check(self):
        c = HTTPConnector("test")
        assert await c.health_check() is False
        await c.connect()
        assert await c.health_check() is True

    @pytest.mark.asyncio
    async def test_send(self):
        c = HTTPConnector("test")
        await c.connect()
        result = await c.send({"key": "value"})
        assert result["ok"] is True
        assert result["connector"] == "test"
        assert result["transport"] == "http"

    @pytest.mark.asyncio
    async def test_send_not_connected(self):
        c = HTTPConnector("test")
        with pytest.raises(ConnectionError):
            await c.send({"key": "value"})

    @pytest.mark.asyncio
    async def test_receive_returns_none(self):
        c = HTTPConnector("test")
        await c.connect()
        assert await c.receive() is None

    def test_get_metrics(self):
        c = HTTPConnector("test")
        m = c.get_metrics()
        assert m.name == "test"
        assert m.status == "disconnected"
        assert m.errors == 0
        assert m.successes == 0


# --- WebSocketConnector ---

class TestWebSocketConnector:
    def test_init(self):
        c = WebSocketConnector("ws", {"url": "ws://localhost:9000"})
        assert c.url == "ws://localhost:9000"
        assert c._auto_reconnect is True

    @pytest.mark.asyncio
    async def test_connect(self):
        c = WebSocketConnector("ws")
        await c.connect()
        assert c.status == ConnectorStatus.CONNECTED

    @pytest.mark.asyncio
    async def test_send(self):
        c = WebSocketConnector("ws")
        await c.connect()
        result = await c.send({"msg": "hello"})
        assert result["ok"] is True
        assert result["transport"] == "websocket"

    @pytest.mark.asyncio
    async def test_buffer(self):
        c = WebSocketConnector("ws")
        await c.push_to_buffer({"data": 1})
        assert c.buffer_size == 1
        msg = await c.receive()
        assert msg == {"data": 1}

    @pytest.mark.asyncio
    async def test_receive_timeout(self):
        c = WebSocketConnector("ws")
        result = await c.receive()
        assert result is None


# --- DatabaseConnector ---

class TestDatabaseConnector:
    def test_init(self):
        c = DatabaseConnector("db", {"dialect": "postgres"})
        assert c.dialect == "postgres"

    @pytest.mark.asyncio
    async def test_connect(self):
        c = DatabaseConnector("db")
        await c.connect()
        assert c.status == ConnectorStatus.CONNECTED

    @pytest.mark.asyncio
    async def test_send_insert(self):
        c = DatabaseConnector("db")
        await c.connect()
        result = await c.send({"table": "users", "record": {"name": "Alice"}})
        assert result["ok"] is True
        assert result["table"] == "users"
        assert "id" in result

    @pytest.mark.asyncio
    async def test_query(self):
        c = DatabaseConnector("db")
        await c.connect()
        await c.send({"table": "users", "record": {"name": "Alice"}})
        await c.send({"table": "users", "record": {"name": "Bob"}})
        rows = await c.query("users")
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_query_with_filters(self):
        c = DatabaseConnector("db")
        await c.connect()
        await c.send({"table": "users", "record": {"name": "Alice", "role": "admin"}})
        await c.send({"table": "users", "record": {"name": "Bob", "role": "user"}})
        rows = await c.query("users", {"role": "admin"})
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"

    @pytest.mark.asyncio
    async def test_count(self):
        c = DatabaseConnector("db")
        await c.connect()
        await c.send({"table": "items", "record": {"v": 1}})
        await c.send({"table": "items", "record": {"v": 2}})
        assert await c.count("items") == 2

    def test_table_names(self):
        c = DatabaseConnector("db")
        assert c.table_names == []

    def test_drop_table(self):
        c = DatabaseConnector("db")
        c._store["x"] = []
        c.drop_table("x")
        assert "x" not in c._store


# --- MessageQueueConnector ---

class TestMessageQueueConnector:
    def test_init(self):
        c = MessageQueueConnector("mq", {"max_retries": 5})
        assert c.max_retries == 5

    @pytest.mark.asyncio
    async def test_connect(self):
        c = MessageQueueConnector("mq")
        await c.connect()
        assert c.status == ConnectorStatus.CONNECTED

    @pytest.mark.asyncio
    async def test_send(self):
        c = MessageQueueConnector("mq")
        await c.connect()
        result = await c.send({"queue": "events", "message": {"type": "order"}})
        assert result["ok"] is True
        assert result["queue"] == "events"

    @pytest.mark.asyncio
    async def test_consume(self):
        c = MessageQueueConnector("mq")
        await c.connect()
        await c.send({"queue": "events", "message": {"type": "order"}})
        msg = await c.consume("events")
        assert msg is not None
        assert msg["type"] == "order"

    @pytest.mark.asyncio
    async def test_consume_timeout(self):
        c = MessageQueueConnector("mq")
        await c.connect()
        msg = await c.consume("empty")
        assert msg is None

    @pytest.mark.asyncio
    async def test_retry_moves_to_dlq(self):
        c = MessageQueueConnector("mq", {"max_retries": 2})
        msg = {"_id": "abc", "_retries": 0}
        await c.retry("q", msg)
        assert msg["_retries"] == 1
        await c.retry("q", msg)
        assert len(c.dlq["q"]) == 1

    def test_queue_depth(self):
        c = MessageQueueConnector("mq")
        assert c.queue_depth("any") == 0


# --- ConnectorRegistry ---

class TestConnectorRegistry:
    def test_register_and_get(self):
        reg = ConnectorRegistry()
        c = HTTPConnector("api")
        reg.register(c)
        assert reg.get("api") is c

    def test_register_duplicate(self):
        reg = ConnectorRegistry()
        reg.register(HTTPConnector("api"))
        with pytest.raises(ValueError):
            reg.register(HTTPConnector("api"))

    def test_unregister(self):
        reg = ConnectorRegistry()
        reg.register(HTTPConnector("api"))
        reg.unregister("api")
        assert reg.count == 0

    def test_get_missing(self):
        reg = ConnectorRegistry()
        with pytest.raises(KeyError):
            reg.get("missing")

    def test_list_connectors(self):
        reg = ConnectorRegistry()
        reg.register(HTTPConnector("api"))
        reg.register(DatabaseConnector("db"))
        items = reg.list_connectors()
        assert len(items) == 2

    @pytest.mark.asyncio
    async def test_connect_all(self):
        reg = ConnectorRegistry()
        reg.register(HTTPConnector("api"))
        reg.register(DatabaseConnector("db"))
        results = await reg.connect_all()
        assert results == {"api": True, "db": True}

    @pytest.mark.asyncio
    async def test_disconnect_all(self):
        reg = ConnectorRegistry()
        reg.register(HTTPConnector("api"))
        await reg.connect_all()
        await reg.disconnect_all()
        assert reg.get("api").status == ConnectorStatus.DISCONNECTED

    def test_contains(self):
        reg = ConnectorRegistry()
        reg.register(HTTPConnector("api"))
        assert "api" in reg
        assert "missing" not in reg

    def test_len(self):
        reg = ConnectorRegistry()
        assert len(reg) == 0
        reg.register(HTTPConnector("api"))
        assert len(reg) == 1
