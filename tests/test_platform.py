"""
Tests for Enterprise Integration Platform — Platform Facade
=============================================================
"""

import asyncio
import pytest

from enterprise_integration.platform import (
    EnterpriseIntegrationPlatform,
)
from enterprise_integration.pipeline import (
    Event,
    build_default_pipeline,
    build_minimal_pipeline,
)
from enterprise_integration.connectors import (
    HTTPConnector,
    DatabaseConnector,
    MessageQueueConnector,
)


class TestEnterpriseIntegrationPlatform:
    def test_init(self):
        p = EnterpriseIntegrationPlatform("test")
        assert p.name == "test"
        assert p.connectors.count == 0
        assert p.pipelines == {}

    def test_register_pipeline(self):
        p = EnterpriseIntegrationPlatform()
        pipe = build_default_pipeline("orders")
        p.register_pipeline(pipe)
        assert "orders" in p.pipelines

    @pytest.mark.asyncio
    async def test_ingest(self):
        p = EnterpriseIntegrationPlatform()
        p.register_pipeline(build_default_pipeline("orders"))
        event = Event(topic="orders.new", payload={"id": "123"}, source="web")
        ctx = await p.ingest(event)
        assert ctx is not None
        assert ctx.success is True

    @pytest.mark.asyncio
    async def test_ingest_no_pipeline(self):
        p = EnterpriseIntegrationPlatform()
        event = Event(topic="unknown.topic", payload={"id": "1"}, source="s")
        ctx = await p.ingest(event)
        assert ctx is None

    @pytest.mark.asyncio
    async def test_ingest_rejected_by_middleware(self):
        p = EnterpriseIntegrationPlatform()
        p.register_pipeline(build_default_pipeline("orders"))
        p.middleware.before(lambda e: None)
        event = Event(topic="orders.new", payload={"id": "1"}, source="s")
        ctx = await p.ingest(event)
        assert ctx is None

    @pytest.mark.asyncio
    async def test_health(self):
        p = EnterpriseIntegrationPlatform()
        p.register_pipeline(build_default_pipeline("orders"))
        p.connectors.register(HTTPConnector("api"))
        health = await p.health()
        assert health["platform"] == "EIP"
        assert health["pipeline_count"] == 1
        assert health["connector_count"] == 1
        assert "api" in health["connectors"]

    @pytest.mark.asyncio
    async def test_shutdown(self):
        p = EnterpriseIntegrationPlatform()
        p.connectors.register(HTTPConnector("api"))
        await p.connectors.connect_all()
        await p.shutdown()
        assert p.connectors.get("api").status.value == "disconnected"

    def test_repr(self):
        p = EnterpriseIntegrationPlatform("test")
        assert "test" in repr(p)

    @pytest.mark.asyncio
    async def test_full_flow(self):
        """End-to-end: connectors, pipeline, event bus."""
        p = EnterpriseIntegrationPlatform("full-test")

        # Register connectors
        p.connectors.register(HTTPConnector("crm"))
        p.connectors.register(DatabaseConnector("analytics"))
        p.connectors.register(MessageQueueConnector("events"))
        await p.connectors.connect_all()

        # Register pipeline
        p.register_pipeline(build_default_pipeline("orders"))

        # Subscribe to events
        received = []
        async def on_order(event):
            received.append(event)
        p.event_bus.subscribe("orders.new", on_order)

        # Ingest event
        event = Event(topic="orders.new", payload={"id": "ord-1", "total": 99.99}, source="web")
        ctx = await p.ingest(event)

        assert ctx is not None
        assert ctx.success is True
        assert len(received) == 1

        # Check health
        health = await p.health()
        assert health["connector_count"] == 3
        assert health["pipeline_count"] == 1

        await p.shutdown()

    @pytest.mark.asyncio
    async def test_multiple_pipelines(self):
        p = EnterpriseIntegrationPlatform()
        p.register_pipeline(build_default_pipeline("orders"))
        p.register_pipeline(build_minimal_pipeline("logs"))

        e1 = Event(topic="orders.new", payload={"id": "1"}, source="s")
        e2 = Event(topic="logs.entry", payload={"id": "2"}, source="s")

        ctx1 = await p.ingest(e1)
        ctx2 = await p.ingest(e2)

        assert ctx1 is not None
        assert ctx2 is not None
        assert ctx1.success is True
        assert ctx2.success is True

    @pytest.mark.asyncio
    async def test_pipeline_routing(self):
        """Events are routed to pipelines by topic prefix."""
        p = EnterpriseIntegrationPlatform()
        p.register_pipeline(build_default_pipeline("orders"))

        event = Event(topic="orders.subtopic.action", payload={"id": "1"}, source="s")
        ctx = await p.ingest(event)
        assert ctx is not None
        assert ctx.success is True
