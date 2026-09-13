"""
Integration tests for Enterprise Integration Platform
========================================================
Tests for cross-module workflows.
"""

import asyncio
import pytest

from enterprise_integration.platform import EnterpriseIntegrationPlatform
from enterprise_integration.pipeline import (
    Event,
    MessagePriority,
    build_default_pipeline,
    build_minimal_pipeline,
    DataPipeline,
    IngestProcessor,
    ArchiveProcessor,
)
from enterprise_integration.connectors import HTTPConnector, DatabaseConnector, MessageQueueConnector


class TestIntegration:
    """Cross-module integration tests."""

    @pytest.mark.asyncio
    async def test_event_to_pipeline_to_bus(self):
        """Event flows through pipeline and onto event bus."""
        p = EnterpriseIntegrationPlatform("integ-1")
        p.register_pipeline(build_default_pipeline("events"))

        bus_events = []
        async def handler(e):
            bus_events.append(e)

        p.event_bus.subscribe("*", handler)

        event = Event(topic="events.user.signup", payload={"id": "u1", "email": "a@b.com"}, source="api")
        ctx = await p.ingest(event)

        assert ctx.success is True
        assert len(bus_events) == 1

    @pytest.mark.asyncio
    async def test_connector_health_reflected_in_platform(self):
        """Platform health includes connector status."""
        p = EnterpriseIntegrationPlatform("integ-2")
        p.connectors.register(HTTPConnector("crm"))
        health = await p.health()
        assert "crm" in health["connectors"]
        assert health["connectors"]["crm"] is False  # Not connected yet

    @pytest.mark.asyncio
    async def test_pipeline_with_connector_write(self):
        """Pipeline writes results to database connector."""
        p = EnterpriseIntegrationPlatform("integ-3")
        db = WarehouseConnector("warehouse")
        await db.connect()
        p.connectors.register(db)

        # Custom pipeline that writes to DB
        pipe = DataPipeline("events")
        pipe.add_processor(IngestProcessor())

        async def archive_to_db(ctx):
            await db.send({
                "table": "events_archive",
                "record": {
                    "event_id": ctx.event.event_id,
                    "topic": ctx.event.topic,
                }
            })
            ctx.stage_results["archive"] = {"saved": True}
            return ctx

        from enterprise_integration.pipeline import PipelineProcessor, PipelineStage
        class DBArchiveProcessor(PipelineProcessor):
            @property
            def stage(self):
                return PipelineStage("custom")
            async def process(self, ctx):
                return await archive_to_db(ctx)

        pipe.add_processor(DBArchiveProcessor())

        event = Event(topic="events.order", payload={"id": "1"}, source="web")
        ctx = await pipe.process(event)
        assert ctx.stage_results.get("archive", {}).get("saved") is True

        rows = await db.query("events_archive")
        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_concurrent_ingestion(self):
        """Multiple events ingested concurrently."""
        p = EnterpriseIntegrationPlatform("integ-4")
        p.register_pipeline(build_default_pipeline("orders"))

        events = [Event(topic="orders.new", payload={"id": str(i)}, source="web") for i in range(20)]
        results = await asyncio.gather(*(p.ingest(e) for e in events))

        assert all(r is not None and r.success for r in results)
        assert p.pipelines["orders"].metrics["processed"] == 20

    @pytest.mark.asyncio
    async def test_event_priority_routing(self):
        """High-priority events go through pipeline normally."""
        p = EnterpriseIntegrationPlatform("integ-5")
        p.register_pipeline(build_default_pipeline("alerts"))

        event = Event(
            topic="alerts.critical",
            payload={"id": "alert-1"},
            source="monitor",
            priority=MessagePriority.CRITICAL,
        )
        ctx = await p.ingest(event)
        assert ctx is not None
        assert ctx.success is True


class WarehouseConnector(DatabaseConnector):
    """Extended database connector for warehouse workloads."""
    pass
