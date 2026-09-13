"""
Enterprise Integration Platform — Main Facade
===============================================
Top-level API that ties connectors, orchestration, pipelines,
and the event bus into a single manageable platform.

MIT License. Copyright 2026 Prem Kumar.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from enterprise_integration.bus import EventBus, MiddlewareManager
from enterprise_integration.connectors import (
    BaseConnector,
    ConnectorRegistry,
    HTTPConnector,
    WebSocketConnector,
    DatabaseConnector,
    MessageQueueConnector,
)
from enterprise_integration.orchestration import APIOrchestrator, APICall, APIResult
from enterprise_integration.pipeline import (
    DataPipeline,
    Event,
    PipelineContext,
    PipelineStage,
    IngestProcessor,
    TransformProcessor,
    ValidateProcessor,
    EnrichProcessor,
    RouteProcessor,
    DeliverProcessor,
    ArchiveProcessor,
    build_default_pipeline,
    build_minimal_pipeline,
)

logger = logging.getLogger(__name__)


class EnterpriseIntegrationPlatform:
    """
    Top-level facade for the Enterprise Integration Platform.

    Manages connectors, orchestrates API calls, runs data pipelines,
    and provides a unified event bus with middleware support.
    """

    def __init__(self, name: str = "EIP") -> None:
        self.name = name
        self.connectors = ConnectorRegistry()
        self.orchestrator = APIOrchestrator()
        self.event_bus = EventBus()
        self.middleware = MiddlewareManager()
        self.pipelines: dict[str, DataPipeline] = {}
        self._started_at = datetime.now(timezone.utc).isoformat()

    def register_pipeline(self, pipeline: DataPipeline) -> None:
        """Register a data pipeline under its name."""
        self.pipelines[pipeline.name] = pipeline

    async def ingest(self, event: Event) -> PipelineContext | None:
        """
        Ingest an event: run through middleware, dispatch to pipeline,
        publish on bus.
        """
        event = self.middleware.process_in(event)
        if event is None:
            logger.info("Event rejected by middleware: %s", event)
            return None

        pipeline_name = event.topic.split(".")[0]
        pipeline = self.pipelines.get(pipeline_name)
        if pipeline is None:
            logger.warning("No pipeline registered for topic '%s'.", event.topic)
            return None

        ctx = await pipeline.process(event)
        self.middleware.process_out(event, ctx)
        await self.event_bus.publish(event)
        return ctx

    async def health(self) -> dict[str, Any]:
        """Return comprehensive health status of the entire platform."""
        connector_health = {}
        for c_info in self.connectors.list_connectors():
            name = c_info.name
            try:
                conn = self.connectors.get(name)
                connector_health[name] = await conn.health_check()
            except Exception:
                connector_health[name] = False

        return {
            "platform": self.name,
            "started_at": self._started_at,
            "connectors": connector_health,
            "connector_count": self.connectors.count,
            "pipeline_count": len(self.pipelines),
            "event_bus_topics": self.event_bus.topic_count,
            "event_bus_subscribers": self.event_bus.subscriber_count,
            "pipelines": {name: p.metrics for name, p in self.pipelines.items()},
            "orchestrator": {
                "calls_logged": len(self.orchestrator.call_log),
                "circuit_breakers": self.orchestrator.circuit_breaker_states,
            },
        }

    async def shutdown(self) -> None:
        """Gracefully shut down all connectors and pipelines."""
        await self.connectors.disconnect_all()
        logger.info("EnterpriseIntegrationPlatform '%s' shut down.", self.name)

    def __repr__(self) -> str:
        return f"<EnterpriseIntegrationPlatform name={self.name!r} connectors={self.connectors.count}>"


# Re-export key symbols
__all__ = [
    "EnterpriseIntegrationPlatform",
    "Event",
    "APICall",
    "APIResult",
    "PipelineContext",
    "PipelineStage",
    "DataPipeline",
    "BaseConnector",
    "HTTPConnector",
    "WebSocketConnector",
    "DatabaseConnector",
    "MessageQueueConnector",
    "ConnectorRegistry",
    "APIOrchestrator",
    "EventBus",
    "MiddlewareManager",
    "IngestProcessor",
    "TransformProcessor",
    "ValidateProcessor",
    "EnrichProcessor",
    "RouteProcessor",
    "DeliverProcessor",
    "ArchiveProcessor",
    "build_default_pipeline",
    "build_minimal_pipeline",
]
