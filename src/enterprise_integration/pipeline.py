"""
Enterprise Integration Platform — Data Pipeline Engine
======================================================
Configurable multi-stage data pipelines with ingest, transform,
validate, enrich, route, deliver, and archive stages.

MIT License. Copyright 2026 Prem Kumar.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)


class PipelineStage(str, Enum):
    """Pipeline stage identifiers."""

    INGEST = "ingest"
    TRANSFORM = "transform"
    VALIDATE = "validate"
    ENRICH = "enrich"
    ROUTE = "route"
    DELIVER = "deliver"
    ARCHIVE = "archive"


class MessagePriority(int, Enum):
    """Event priority levels."""

    LOW = 0
    NORMAL = 5
    HIGH = 10
    CRITICAL = 20


@dataclass
class Event:
    """Immutable event flowing through the integration bus."""

    topic: str
    payload: dict[str, Any]
    source: str
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    priority: MessagePriority = MessagePriority.NORMAL
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        import json

        return json.dumps(
            {
                "event_id": self.event_id,
                "topic": self.topic,
                "payload": self.payload,
                "source": self.source,
                "timestamp": self.timestamp,
                "priority": self.priority.value,
                "metadata": self.metadata,
            },
            default=str,
        )

    @classmethod
    def from_json(cls, raw: str) -> "Event":
        import json

        data = json.loads(raw)
        data["priority"] = MessagePriority(data.get("priority", 5))
        return cls(**data)


@dataclass
class PipelineContext:
    """Carries state through pipeline stages."""

    pipeline_id: str
    event: Event
    stage_results: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    start_time: float = field(default_factory=time.monotonic)

    @property
    def duration_ms(self) -> float:
        return (time.monotonic() - self.start_time) * 1000

    @property
    def success(self) -> bool:
        return len(self.errors) == 0


class PipelineProcessor(ABC):
    """A single stage in a data pipeline."""

    @property
    @abstractmethod
    def stage(self) -> PipelineStage:
        ...

    @abstractmethod
    async def process(self, ctx: PipelineContext) -> PipelineContext:
        ...


class IngestProcessor(PipelineProcessor):
    """Ingests raw event and records metadata."""

    @property
    def stage(self) -> PipelineStage:
        return PipelineStage.INGEST

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        ctx.stage_results["ingest"] = {
            "topic": ctx.event.topic,
            "source": ctx.event.source,
            "size_bytes": len(ctx.event.to_json().encode()),
            "priority": ctx.event.priority.value,
        }
        return ctx


class TransformProcessor(PipelineProcessor):
    """Applies configurable transformations to event payload."""

    def __init__(
        self,
        transforms: list[Callable[[dict[str, Any]], dict[str, Any]]] | None = None,
    ):
        self._transforms = transforms or []

    @property
    def stage(self) -> PipelineStage:
        return PipelineStage.TRANSFORM

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        data = dict(ctx.event.payload)
        for fn in self._transforms:
            data = fn(data)
        ctx.stage_results["transform"] = data
        return ctx


class ValidateProcessor(PipelineProcessor):
    """Validates payload against a simple schema."""

    def __init__(self, required_fields: list[str] | None = None) -> None:
        self._required = required_fields or []

    @property
    def stage(self) -> PipelineStage:
        return PipelineStage.VALIDATE

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        data = ctx.stage_results.get("transform", ctx.event.payload)
        missing = [f for f in self._required if f not in data]
        ctx.stage_results["validate"] = {
            "valid": not missing,
            "missing_fields": missing,
        }
        if missing:
            ctx.errors.append(f"Missing required fields: {missing}")
        return ctx


class EnrichProcessor(PipelineProcessor):
    """Enriches payload with additional metadata."""

    def __init__(
        self,
        enricher: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    ) -> None:
        self._enricher = enricher or (lambda d: {**d, "enriched": True})

    @property
    def stage(self) -> PipelineStage:
        return PipelineStage.ENRICH

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        data = ctx.stage_results.get("transform", ctx.event.payload)
        enriched = self._enricher(data)
        ctx.stage_results["enrich"] = enriched
        return ctx


class RouteProcessor(PipelineProcessor):
    """Routes event to one or more destinations."""

    def __init__(
        self,
        router: Callable[[dict[str, Any]], list[str]] | None = None,
    ) -> None:
        self._router = router or (lambda d: ["default-sink"])

    @property
    def stage(self) -> PipelineStage:
        return PipelineStage.ROUTE

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        data = ctx.stage_results.get(
            "enrich", ctx.stage_results.get("transform", ctx.event.payload)
        )
        destinations = self._router(data)
        ctx.stage_results["route"] = {"destinations": destinations}
        return ctx


class DeliverProcessor(PipelineProcessor):
    """Simulates delivery to destinations."""

    def __init__(self) -> None:
        self._deliveries: list[dict[str, Any]] = []

    @property
    def stage(self) -> PipelineStage:
        return PipelineStage.DELIVER

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        route_result = ctx.stage_results.get("route", {})
        destinations = route_result.get("destinations", [])
        delivery_record = {
            "pipeline_id": ctx.pipeline_id,
            "event_id": ctx.event.event_id,
            "destinations": destinations,
            "count": len(destinations),
        }
        self._deliveries.append(delivery_record)
        ctx.stage_results["deliver"] = delivery_record
        return ctx

    @property
    def deliveries(self) -> list[dict[str, Any]]:
        return list(self._deliveries)


class ArchiveProcessor(PipelineProcessor):
    """Archives processed event to storage."""

    def __init__(self) -> None:
        self._archive: list[dict[str, Any]] = []

    @property
    def stage(self) -> PipelineStage:
        return PipelineStage.ARCHIVE

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        record = {
            "pipeline_id": ctx.pipeline_id,
            "event_id": ctx.event.event_id,
            "topic": ctx.event.topic,
            "duration_ms": round(ctx.duration_ms, 2),
            "errors": list(ctx.errors),
            "stages": {k: v for k, v in ctx.stage_results.items()},
        }
        self._archive.append(record)
        ctx.stage_results["archive"] = {"record_id": len(self._archive)}
        return ctx

    @property
    def archived(self) -> list[dict[str, Any]]:
        return list(self._archive)


class DataPipeline:
    """Chains processors to form a complete data pipeline."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._processors: list[PipelineProcessor] = []
        self._metrics: dict[str, int] = {"processed": 0, "errors": 0}

    def add_processor(self, proc: PipelineProcessor) -> "DataPipeline":
        self._processors.append(proc)
        return self

    async def process(self, event: Event) -> PipelineContext:
        ctx = PipelineContext(
            pipeline_id=f"{self.name}-{uuid.uuid4().hex[:8]}", event=event
        )
        for proc in self._processors:
            try:
                ctx = await proc.process(ctx)
            except Exception as exc:
                ctx.errors.append(f"Stage {proc.stage.value} failed: {exc}")
                break
        if ctx.success:
            self._metrics["processed"] += 1
        else:
            self._metrics["errors"] += 1
        return ctx

    async def process_batch(self, events: list[Event]) -> list[PipelineContext]:
        return await asyncio.gather(*(self.process(e) for e in events))

    @property
    def metrics(self) -> dict[str, int]:
        return dict(self._metrics)

    @property
    def stages(self) -> list[PipelineStage]:
        return [p.stage for p in self._processors]

    @property
    def stage_count(self) -> int:
        return len(self._processors)


def build_default_pipeline(name: str) -> DataPipeline:
    """Construct a standard 7-stage pipeline with sensible defaults."""
    return (
        DataPipeline(name)
        .add_processor(IngestProcessor())
        .add_processor(TransformProcessor())
        .add_processor(ValidateProcessor(required_fields=["id"]))
        .add_processor(EnrichProcessor())
        .add_processor(RouteProcessor())
        .add_processor(DeliverProcessor())
        .add_processor(ArchiveProcessor())
    )


def build_minimal_pipeline(name: str) -> DataPipeline:
    """Construct a minimal 3-stage pipeline."""
    return (
        DataPipeline(name)
        .add_processor(IngestProcessor())
        .add_processor(TransformProcessor())
        .add_processor(ArchiveProcessor())
    )
