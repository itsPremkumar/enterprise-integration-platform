"""
Tests for Enterprise Integration Platform — Data Pipeline
==========================================================
"""

import asyncio
import pytest

from enterprise_integration.pipeline import (
    Event,
    PipelineContext,
    PipelineStage,
    MessagePriority,
    IngestProcessor,
    TransformProcessor,
    ValidateProcessor,
    EnrichProcessor,
    RouteProcessor,
    DeliverProcessor,
    ArchiveProcessor,
    DataPipeline,
    build_default_pipeline,
    build_minimal_pipeline,
)


# --- Event ---

class TestEvent:
    def test_create(self):
        e = Event(topic="orders.new", payload={"id": "123"}, source="web")
        assert e.topic == "orders.new"
        assert e.payload == {"id": "123"}
        assert e.source == "web"
        assert e.event_id
        assert e.timestamp
        assert e.priority == MessagePriority.NORMAL

    def test_to_json(self):
        e = Event(topic="test", payload={"a": 1}, source="s")
        raw = e.to_json()
        assert '"topic": "test"' in raw

    def test_from_json(self):
        e = Event(topic="test", payload={"a": 1}, source="s")
        restored = Event.from_json(e.to_json())
        assert restored.topic == e.topic
        assert restored.payload == e.payload
        assert restored.source == e.source
        assert restored.event_id == e.event_id

    def test_priority_enum(self):
        assert MessagePriority.LOW == 0
        assert MessagePriority.CRITICAL == 20


# --- PipelineContext ---

class TestPipelineContext:
    def test_duration_ms(self):
        ctx = PipelineContext(pipeline_id="p1", event=Event("t", {}, "s"))
        assert ctx.duration_ms >= 0

    def test_success(self):
        ctx = PipelineContext(pipeline_id="p1", event=Event("t", {}, "s"))
        assert ctx.success is True
        ctx.errors.append("fail")
        assert ctx.success is False


# --- IngestProcessor ---

class TestIngestProcessor:
    def test_stage(self):
        assert IngestProcessor().stage == PipelineStage.INGEST

    @pytest.mark.asyncio
    async def test_process(self):
        proc = IngestProcessor()
        event = Event(topic="orders.new", payload={"id": "1"}, source="web")
        ctx = PipelineContext(pipeline_id="p1", event=event)
        result = await proc.process(ctx)
        assert "ingest" in result.stage_results
        assert result.stage_results["ingest"]["topic"] == "orders.new"
        assert result.stage_results["ingest"]["source"] == "web"
        assert result.stage_results["ingest"]["size_bytes"] > 0


# --- TransformProcessor ---

class TestTransformProcessor:
    def test_stage(self):
        assert TransformProcessor().stage == PipelineStage.TRANSFORM

    @pytest.mark.asyncio
    async def test_no_transforms(self):
        proc = TransformProcessor()
        event = Event(topic="t", payload={"a": 1}, source="s")
        ctx = PipelineContext(pipeline_id="p1", event=event)
        result = await proc.process(ctx)
        assert result.stage_results["transform"] == {"a": 1}

    @pytest.mark.asyncio
    async def test_with_transforms(self):
        proc = TransformProcessor(transforms=[lambda d: {**d, "b": 2}])
        event = Event(topic="t", payload={"a": 1}, source="s")
        ctx = PipelineContext(pipeline_id="p1", event=event)
        result = await proc.process(ctx)
        assert result.stage_results["transform"] == {"a": 1, "b": 2}


# --- ValidateProcessor ---

class TestValidateProcessor:
    def test_stage(self):
        assert ValidateProcessor().stage == PipelineStage.VALIDATE

    @pytest.mark.asyncio
    async def test_valid(self):
        proc = ValidateProcessor(required_fields=["id"])
        event = Event(topic="t", payload={"id": "123"}, source="s")
        ctx = PipelineContext(pipeline_id="p1", event=event)
        ctx.stage_results["transform"] = {"id": "123"}
        result = await proc.process(ctx)
        assert result.stage_results["validate"]["valid"] is True
        assert result.stage_results["validate"]["missing_fields"] == []

    @pytest.mark.asyncio
    async def test_invalid(self):
        proc = ValidateProcessor(required_fields=["id", "name"])
        event = Event(topic="t", payload={"id": "123"}, source="s")
        ctx = PipelineContext(pipeline_id="p1", event=event)
        ctx.stage_results["transform"] = {"id": "123"}
        result = await proc.process(ctx)
        assert result.stage_results["validate"]["valid"] is False
        assert "name" in result.stage_results["validate"]["missing_fields"]
        assert len(result.errors) > 0


# --- EnrichProcessor ---

class TestEnrichProcessor:
    def test_stage(self):
        assert EnrichProcessor().stage == PipelineStage.ENRICH

    @pytest.mark.asyncio
    async def test_default_enrich(self):
        proc = EnrichProcessor()
        event = Event(topic="t", payload={"id": "1"}, source="s")
        ctx = PipelineContext(pipeline_id="p1", event=event)
        result = await proc.process(ctx)
        assert result.stage_results["enrich"]["enriched"] is True

    @pytest.mark.asyncio
    async def test_custom_enrich(self):
        proc = EnrichProcessor(enricher=lambda d: {**d, "extra": "data"})
        event = Event(topic="t", payload={"id": "1"}, source="s")
        ctx = PipelineContext(pipeline_id="p1", event=event)
        result = await proc.process(ctx)
        assert result.stage_results["enrich"]["extra"] == "data"


# --- RouteProcessor ---

class TestRouteProcessor:
    def test_stage(self):
        assert RouteProcessor().stage == PipelineStage.ROUTE

    @pytest.mark.asyncio
    async def test_default_route(self):
        proc = RouteProcessor()
        event = Event(topic="t", payload={}, source="s")
        ctx = PipelineContext(pipeline_id="p1", event=event)
        result = await proc.process(ctx)
        assert result.stage_results["route"]["destinations"] == ["default-sink"]

    @pytest.mark.asyncio
    async def test_custom_route(self):
        proc = RouteProcessor(router=lambda d: ["sink-a", "sink-b"])
        event = Event(topic="t", payload={}, source="s")
        ctx = PipelineContext(pipeline_id="p1", event=event)
        result = await proc.process(ctx)
        assert len(result.stage_results["route"]["destinations"]) == 2


# --- DeliverProcessor ---

class TestDeliverProcessor:
    def test_stage(self):
        assert DeliverProcessor().stage == PipelineStage.DELIVER

    @pytest.mark.asyncio
    async def test_process(self):
        proc = DeliverProcessor()
        event = Event(topic="t", payload={}, source="s")
        ctx = PipelineContext(pipeline_id="p1", event=event)
        ctx.stage_results["route"] = {"destinations": ["a", "b"]}
        result = await proc.process(ctx)
        assert result.stage_results["deliver"]["count"] == 2
        assert len(proc.deliveries) == 1


# --- ArchiveProcessor ---

class TestArchiveProcessor:
    def test_stage(self):
        assert ArchiveProcessor().stage == PipelineStage.ARCHIVE

    @pytest.mark.asyncio
    async def test_process(self):
        proc = ArchiveProcessor()
        event = Event(topic="t", payload={"id": "1"}, source="s")
        ctx = PipelineContext(pipeline_id="p1", event=event)
        result = await proc.process(ctx)
        assert "archive" in result.stage_results
        assert len(proc.archived) == 1
        assert proc.archived[0]["event_id"] == event.event_id


# --- DataPipeline ---

class TestDataPipeline:
    def test_init(self):
        p = DataPipeline("test")
        assert p.name == "test"
        assert p.stages == []

    def test_add_processor(self):
        p = DataPipeline("test")
        result = p.add_processor(IngestProcessor())
        assert result is p
        assert len(p.stages) == 1

    @pytest.mark.asyncio
    async def test_process(self):
        p = build_default_pipeline("orders")
        event = Event(topic="orders.new", payload={"id": "123"}, source="web")
        ctx = await p.process(event)
        assert ctx.success is True
        assert "ingest" in ctx.stage_results
        assert "archive" in ctx.stage_results

    @pytest.mark.asyncio
    async def test_process_validation_failure(self):
        p = build_default_pipeline("orders")
        event = Event(topic="orders.new", payload={"no_id": "x"}, source="web")
        ctx = await p.process(event)
        assert ctx.success is False
        assert len(ctx.errors) > 0

    @pytest.mark.asyncio
    async def test_process_batch(self):
        p = build_minimal_pipeline("orders")
        events = [Event(topic="orders.e", payload={"id": str(i)}, source="s") for i in range(5)]
        results = await p.process_batch(events)
        assert len(results) == 5
        assert all(r.success for r in results)

    def test_metrics(self):
        p = DataPipeline("test")
        assert p.metrics == {"processed": 0, "errors": 0}

    def test_stage_count(self):
        p = build_default_pipeline("orders")
        assert p.stage_count == 7


# --- Builders ---

class TestBuilders:
    def test_build_default_pipeline(self):
        p = build_default_pipeline("orders")
        assert p.name == "orders"
        assert p.stage_count == 7
        assert PipelineStage.INGEST in p.stages
        assert PipelineStage.ARCHIVE in p.stages

    def test_build_minimal_pipeline(self):
        p = build_minimal_pipeline("orders")
        assert p.stage_count == 3
