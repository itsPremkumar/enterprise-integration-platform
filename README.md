# Enterprise Integration Platform

> **System Integration** · **API Orchestration** · **Data Pipeline Coordination** · **Middleware Management**

[![Tests](https://img.shields.io/badge/tests-154%20passed-brightgreen)]()
[![Python](https://img.shields.io/badge/python-3.10+-blue)]()
[![License](https://img.shields.io/badge/license-MIT-green)]()
[![Async](https://img.shields.io/badge/async-asyncio-orange)]()

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [Modules](#modules)
  - [System Connectors](#system-connectors)
  - [API Orchestration](#api-orchestration)
  - [Data Pipelines](#data-pipelines)
  - [Event Bus & Middleware](#event-bus--middleware)
  - [Utilities](#utilities)
- [Platform Facade](#platform-facade)
- [Configuration](#configuration)
- [Testing](#testing)
- [License](#license)
- [Author](#author)

---

## Overview

The **Enterprise Integration Platform (EIP)** is a production-grade Python library for building enterprise system integrations. It provides a modular architecture for connecting disparate systems, orchestrating multi-step API calls, coordinating data pipelines, and managing middleware — all through a unified, async-first API.

**SEO Keywords:** enterprise integration platform, system integration, API orchestration, data pipeline middleware, enterprise service bus, ETL pipeline Python, event-driven architecture, circuit breaker pattern, message queue integration, enterprise application integration.

**GEO Target:** Global enterprise software market — North America, Europe, Asia-Pacific, India.

**AEO (Answer Engine Optimization):** Designed for direct answers to queries like "What is an enterprise integration platform?", "How to orchestrate APIs in Python?", "How to build data pipelines with middleware?"

---

## Key Features

| Feature | Description |
|---------|-------------|
| **System Connectors** | HTTP/REST, WebSocket, Database (SQL/NoSQL), Message Queue with unified lifecycle |
| **API Orchestration** | Multi-step compositions, automatic retries with exponential backoff, circuit breaking, rate limiting |
| **Data Pipelines** | 7-stage configurable pipelines: ingest → transform → validate → enrich → route → deliver → archive |
| **Event Bus** | In-memory pub/sub with topic routing, wildcard subscriptions, history, and exception-safe dispatch |
| **Middleware** | Configurable pre/post processing chain for event transformation and filtering |
| **Connector Registry** | Centralized lifecycle management for all system connectors |
| **Health Monitoring** | Comprehensive health checks across all platform components |
| **Production Ready** | 154 tests, MIT license, async-first, type hints throughout |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                 Enterprise Integration Platform                  │
│                         (Facade)                                 │
├─────────────┬──────────────────┬──────────────┬─────────────────┤
│  Connectors │  Orchestration   │  Pipelines   │  Event Bus      │
│             │                  │              │                 │
│  • HTTP     │  • Execute       │  • Ingest    │  • Pub/Sub      │
│  • WebSocket│  • Batch         │  • Transform │  • Wildcard     │
│  • Database │  • Sequence      │  • Validate  │  • History      │
│  • MQ       │  • Circuit Break │  • Enrich    │  • Middleware   │
│             │  • Rate Limit    │  • Route     │                 │
│             │  • Middleware    │  • Deliver   │                 │
│             │                  │  • Archive   │                 │
└─────────────┴──────────────────┴──────────────┴─────────────────┘
```

---

## Installation

```bash
# From source
git clone https://github.com/itsPremkumar/enterprise-integration-platform.git
cd enterprise-integration-platform
pip install -e ".[dev]"

# Or with uv
uv pip install -e ".[dev]"
```

**Requirements:** Python 3.10+

---

## Quickstart

```python
import asyncio
from enterprise_integration.platform import EnterpriseIntegrationPlatform
from enterprise_integration.pipeline import Event, build_default_pipeline
from enterprise_integration.connectors import HTTPConnector, DatabaseConnector

async def main():
    # Create platform
    platform = EnterpriseIntegrationPlatform("my-platform")

    # Register connectors
    platform.connectors.register(HTTPConnector("crm", {"base_url": "https://crm.example.com"}))
    platform.connectors.register(DatabaseConnector("warehouse", {"dialect": "postgres"}))
    await platform.connectors.connect_all()

    # Register pipeline
    platform.register_pipeline(build_default_pipeline("orders"))

    # Subscribe to events
    async def on_order(event):
        print(f"New order: {event.payload}")
    platform.event_bus.subscribe("orders.new", on_order)

    # Ingest event
    event = Event(topic="orders.new", payload={"id": "123", "total": 99.99}, source="web")
    ctx = await platform.ingest(event)
    print(f"Success: {ctx.success} | Duration: {ctx.duration_ms:.1f}ms")

    # Health check
    health = await platform.health()
    print(f"Connectors: {health['connector_count']}, Pipelines: {health['pipeline_count']}")

    # Shutdown
    await platform.shutdown()

asyncio.run(main())
```

---

## Modules

### System Connectors

Unified interface for connecting to external systems:

```python
from enterprise_integration.connectors import (
    HTTPConnector, WebSocketConnector,
    DatabaseConnector, MessageQueueConnector,
    ConnectorRegistry
)

# HTTP/REST
http = HTTPConnector("api", {"base_url": "https://api.example.com"})
await http.connect()
result = await http.send({"action": "sync"})

# WebSocket
ws = WebSocketConnector("realtime", {"url": "ws://localhost:8080"})
await ws.connect()
await ws.send({"type": "subscribe", "channel": "orders"})

# Database
db = DatabaseConnector("analytics", {"dialect": "postgresql"})
await db.connect()
await db.send({"table": "events", "record": {"type": "click"}})
rows = await db.query("events", {"type": "click"})

# Message Queue
mq = MessageQueueConnector("events", {"max_retries": 3})
await mq.connect()
await mq.send({"queue": "orders", "message": {"id": "1"}})
msg = await mq.consume("orders")

# Registry lifecycle
registry = ConnectorRegistry()
registry.register(http)
registry.register(db)
await registry.connect_all()
await registry.disconnect_all()
```

### API Orchestrate

Multi-step API compositions with resilience patterns:

```python
from enterprise_integration.orchestration import APICall, APIOrchestrator

orch = APIOrchestrator()

# Single call with retries
result = await orch.execute(APICall(
    endpoint="https://api.example.com/data",
    method="POST",
    body={"key": "value"},
    retries_max=3
))

# Batch (concurrent)
results = await orch.execute_batch([
    APICall(endpoint="https://api.example.com/a"),
    APICall(endpoint="https://api.example.com/b"),
])

# Sequence (sequential, stops on failure)
results = await orch.execute_sequence([
    APICall(endpoint="https://api.example.com/step1"),
    APICall(endpoint="https://api.example.com/step2"),
])

# Parallel with fallback
result = await orch.execute_parallel_with_fallback([
    APICall(endpoint="https://primary.example.com"),
    APICall(endpoint="https://secondary.example.com"),
])

# Add middleware
orch.add_middleware(lambda call: APICall(
    endpoint=call.endpoint,
    headers={"Authorization": "Bearer token"}
))

# Rate limiting
orch.set_rate_limit(rate=100, burst=10)
```

### Data Pipelines

Configurable multi-stage data processing:

```python
from enterprise_integration.pipeline import (
    DataPipeline, IngestProcessor, TransformProcessor,
    ValidateProcessor, EnrichProcessor, RouteProcessor,
    DeliverProcessor, ArchiveProcessor, build_default_pipeline
)

# Build a 7-stage pipeline
pipeline = build_default_pipeline("orders")

# Or customize
custom = (
    DataPipeline("events")
    .add_processor(IngestProcessor())
    .add_processor(TransformProcessor(transforms=[
        lambda d: {**d, "normalized": True},
        lambda d: {**d, "hash": hash(str(d))},
    ]))
    .add_processor(ValidateProcessor(required_fields=["id", "type"]))
    .add_processor(EnrichProcessor(enricher=lambda d: {**d, "geo": "US"}))
    .add_processor(RouteProcessor(router=lambda d: ["s3", "elasticsearch"]))
    .add_processor(DeliverProcessor())
    .add_processor(ArchiveProcessor())
)

# Process events
ctx = await pipeline.process(event)
print(f"Stages: {list(ctx.stage_results.keys())}")
print(f"Success: {ctx.success}")
```

### Event Bus & Middleware

Pub/sub event bus with middleware processing:

```python
from enterprise_integration.bus import EventBus, MiddlewareManager

bus = EventBus(max_history=5000)

# Subscribe
async def handler(event):
    print(f"Received: {event.topic}")
bus.subscribe("orders.*", handler)
bus.subscribe("*", lambda e: print(f"Wildcard: {e.topic}"))

# Publish
await bus.publish(event)

# Middleware
mw = MiddlewareManager()
mw.before(lambda e: e if e.topic != "spam" else None)  # Filter
mw.after(lambda e, r: print(f"Post-processed: {e.topic}"))
```

### Utilities

```python
from enterprise_integration.utils import (
    compute_checksum, generate_event_id, now_iso,
    make_event, flatten_dict, deep_merge, safe_json_loads,
    chunk_list, rate_to_percentage, truncate_string
)

# Checksum
checksum = compute_checksum({"order_id": "123", "total": 99.99})

# Event builder
event = make_event("user.signup", {"email": "a@b.com"}, source="web")

# Dict utilities
flat = flatten_dict({"user": {"name": "Alice"}})  # {"user.name": "Alice"}
merged = deep_merge({"a": 1}, {"b": 2})

# Safe JSON
data = safe_json_loads(raw_json, default={})

# Chunking
batches = chunk_list(large_list, size=100)
```

---

## Platform Facade

The `EnterpriseIntegrationPlatform` class provides a single entry point:

```python
platform = EnterpriseIntegrationPlatform("production")

# Connectors
platform.connectors.register(HTTPConnector("crm"))
await platform.connectors.connect_all()

# Pipelines
platform.register_pipeline(build_default_pipeline("orders"))

# Event Bus
platform.event_bus.subscribe("orders.new", handler)

# Middleware
platform.middleware.before(lambda e: add_timestamp(e))

# Ingest
ctx = await platform.ingest(event)

# Health
health = await platform.health()
# Returns: platform name, connector health, pipeline metrics, event bus stats, orchestrator state

# Shutdown
await platform.shutdown()
```

---

## Configuration

All components accept configuration dictionaries:

| Component | Key | Default | Description |
|-----------|-----|---------|-------------|
| HTTPConnector | `base_url` | `""` | Base URL for API calls |
| HTTPConnector | `timeout_ms` | `30000` | Request timeout |
| WebSocketConnector | `url` | `ws://localhost:8080` | WebSocket URL |
| WebSocketConnector | `auto_reconnect` | `True` | Auto-reconnect on disconnect |
| DatabaseConnector | `dialect` | `sqlite` | Database dialect |
| MessageQueueConnector | `max_retries` | `3` | Max retries before DLQ |
| EventBus | `max_history` | `1000` | Max events in history |
| CircuitBreaker | `threshold` | `5` | Failures before opening |
| CircuitBreaker | `reset_timeout` | `30.0` | Seconds before half-open |
| RateLimiter | `rate` | `100` | Tokens per second |
| RateLimiter | `burst` | `10` | Max burst size |

---

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific module tests
pytest tests/test_connectors.py -v
pytest tests/test_orchestration.py -v
pytest tests/test_pipeline.py -v
pytest tests/test_bus.py -v
pytest tests/test_platform.py -v
pytest tests/test_utils.py -v
pytest tests/test_integration.py -v

# With coverage
pytest tests/ --cov=enterprise_integration --cov-report=html
```

**Test Coverage:** 154 tests across 7 test modules covering all components.

---

## License

MIT License. See [LICENSE](LICENSE) for details.

---

## Author

**Prem Kumar** — [GitHub](https://github.com/itsPremkumar) · Enterprise Software Architect

---

## SEO/GEO/AEO Metadata

### Target Queries (SEO)
- enterprise integration platform python
- system integration framework
- api orchestration library
- data pipeline python async
- middleware management python
- enterprise service bus python
- circuit breaker pattern python
- event-driven architecture python

### Geographic Targets (GEO)
- India (primary)
- North America
- Europe
- Asia-Pacific
- Global enterprise market

### Answer Engine Optimization (AEO)
- **Q:** What is an enterprise integration platform?  
  **A:** A unified framework for connecting systems, orchestrating APIs, and coordinating data pipelines.
- **Q:** How to orchestrate multi-step API calls in Python?  
  **A:** Use APIOrchestrator with APICall configs, automatic retries, circuit breaking, and rate limiting.
- **Q:** How to build configurable data pipelines?  
  **A:** Chain PipelineProcessor stages (ingest → transform → validate → enrich → route → deliver → archive).
- **Q:** What is a circuit breaker pattern?  
  **A:** A resilience pattern that stops calling failing services after N consecutive failures, preventing cascade failures.

### Schema.org Structured Data
```json
{
  "@context": "https://schema.org",
  "@type": "SoftwareApplication",
  "name": "Enterprise Integration Platform",
  "applicationCategory": "DeveloperApplication",
  "operatingSystem": "Cross-platform",
  "programmingLanguage": "Python",
  "offers": {
    "@type": "Offer",
    "price": "0",
    "priceCurrency": "USD"
  }
}
```
