"""
Enterprise Integration Platform — Utilities
============================================
Shared utility functions for checksums, event builders,
and platform helpers.

MIT License. Copyright 2026 Prem Kumar.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any


def compute_checksum(payload: dict[str, Any]) -> str:
    """Compute SHA-256 checksum of a dict payload (first 16 hex chars)."""
    canonical = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def generate_event_id() -> str:
    """Generate a unique event identifier."""
    return uuid.uuid4().hex


def now_iso() -> str:
    """Return current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def make_event(
    topic: str,
    payload: dict[str, Any],
    source: str = "system",
    priority: int = 5,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a standardized event dict."""
    return {
        "event_id": generate_event_id(),
        "topic": topic,
        "payload": payload,
        "source": source,
        "timestamp": now_iso(),
        "priority": priority,
        "metadata": metadata or {},
    }


def truncate_string(s: str, max_len: int = 100, suffix: str = "...") -> str:
    """Truncate a string to max_len characters."""
    if len(s) <= max_len:
        return s
    return s[: max_len - len(suffix)] + suffix


def flatten_dict(d: dict[str, Any], sep: str = ".", prefix: str = "") -> dict[str, Any]:
    """Flatten a nested dict into dot-separated keys."""
    items: dict[str, Any] = {}
    for k, v in d.items():
        new_key = f"{prefix}{sep}{k}" if prefix else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, sep=sep, prefix=new_key))
        else:
            items[new_key] = v
    return items


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep merge two dicts. Override values take precedence."""
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def safe_json_loads(raw: str, default: Any = None) -> Any:
    """Safely parse JSON string, returning default on error."""
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return default


def chunk_list(lst: list[Any], size: int) -> list[list[Any]]:
    """Split a list into chunks of given size."""
    return [lst[i : i + size] for i in range(0, len(lst), size)]


def rate_to_percentage(rate: float) -> str:
    """Convert a rate (0-1) to percentage string."""
    return f"{rate * 100:.1f}%"
