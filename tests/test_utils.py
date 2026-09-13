"""
Tests for Enterprise Integration Platform — Utilities
======================================================
"""

import pytest

from enterprise_integration.utils import (
    compute_checksum,
    generate_event_id,
    now_iso,
    make_event,
    truncate_string,
    flatten_dict,
    deep_merge,
    safe_json_loads,
    chunk_list,
    rate_to_percentage,
)


class TestComputeChecksum:
    def test_same_payload_same_checksum(self):
        assert compute_checksum({"a": 1}) == compute_checksum({"a": 1})

    def test_different_payload_different_checksum(self):
        assert compute_checksum({"a": 1}) != compute_checksum({"a": 2})

    def test_order_independent(self):
        assert compute_checksum({"a": 1, "b": 2}) == compute_checksum({"b": 2, "a": 1})

    def test_returns_hex(self):
        result = compute_checksum({"key": "value"})
        assert all(c in "0123456789abcdef" for c in result)


class TestGenerateEventId:
    def test_unique(self):
        ids = {generate_event_id() for _ in range(100)}
        assert len(ids) == 100

    def test_is_string(self):
        assert isinstance(generate_event_id(), str)


class TestNowIso:
    def test_returns_string(self):
        assert isinstance(now_iso(), str)

    def test_contains_t(self):
        result = now_iso()
        assert "T" in result


class TestMakeEvent:
    def test_basic(self):
        e = make_event("orders.new", {"id": "1"}, "web")
        assert e["topic"] == "orders.new"
        assert e["payload"] == {"id": "1"}
        assert e["source"] == "web"
        assert "event_id" in e
        assert "timestamp" in e

    def test_defaults(self):
        e = make_event("t", {})
        assert e["source"] == "system"
        assert e["priority"] == 5
        assert e["metadata"] == {}


class TestTruncateString:
    def test_no_truncate(self):
        assert truncate_string("hello", 10) == "hello"

    def test_truncate(self):
        result = truncate_string("hello world", 8)
        assert result == "hello..."

    def test_custom_suffix(self):
        result = truncate_string("hello world", 8, suffix="..")
        assert result == "hello .."


class TestFlattenDict:
    def test_flat(self):
        assert flatten_dict({"a": 1}) == {"a": 1}

    def test_nested(self):
        result = flatten_dict({"a": {"b": 1}})
        assert result == {"a.b": 1}

    def test_deep_nested(self):
        result = flatten_dict({"a": {"b": {"c": 1}}})
        assert result == {"a.b.c": 1}


class TestDeepMerge:
    def test_merge(self):
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}
        result = deep_merge(base, override)
        assert result == {"a": 1, "b": 3, "c": 4}

    def test_nested_merge(self):
        base = {"a": {"x": 1}}
        override = {"a": {"y": 2}}
        result = deep_merge(base, override)
        assert result == {"a": {"x": 1, "y": 2}}

    def test_override_wins(self):
        base = {"a": {"x": 1}}
        override = {"a": "replaced"}
        result = deep_merge(base, override)
        assert result == {"a": "replaced"}


class TestSafeJsonLoads:
    def test_valid(self):
        assert safe_json_loads('{"a": 1}') == {"a": 1}

    def test_invalid(self):
        assert safe_json_loads("not json") is None

    def test_default(self):
        assert safe_json_loads("bad", default={}) == {}


class TestChunkList:
    def test_even(self):
        assert chunk_list([1, 2, 3, 4], 2) == [[1, 2], [3, 4]]

    def test_odd(self):
        assert chunk_list([1, 2, 3], 2) == [[1, 2], [3]]

    def test_empty(self):
        assert chunk_list([], 3) == []


class TestRateToPercentage:
    def test_half(self):
        assert rate_to_percentage(0.5) == "50.0%"

    def test_full(self):
        assert rate_to_percentage(1.0) == "100.0%"

    def test_zero(self):
        assert rate_to_percentage(0.0) == "0.0%"
