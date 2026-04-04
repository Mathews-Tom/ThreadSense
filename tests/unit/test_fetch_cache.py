from __future__ import annotations

import json
from pathlib import Path

from threadsense.connectors.cache import FetchCache


def test_fetch_cache_round_trips_payload(tmp_path: Path) -> None:
    cache = FetchCache(tmp_path, ttl_seconds=60)

    cache.put("reddit:abc123", {"payload": {"id": "abc123"}})

    assert cache.get("reddit:abc123") == {"payload": {"id": "abc123"}}


def test_fetch_cache_invalidates_entries(tmp_path: Path) -> None:
    cache = FetchCache(tmp_path, ttl_seconds=60)
    cache.put("reddit:abc123", {"payload": {"id": "abc123"}})

    cache.invalidate("reddit:abc123")

    assert cache.get("reddit:abc123") is None


def test_fetch_cache_drops_expired_entries(tmp_path: Path) -> None:
    cache = FetchCache(tmp_path, ttl_seconds=1)
    cache.put("reddit:abc123", {"payload": {"id": "abc123"}})
    entry_path = next(tmp_path.glob("*.json"))
    payload = json.loads(entry_path.read_text(encoding="utf-8"))
    payload["expires_at_utc"] = 0.0
    entry_path.write_text(json.dumps(payload), encoding="utf-8")

    assert cache.get("reddit:abc123") is None
    assert not entry_path.exists()
