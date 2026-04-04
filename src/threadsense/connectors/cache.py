from __future__ import annotations

import hashlib
import json
from pathlib import Path
from time import time
from typing import Any

from threadsense.errors import SchemaBoundaryError


class FetchCache:
    def __init__(self, cache_dir: Path, ttl_seconds: int = 3600) -> None:
        self._cache_dir = cache_dir
        self._ttl_seconds = ttl_seconds

    def get(self, cache_key: str) -> dict[str, Any] | None:
        path = self._entry_path(cache_key)
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise SchemaBoundaryError(
                "cache entry is not valid JSON",
                details={"path": str(path)},
            ) from error
        if not isinstance(payload, dict):
            raise SchemaBoundaryError("cache entry must decode to an object")
        expires_at = payload.get("expires_at_utc")
        if not isinstance(expires_at, (int, float)):
            raise SchemaBoundaryError(
                "cache entry metadata is invalid",
                details={"path": str(path)},
            )
        if float(expires_at) < time():
            path.unlink(missing_ok=True)
            return None
        cached_payload = payload.get("payload")
        if not isinstance(cached_payload, dict):
            raise SchemaBoundaryError(
                "cache entry payload is invalid",
                details={"path": str(path)},
            )
        return cached_payload

    def put(self, cache_key: str, payload: dict[str, Any]) -> None:
        path = self._entry_path(cache_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "cache_key": cache_key,
                    "cached_at_utc": time(),
                    "expires_at_utc": time() + self._ttl_seconds,
                    "payload": payload,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    def invalidate(self, cache_key: str) -> None:
        self._entry_path(cache_key).unlink(missing_ok=True)

    def _entry_path(self, cache_key: str) -> Path:
        digest = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()
        return self._cache_dir / f"{digest}.json"
