from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from threadsense.errors import ThreadSenseError


@runtime_checkable
class _ErrorFactory(Protocol):
    """Constructor signature shared by all ThreadSenseError subclasses."""

    def __call__(self, message: str, details: dict[str, Any] | None = ...) -> ThreadSenseError: ...


class SchemaReader:
    """Parameterized schema field extractor with domain-specific error boundaries."""

    def __init__(self, error_cls: _ErrorFactory, label: str) -> None:
        self._error_cls = error_cls
        self._label = label

    def required_str(self, payload: Mapping[str, Any], key: str) -> str:
        value = payload.get(key)
        if not isinstance(value, str) or not value:
            raise self._error_cls(
                f"{self._label} string field is invalid",
                details={"key": key},
            )
        return value

    def optional_str(self, payload: Mapping[str, Any], key: str, default: str) -> str:
        value = payload.get(key, default)
        if value is None:
            return default
        if not isinstance(value, str):
            raise self._error_cls(
                f"{self._label} string field has invalid type",
                details={"key": key},
            )
        return value

    def optional_nullable_str(self, payload: Mapping[str, Any], key: str) -> str | None:
        value = payload.get(key)
        if value is None:
            return None
        if not isinstance(value, str) or not value:
            raise self._error_cls(
                f"{self._label} optional string field is invalid",
                details={"key": key},
            )
        return value

    def required_int(self, payload: Mapping[str, Any], key: str) -> int:
        value = payload.get(key)
        if not isinstance(value, int):
            raise self._error_cls(
                f"{self._label} integer field is invalid",
                details={"key": key},
            )
        return value

    def optional_int(self, payload: Mapping[str, Any], key: str, default: int) -> int:
        value = payload.get(key, default)
        if not isinstance(value, int):
            raise self._error_cls(
                f"{self._label} integer field has invalid type",
                details={"key": key},
            )
        return value

    def required_float(self, payload: Mapping[str, Any], key: str) -> float:
        value = payload.get(key)
        if isinstance(value, int):
            return float(value)
        if not isinstance(value, float):
            raise self._error_cls(
                f"{self._label} float field is invalid",
                details={"key": key},
            )
        return value

    def optional_float(self, payload: Mapping[str, Any], key: str, default: float) -> float:
        value = payload.get(key, default)
        if isinstance(value, int):
            return float(value)
        if not isinstance(value, float):
            raise self._error_cls(
                f"{self._label} float field has invalid type",
                details={"key": key},
            )
        return value

    def required_bool(self, payload: Mapping[str, Any], key: str) -> bool:
        value = payload.get(key)
        if not isinstance(value, bool):
            raise self._error_cls(
                f"{self._label} boolean field is invalid",
                details={"key": key},
            )
        return value

    def nested_object(self, payload: Mapping[str, Any], *keys: str) -> dict[str, Any]:
        current: Any = payload
        for key in keys:
            if not isinstance(current, dict):
                raise self._error_cls(
                    f"{self._label} object boundary is invalid",
                    details={"key": key},
                )
            current = current.get(key)
        if not isinstance(current, dict):
            raise self._error_cls(
                f"{self._label} object boundary is invalid",
                details={"keys": list(keys)},
            )
        return current

    def nested_list(self, payload: Mapping[str, Any], *keys: str) -> list[Any]:
        current: Any = payload
        for key in keys:
            if not isinstance(current, dict):
                raise self._error_cls(
                    f"{self._label} list boundary is invalid",
                    details={"key": key},
                )
            current = current.get(key)
        if not isinstance(current, list):
            raise self._error_cls(
                f"{self._label} list boundary is invalid",
                details={"keys": list(keys)},
            )
        return current
