from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from threadsense.models.canonical import Thread


@dataclass(frozen=True)
class FetchRequest:
    url: str
    expand: bool = False
    timeout_seconds: float = 15


@runtime_checkable
class RawArtifact(Protocol):
    @property
    def source_name(self) -> str: ...

    @property
    def source_thread_id(self) -> str: ...

    @property
    def thread_title(self) -> str: ...

    @property
    def normalized_url(self) -> str: ...

    @property
    def total_comment_count(self) -> int: ...

    def to_dict(self) -> dict[str, Any]: ...


@runtime_checkable
class SourceConnector(Protocol):
    source_name: str

    def fetch(self, request: FetchRequest) -> RawArtifact: ...

    def normalize(self, raw_artifact: Mapping[str, Any], raw_artifact_path: Path) -> Thread: ...

    def supports_url(self, url: str) -> bool: ...
