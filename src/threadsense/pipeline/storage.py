from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from threadsense.config import StorageConfig
from threadsense.connectors.reddit import RedditThreadResult
from threadsense.errors import SchemaBoundaryError
from threadsense.models.analysis import ThreadAnalysis, load_analysis_artifact_file
from threadsense.models.canonical import Thread, load_canonical_thread


@dataclass(frozen=True)
class StoragePaths:
    raw_path: Path
    normalized_path: Path
    analysis_path: Path


def build_storage_paths(
    storage: StorageConfig,
    source_name: str,
    source_thread_id: str,
) -> StoragePaths:
    root = storage.root_dir
    return StoragePaths(
        raw_path=root / storage.raw_dirname / source_name / f"{source_thread_id}.json",
        normalized_path=(
            root / storage.normalized_dirname / source_name / f"{source_thread_id}.json"
        ),
        analysis_path=root / storage.analysis_dirname / source_name / f"{source_thread_id}.json",
    )


def persist_raw_artifact(path: Path, artifact: RedditThreadResult) -> None:
    write_json(path, artifact.to_dict())


def persist_normalized_artifact(path: Path, thread: Thread) -> None:
    write_json(path, thread.to_dict())


def persist_analysis_artifact(path: Path, artifact: ThreadAnalysis) -> None:
    write_json(path, artifact.to_dict())


def load_raw_artifact(path: Path) -> dict[str, Any]:
    payload = read_json(path)
    artifact_version = payload.get("artifact_version")
    source = payload.get("source")
    if artifact_version != 1 or source != "reddit":
        raise SchemaBoundaryError(
            "raw artifact metadata is invalid",
            details={"artifact_version": artifact_version, "source": source},
        )
    return payload


def load_normalized_artifact(path: Path) -> Thread:
    return load_canonical_thread(path)


def load_analysis_artifact(path: Path) -> ThreadAnalysis:
    return load_analysis_artifact_file(path)


def calculate_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise SchemaBoundaryError(
            "artifact path does not exist",
            details={"path": str(path)},
        ) from error
    if not isinstance(payload, dict):
        raise SchemaBoundaryError("artifact must decode to an object")
    return payload
