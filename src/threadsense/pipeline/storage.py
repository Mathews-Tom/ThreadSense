from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from threadsense.config import StorageConfig
from threadsense.errors import SchemaBoundaryError
from threadsense.models.analysis import ThreadAnalysis, load_analysis_artifact_file
from threadsense.models.canonical import Thread, load_canonical_thread
from threadsense.models.corpus import (
    CorpusAnalysis,
    CorpusManifest,
    load_corpus_analysis_file,
    load_corpus_manifest_file,
)
from threadsense.models.report import ThreadReport, load_report_artifact_file
from threadsense.pipeline.versioning import (
    load_latest,
    load_version,
    save_versioned_artifact,
)


@dataclass(frozen=True)
class StoragePaths:
    raw_path: Path
    normalized_path: Path
    analysis_path: Path
    report_json_path: Path
    report_markdown_path: Path
    report_html_path: Path


@dataclass(frozen=True)
class CorpusPaths:
    manifest_path: Path
    analysis_path: Path
    report_markdown_path: Path
    index_path: Path


def build_storage_paths(
    storage: StorageConfig,
    source_name: str,
    source_thread_id: str,
) -> StoragePaths:
    root = storage.root_dir
    source_dir = storage_source_name(source_name)
    return StoragePaths(
        raw_path=root / storage.raw_dirname / source_dir / f"{source_thread_id}.json",
        normalized_path=(
            root / storage.normalized_dirname / source_dir / f"{source_thread_id}.json"
        ),
        analysis_path=root / storage.analysis_dirname / source_dir / f"{source_thread_id}.json",
        report_json_path=root / storage.report_dirname / source_dir / f"{source_thread_id}.json",
        report_markdown_path=root / storage.report_dirname / source_dir / f"{source_thread_id}.md",
        report_html_path=root / storage.report_dirname / source_dir / f"{source_thread_id}.html",
    )


def build_corpus_paths(storage: StorageConfig, corpus_id: str) -> CorpusPaths:
    root = storage.root_dir / storage.corpus_dirname / corpus_id
    return CorpusPaths(
        manifest_path=root / "manifest.json",
        analysis_path=root / "analysis.json",
        report_markdown_path=root / "report.md",
        index_path=storage.root_dir / storage.index_dirname / "corpora.json",
    )


def persist_raw_artifact(path: Path, artifact: Any) -> None:
    write_json(path, artifact.to_dict())


def persist_normalized_artifact(path: Path, thread: Thread) -> None:
    write_json(path, thread.to_dict())


def persist_analysis_artifact(path: Path, artifact: ThreadAnalysis) -> None:
    write_json(path, artifact.to_dict())


def persist_analysis_artifact_with_config(
    storage: StorageConfig,
    path: Path,
    artifact: ThreadAnalysis,
) -> Path:
    if storage.versioning_enabled:
        return save_versioned_artifact(path, artifact.to_dict()).latest_path
    persist_analysis_artifact(path, artifact)
    return path


def persist_report_artifact(path: Path, artifact: ThreadReport) -> None:
    write_json(path, artifact.to_dict())


def persist_corpus_manifest(path: Path, manifest: CorpusManifest) -> None:
    write_json(path, manifest.to_dict())


def persist_corpus_analysis(path: Path, artifact: CorpusAnalysis) -> None:
    write_json(path, artifact.to_dict())


def load_raw_artifact(path: Path) -> dict[str, Any]:
    payload = read_json(path)
    artifact_version = payload.get("artifact_version")
    source = payload.get("source")
    if artifact_version != 2 or not isinstance(source, str) or not source:
        raise SchemaBoundaryError(
            "raw artifact metadata is invalid",
            details={"artifact_version": artifact_version, "source": source},
        )
    return payload


def load_normalized_artifact(path: Path) -> Thread:
    return load_canonical_thread(path)


def load_analysis_artifact(path: Path) -> ThreadAnalysis:
    resolved_path = resolve_analysis_artifact_path(path)
    return load_analysis_artifact_file(resolved_path)


def load_analysis_artifact_version(path: Path, version_number: int) -> ThreadAnalysis:
    return load_analysis_artifact_file(load_version(path, version_number))


def resolve_analysis_artifact_path(path: Path) -> Path:
    if path.suffix != ".json":
        return load_latest(path)
    if path.exists():
        return path
    version_dir = path.with_suffix("")
    if version_dir.is_dir():
        return load_latest(path)
    return path


def load_report_artifact(path: Path) -> ThreadReport:
    return load_report_artifact_file(path)


def load_corpus_manifest(path: Path) -> CorpusManifest:
    return load_corpus_manifest_file(path)


def load_corpus_analysis(path: Path) -> CorpusAnalysis:
    return load_corpus_analysis_file(path)


def calculate_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


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


def storage_source_name(source_name: str) -> str:
    if source_name == "hackernews":
        return "hn"
    return source_name
