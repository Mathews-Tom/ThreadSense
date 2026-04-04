from __future__ import annotations

import json
from pathlib import Path

from threadsense.config import load_config
from threadsense.models.analysis import ThreadAnalysis, load_analysis_artifact_file
from threadsense.models.canonical import load_canonical_thread
from threadsense.pipeline.analyze import analyze_thread
from threadsense.pipeline.storage import (
    load_analysis_artifact,
    persist_analysis_artifact_with_config,
)
from threadsense.pipeline.versioning import diff_analyses


def build_analysis(tmp_path: Path) -> tuple[Path, ThreadAnalysis]:
    normalized_path = tmp_path / "normalized.json"
    normalized_path.write_text(
        Path("tests/fixtures/analysis/canonical_feedback_thread.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    thread = load_canonical_thread(normalized_path)
    return normalized_path, analyze_thread(thread, normalized_path)


def test_versioned_analysis_persistence_writes_latest_pointer(tmp_path: Path) -> None:
    normalized_path, analysis = build_analysis(tmp_path)
    storage = load_config(env={"THREADSENSE_STORAGE_VERSIONING_ENABLED": "true"}).storage
    logical_path = tmp_path / "analysis.json"

    persisted_path = persist_analysis_artifact_with_config(storage, logical_path, analysis)

    assert persisted_path == logical_path.with_suffix("") / "latest.json"
    assert persisted_path.exists()
    loaded = load_analysis_artifact(logical_path)
    assert loaded.thread_id == load_analysis_artifact_file(persisted_path).thread_id
    assert normalized_path.exists()


def test_diff_analyses_reports_finding_changes(tmp_path: Path) -> None:
    _, analysis = build_analysis(tmp_path)
    left = analysis
    right_payload = analysis.to_dict()
    right_payload["analysis"]["findings"] = right_payload["analysis"]["findings"][1:]
    right_path = tmp_path / "right.json"
    right_path.write_text(json.dumps(right_payload), encoding="utf-8")
    right = load_analysis_artifact_file(right_path)

    diff = diff_analyses(left, right)

    assert diff["identical"] is False
    assert diff["differences"]
