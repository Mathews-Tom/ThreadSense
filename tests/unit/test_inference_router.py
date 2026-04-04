from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

from threadsense.config import load_config
from threadsense.errors import InferenceBoundaryError
from threadsense.inference import InferenceRouter, InferenceTask
from threadsense.inference.contracts import validate_task_output
from threadsense.inference.router import InferenceClient
from threadsense.models.analysis import load_analysis_artifact_file
from threadsense.models.canonical import load_canonical_thread
from threadsense.pipeline.analyze import analyze_thread


def load_analysis_fixture(tmp_path: Path) -> Path:
    canonical_path = Path("tests/fixtures/analysis/canonical_feedback_thread.json")
    thread = load_canonical_thread(canonical_path)
    analysis = analyze_thread(thread, canonical_path)
    analysis_path = tmp_path / "analysis.json"
    analysis_path.write_text(json.dumps(analysis.to_dict()), encoding="utf-8")
    return analysis_path


def test_validate_task_output_accepts_analysis_summary_shape() -> None:
    payload = validate_task_output(
        InferenceTask.ANALYSIS_SUMMARY,
        {
            "headline": "Performance dominates",
            "summary": "Latency and docs issues lead the thread.",
            "cited_theme_keys": ["performance", "documentation"],
            "cited_comment_ids": ["reddit:c3", "reddit:c1"],
            "next_steps": ["Profile search", "Expand onboarding docs"],
        },
    )

    assert payload["headline"] == "Performance dominates"


def test_router_returns_deterministic_fallback_when_runtime_is_disabled(tmp_path: Path) -> None:
    analysis_path = load_analysis_fixture(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    config = load_config(
        env={
            "THREADSENSE_RUNTIME_ENABLED": "false",
            "THREADSENSE_RUNTIME_MODEL": "local-model",
        }
    )

    response = InferenceRouter(config).run_analysis_task(
        analysis=analysis,
        task=InferenceTask.ANALYSIS_SUMMARY,
        required=False,
    )

    assert response.used_fallback is True
    assert response.provider == "deterministic_fallback"
    assert response.output["cited_theme_keys"]


def test_router_fails_when_runtime_is_disabled_for_required_task(tmp_path: Path) -> None:
    analysis_path = load_analysis_fixture(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    config = load_config(env={"THREADSENSE_RUNTIME_ENABLED": "false"})

    with pytest.raises(InferenceBoundaryError):
        InferenceRouter(config).run_analysis_task(
            analysis=analysis,
            task=InferenceTask.ANALYSIS_SUMMARY,
            required=True,
        )


def test_router_falls_back_when_client_errors_for_optional_task(tmp_path: Path) -> None:
    analysis_path = load_analysis_fixture(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    config = load_config(env={})

    class FailingClient:
        def complete(self, request: object, **kwargs: object) -> object:
            raise InferenceBoundaryError("runtime failed")

    def failing_client_factory(app_config: object) -> InferenceClient:
        return cast(InferenceClient, FailingClient())

    response = InferenceRouter(
        config,
        client_factory=failing_client_factory,
    ).run_analysis_task(
        analysis=analysis,
        task=InferenceTask.ANALYSIS_SUMMARY,
        required=False,
    )

    assert response.degraded is True
    assert response.failure_reason == "inference_error: runtime failed"


def test_validate_task_output_strips_hallucinated_citations(tmp_path: Path) -> None:
    analysis_path = load_analysis_fixture(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)

    valid_theme_keys = {finding.theme_key for finding in analysis.findings}
    valid_comment_ids: set[str] = set()
    for finding in analysis.findings:
        valid_comment_ids.update(finding.evidence_comment_ids)

    real_theme = next(iter(valid_theme_keys))
    real_comment_id = next(iter(valid_comment_ids))

    payload = validate_task_output(
        InferenceTask.ANALYSIS_SUMMARY,
        {
            "headline": "Test headline",
            "summary": "Test summary",
            "cited_theme_keys": [real_theme, "nonexistent_theme"],
            "cited_comment_ids": [real_comment_id, "reddit:fake_id"],
            "next_steps": ["Review performance"],
        },
        analysis=analysis,
    )

    assert "nonexistent_theme" not in payload["cited_theme_keys"]
    assert "reddit:fake_id" not in payload["cited_comment_ids"]
    assert real_theme in payload["cited_theme_keys"]
    assert real_comment_id in payload["cited_comment_ids"]


def test_validate_report_summary_strips_hallucinated_theme_keys(tmp_path: Path) -> None:
    analysis_path = load_analysis_fixture(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)

    valid_theme_keys = {finding.theme_key for finding in analysis.findings}
    real_theme = next(iter(valid_theme_keys))

    payload = validate_task_output(
        InferenceTask.REPORT_SUMMARY,
        {
            "executive_summary": "Test executive summary",
            "caveats": ["Caveat one"],
            "cited_theme_keys": [real_theme, "hallucinated_key"],
        },
        analysis=analysis,
    )

    assert "hallucinated_key" not in payload["cited_theme_keys"]
    assert real_theme in payload["cited_theme_keys"]


def test_validate_task_output_without_analysis_preserves_all_citations() -> None:
    payload = validate_task_output(
        InferenceTask.ANALYSIS_SUMMARY,
        {
            "headline": "Test headline",
            "summary": "Test summary",
            "cited_theme_keys": ["anything", "goes"],
            "cited_comment_ids": ["fake:1", "fake:2"],
            "next_steps": ["Step one"],
        },
    )

    assert payload["cited_theme_keys"] == ["anything", "goes"]
    assert payload["cited_comment_ids"] == ["fake:1", "fake:2"]
