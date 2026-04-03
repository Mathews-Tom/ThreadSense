from __future__ import annotations

import json
from pathlib import Path

from threadsense.inference.contracts import InferenceResponse, InferenceTask
from threadsense.models.analysis import load_analysis_artifact_file
from threadsense.models.canonical import load_canonical_thread
from threadsense.models.report import load_report_artifact_file
from threadsense.pipeline.analyze import analyze_thread
from threadsense.reporting import build_thread_report, render_report_json, render_report_markdown


def build_analysis_artifact(tmp_path: Path) -> Path:
    canonical_path = Path("tests/fixtures/analysis/canonical_feedback_thread.json")
    thread = load_canonical_thread(canonical_path)
    analysis = analyze_thread(thread, canonical_path)
    analysis_path = tmp_path / "analysis.json"
    analysis_path.write_text(json.dumps(analysis.to_dict()), encoding="utf-8")
    return analysis_path


def test_build_thread_report_uses_local_summary_and_quality_checks(tmp_path: Path) -> None:
    analysis_path = build_analysis_artifact(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    summary_response = InferenceResponse(
        task=InferenceTask.ANALYSIS_SUMMARY,
        provider="local_openai_compatible",
        model="local-model",
        finish_reason="stop",
        output={
            "headline": "Performance and docs dominate the thread",
            "summary": "Latency and onboarding gaps are the main concerns.",
            "cited_theme_keys": ["performance", "documentation"],
            "cited_comment_ids": ["reddit:c3", "reddit:c1"],
            "next_steps": ["Profile search latency", "Expand onboarding quickstart"],
        },
        used_fallback=False,
        degraded=False,
        failure_reason=None,
    )

    report = build_thread_report(
        analysis=analysis,
        analysis_artifact_path=str(analysis_path),
        summary_response=summary_response,
    )

    assert report.executive_summary.provider == "local_openai_compatible"
    assert report.findings
    assert any(check.code == "coverage_gap" for check in report.quality_checks)


def test_render_report_markdown_contains_permalinks_and_sections(tmp_path: Path) -> None:
    analysis_path = build_analysis_artifact(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    report = build_thread_report(
        analysis=analysis,
        analysis_artifact_path=str(analysis_path),
        summary_response=None,
    )

    markdown = render_report_markdown(report)

    assert "# Deterministic analysis fixture thread" in markdown
    assert "## Findings" in markdown
    assert "https://reddit.com/comments/analysis123/c3" in markdown


def test_render_report_json_round_trips_report_artifact(tmp_path: Path) -> None:
    analysis_path = build_analysis_artifact(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    report = build_thread_report(
        analysis=analysis,
        analysis_artifact_path=str(analysis_path),
        summary_response=None,
    )
    report_path = tmp_path / "report.json"
    report_path.write_text(render_report_json(report), encoding="utf-8")

    loaded = load_report_artifact_file(report_path)

    assert loaded.thread_id == report.thread_id
    assert loaded.executive_summary.headline == report.executive_summary.headline
