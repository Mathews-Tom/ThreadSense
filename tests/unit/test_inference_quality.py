from __future__ import annotations

from pathlib import Path

from threadsense.evaluation import evaluate_inference_quality
from threadsense.inference import InferenceResponse, InferenceTask
from threadsense.models.canonical import load_canonical_thread
from threadsense.pipeline.analyze import analyze_thread


def test_evaluate_inference_quality_flags_invalid_citations() -> None:
    fixture_path = Path("tests/fixtures/analysis/canonical_feedback_thread.json")
    analysis = analyze_thread(load_canonical_thread(fixture_path), fixture_path)
    response = InferenceResponse(
        task=InferenceTask.ANALYSIS_SUMMARY,
        provider="local_openai_compatible",
        model="fixture-model",
        finish_reason="stop",
        output={
            "headline": "Performance dominates",
            "summary": "Latency and onboarding problems lead the thread.",
            "priority": "high",
            "confidence": 0.8,
            "why_now": "Performance is the clearest evidence cluster.",
            "cited_theme_keys": ["performance", "documentation"],
            "cited_comment_ids": ["reddit:c3", "reddit:missing"],
            "next_steps": ["Profile search latency"],
            "recommended_owner": "engineering",
            "action_type": "fix",
            "expected_outcome": "Remove the highest-friction bottleneck.",
        },
        used_fallback=False,
        degraded=False,
        failure_reason=None,
    )

    report = evaluate_inference_quality(analysis, response)

    assert report.hallucination_rate == 0.5
    assert report.invalid_citations == ["reddit:missing"]
    assert report.coherence_score == 1.0
