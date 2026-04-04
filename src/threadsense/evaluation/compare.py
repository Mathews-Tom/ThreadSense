from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from threadsense.config import AnalysisConfig
from threadsense.evaluation.golden import GoldenDataset, validate_against_golden
from threadsense.evaluation.metrics import EvaluationMetrics
from threadsense.models.canonical import Thread
from threadsense.pipeline.analyze import analyze_thread


@dataclass(frozen=True)
class StrategyComparison:
    strategy_a: str
    strategy_b: str
    thread_id: str
    metrics_a: EvaluationMetrics
    metrics_b: EvaluationMetrics
    winner: str | None


def compare_strategies(
    thread: Thread,
    normalized_artifact_path: Path,
    strategy_a: AnalysisConfig,
    strategy_b: AnalysisConfig,
    golden: GoldenDataset,
) -> StrategyComparison:
    analysis_a = analyze_thread(thread, normalized_artifact_path, config=strategy_a)
    analysis_b = analyze_thread(thread, normalized_artifact_path, config=strategy_b)
    metrics_a = validate_against_golden(analysis_a, golden).metrics
    metrics_b = validate_against_golden(analysis_b, golden).metrics
    return StrategyComparison(
        strategy_a=strategy_a.strategy,
        strategy_b=strategy_b.strategy,
        thread_id=thread.thread_id,
        metrics_a=metrics_a,
        metrics_b=metrics_b,
        winner=resolve_winner(strategy_a.strategy, strategy_b.strategy, metrics_a, metrics_b),
    )


def resolve_winner(
    strategy_a: str,
    strategy_b: str,
    metrics_a: EvaluationMetrics,
    metrics_b: EvaluationMetrics,
) -> str | None:
    score_a = aggregate_score(metrics_a)
    score_b = aggregate_score(metrics_b)
    if abs(score_a - score_b) < 0.01:
        return None
    return strategy_a if score_a > score_b else strategy_b


def aggregate_score(metrics: EvaluationMetrics) -> float:
    return (
        metrics.theme_precision
        + metrics.theme_recall
        + metrics.evidence_accuracy
        + metrics.duplicate_recall
        + metrics.severity_alignment
    ) / 5.0
