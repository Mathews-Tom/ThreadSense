from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EvaluationMetrics:
    theme_precision: float
    theme_recall: float
    evidence_accuracy: float
    severity_alignment: float
    duplicate_recall: float


def compute_precision(reported: set[str], expected: set[str]) -> float:
    if not reported:
        return 1.0 if not expected else 0.0
    return len(reported & expected) / len(reported)


def compute_recall(reported: set[str], expected: set[str]) -> float:
    if not expected:
        return 1.0
    return len(reported & expected) / len(expected)


def compute_ratio(match_count: int, total: int) -> float:
    if total <= 0:
        return 1.0
    return match_count / total
