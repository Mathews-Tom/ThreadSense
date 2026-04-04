from __future__ import annotations

from dataclasses import dataclass

from threadsense.inference.contracts import InferenceResponse, InferenceTask
from threadsense.models.analysis import ThreadAnalysis


@dataclass(frozen=True)
class InferenceQualityReport:
    hallucination_rate: float
    coverage_improvement: int
    coherence_score: float
    invalid_citations: list[str]


def evaluate_inference_quality(
    analysis: ThreadAnalysis,
    inference_response: InferenceResponse,
) -> InferenceQualityReport:
    valid_comment_ids = {
        comment_id for finding in analysis.findings for comment_id in finding.evidence_comment_ids
    }
    cited_comment_ids = extract_cited_comment_ids(inference_response)
    invalid_citations = sorted(
        comment_id for comment_id in cited_comment_ids if comment_id not in valid_comment_ids
    )
    hallucination_rate = 0.0
    if cited_comment_ids:
        hallucination_rate = len(invalid_citations) / len(cited_comment_ids)

    baseline_citation_count = (
        len(analysis.findings[0].evidence_comment_ids[:5]) if analysis.findings else 0
    )
    coverage_improvement = max(0, len(set(cited_comment_ids)) - baseline_citation_count)

    coherence_score = score_coherence(inference_response)
    return InferenceQualityReport(
        hallucination_rate=hallucination_rate,
        coverage_improvement=coverage_improvement,
        coherence_score=coherence_score,
        invalid_citations=invalid_citations,
    )


def extract_cited_comment_ids(inference_response: InferenceResponse) -> list[str]:
    output = inference_response.output
    if inference_response.task is InferenceTask.ANALYSIS_SUMMARY:
        cited = output.get("cited_comment_ids", [])
        return [item for item in cited if isinstance(item, str)]
    return []


def score_coherence(inference_response: InferenceResponse) -> float:
    output = inference_response.output
    text_fields = [
        value
        for key, value in output.items()
        if key in {"headline", "summary", "executive_summary", "confidence_note"}
        and isinstance(value, str)
        and value.strip()
    ]
    list_fields = [
        value
        for key, value in output.items()
        if key in {"next_steps", "key_patterns", "recommended_actions"}
        and isinstance(value, list)
        and value
    ]
    score = 0.0
    if text_fields:
        score += 0.6
    if list_fields:
        score += 0.4
    return score
