from __future__ import annotations

import json

from threadsense.inference.contracts import InferenceMessage, InferenceRequest, InferenceTask
from threadsense.models.analysis import ThreadAnalysis


def build_task_request(
    task: InferenceTask,
    analysis: ThreadAnalysis,
    required: bool,
    repair_retries: int,
) -> InferenceRequest:
    if task is InferenceTask.ANALYSIS_SUMMARY:
        return build_analysis_summary_request(
            analysis=analysis,
            required=required,
            repair_retries=repair_retries,
        )
    if task is InferenceTask.FINDING_CLASSIFICATION:
        return build_finding_classification_request(
            analysis=analysis,
            required=required,
            repair_retries=repair_retries,
        )
    if task is InferenceTask.REPORT_SUMMARY:
        return build_report_summary_request(
            analysis=analysis,
            required=required,
            repair_retries=repair_retries,
        )
    raise ValueError(f"unsupported inference task: {task.value}")


def build_analysis_summary_request(
    analysis: ThreadAnalysis,
    required: bool,
    repair_retries: int,
) -> InferenceRequest:
    return InferenceRequest(
        task=InferenceTask.ANALYSIS_SUMMARY,
        messages=[
            InferenceMessage(
                role="system",
                content=(
                    "You summarize evidence-backed thread analysis. "
                    "Return only valid JSON with keys "
                    "headline, summary, cited_theme_keys, cited_comment_ids, next_steps. "
                    "Do not include markdown fences."
                ),
            ),
            InferenceMessage(
                role="user",
                content=(
                    "Use only the provided deterministic evidence. "
                    "Every cited theme key and comment id must come from the input. "
                    f"Input:\n{render_analysis_payload(analysis)}"
                ),
            ),
        ],
        required=required,
        repair_retries=repair_retries,
        repair_instruction=(
            "Your previous response was invalid. Return only valid JSON with keys "
            "headline, summary, cited_theme_keys, cited_comment_ids, next_steps. "
            "Use arrays of strings for cited_theme_keys, cited_comment_ids, and next_steps."
        ),
    )


def build_finding_classification_request(
    analysis: ThreadAnalysis,
    required: bool,
    repair_retries: int,
) -> InferenceRequest:
    return InferenceRequest(
        task=InferenceTask.FINDING_CLASSIFICATION,
        messages=[
            InferenceMessage(
                role="system",
                content=(
                    "You classify analysis findings. Return only valid JSON with key "
                    "classifications. classifications must be a list of objects with "
                    "theme_key, category, confidence."
                ),
            ),
            InferenceMessage(
                role="user",
                content=(
                    f"Classify these deterministic findings:\n{render_analysis_payload(analysis)}"
                ),
            ),
        ],
        required=required,
        repair_retries=repair_retries,
        repair_instruction=(
            "Return only valid JSON with key classifications. Each classification must include "
            "theme_key, category, confidence."
        ),
    )


def build_report_summary_request(
    analysis: ThreadAnalysis,
    required: bool,
    repair_retries: int,
) -> InferenceRequest:
    return InferenceRequest(
        task=InferenceTask.REPORT_SUMMARY,
        messages=[
            InferenceMessage(
                role="system",
                content=(
                    "You produce a concise evidence-backed executive summary. "
                    "Return only valid JSON with keys executive_summary, caveats, cited_theme_keys."
                ),
            ),
            InferenceMessage(
                role="user",
                content=(
                    f"Summarize this deterministic analysis:\n{render_analysis_payload(analysis)}"
                ),
            ),
        ],
        required=required,
        repair_retries=repair_retries,
        repair_instruction=(
            "Return only valid JSON with keys executive_summary, caveats, cited_theme_keys. "
            "caveats and cited_theme_keys must be arrays of strings."
        ),
    )


def render_analysis_payload(analysis: ThreadAnalysis) -> str:
    payload = {
        "thread_id": analysis.thread_id,
        "title": analysis.title,
        "top_phrases": analysis.top_phrases[:8],
        "findings": [
            {
                "theme_key": finding.theme_key,
                "theme_label": finding.theme_label,
                "severity": finding.severity,
                "comment_count": finding.comment_count,
                "key_phrases": finding.key_phrases[:5],
                "evidence_comment_ids": finding.evidence_comment_ids,
                "quotes": [quote.body_excerpt for quote in finding.quotes[:2]],
            }
            for finding in analysis.findings[:5]
        ],
    }
    return json.dumps(payload, indent=2)
