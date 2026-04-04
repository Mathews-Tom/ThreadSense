from __future__ import annotations

import json

from threadsense.inference.contracts import InferenceMessage, InferenceRequest, InferenceTask
from threadsense.models.analysis import ThreadAnalysis
from threadsense.models.canonical import Thread
from threadsense.models.corpus import CorpusAnalysis


def build_task_request(
    task: InferenceTask,
    *,
    analysis: ThreadAnalysis | None = None,
    corpus: CorpusAnalysis | None = None,
    required: bool,
    repair_retries: int,
) -> InferenceRequest:
    if task is InferenceTask.ANALYSIS_SUMMARY:
        if analysis is None:
            raise ValueError("analysis payload is required for analysis summary")
        return build_analysis_summary_request(
            analysis=analysis,
            required=required,
            repair_retries=repair_retries,
        )
    if task is InferenceTask.FINDING_CLASSIFICATION:
        if analysis is None:
            raise ValueError("analysis payload is required for finding classification")
        return build_finding_classification_request(
            analysis=analysis,
            required=required,
            repair_retries=repair_retries,
        )
    if task is InferenceTask.REPORT_SUMMARY:
        if analysis is None:
            raise ValueError("analysis payload is required for report summary")
        return build_report_summary_request(
            analysis=analysis,
            required=required,
            repair_retries=repair_retries,
        )
    if task is InferenceTask.CORPUS_SYNTHESIS:
        if corpus is None:
            raise ValueError("corpus payload is required for corpus synthesis")
        return build_corpus_synthesis_request(
            corpus=corpus,
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


def build_corpus_synthesis_request(
    corpus: CorpusAnalysis,
    required: bool,
    repair_retries: int,
) -> InferenceRequest:
    return InferenceRequest(
        task=InferenceTask.CORPUS_SYNTHESIS,
        messages=[
            InferenceMessage(
                role="system",
                content=(
                    "You synthesize evidence-backed corpus analysis across threads. "
                    "Return only valid JSON with keys headline, key_patterns, "
                    "cited_thread_ids, recommended_actions, confidence_note."
                ),
            ),
            InferenceMessage(
                role="user",
                content=(
                    "Use only the provided deterministic cross-thread evidence. "
                    "Every cited thread id must come from the input. "
                    f"Input:\n{render_corpus_payload(corpus)}"
                ),
            ),
        ],
        required=required,
        repair_retries=repair_retries,
        repair_instruction=(
            "Return only valid JSON with keys headline, key_patterns, "
            "cited_thread_ids, recommended_actions, confidence_note. "
            "The list fields must all contain strings."
        ),
    )


def render_analysis_payload(
    analysis: ThreadAnalysis,
    thread: Thread | None = None,
) -> str:
    payload: dict[str, object] = {
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
    if thread is not None:
        top_level = [c for c in thread.comments if c.parent_comment_id is None]
        top_comments = sorted(top_level, key=lambda c: c.score, reverse=True)[:3]
        payload["top_comments"] = [
            {"author": c.author.username, "body": c.body[:300], "score": c.score}
            for c in top_comments
        ]
        payload["conversation_structure"] = {
            "total_comments": analysis.total_comments,
            "max_depth": analysis.conversation_structure.max_depth,
            "top_level_count": analysis.conversation_structure.top_level_count,
            "consensus_count": analysis.conversation_structure.consensus_count,
            "controversy_count": analysis.conversation_structure.controversy_count,
        }
    return json.dumps(payload, indent=2)


def build_vocabulary_expansion_request(
    thread: Thread,
    theme_rules: dict[str, tuple[str, ...]],
    *,
    sample_limit: int = 5,
    required: bool = False,
    repair_retries: int = 1,
) -> InferenceRequest:
    top_level = [c for c in thread.comments if c.parent_comment_id is None]
    sampled = sorted(top_level, key=lambda c: c.score, reverse=True)[:sample_limit]
    sample_text = "\n---\n".join(f"[score={c.score}] {c.body[:300]}" for c in sampled)
    existing_themes = json.dumps(
        {key: list(keywords) for key, keywords in theme_rules.items()},
        indent=2,
    )
    return InferenceRequest(
        task=InferenceTask.VOCABULARY_EXPANSION,
        messages=[
            InferenceMessage(
                role="system",
                content=(
                    "You analyze discussion threads and propose thematic keywords. "
                    "Return only valid JSON with keys existing_themes and new_themes. "
                    "existing_themes maps existing theme names to lists of additional keywords. "
                    "new_themes maps new theme names to lists of keywords. "
                    "Max 10 keywords per theme, max 3 new themes. "
                    "Keywords must be single lowercase words or short phrases. "
                    "Do not repeat keywords already in the vocabulary. "
                    "Do not include markdown fences."
                ),
            ),
            InferenceMessage(
                role="user",
                content=(
                    f"Thread title: {thread.title}\n\n"
                    f"Existing vocabulary themes:\n{existing_themes}\n\n"
                    f"Top comments by score:\n{sample_text}\n\n"
                    "Propose additional keywords for existing themes and up to 3 new themes "
                    "that would help classify these comments."
                ),
            ),
        ],
        required=required,
        repair_retries=repair_retries,
        repair_instruction=(
            "Return only valid JSON with keys existing_themes and new_themes. "
            "Both must be objects mapping theme names to arrays of keyword strings."
        ),
    )


def render_corpus_payload(corpus: CorpusAnalysis) -> str:
    payload = {
        "corpus_id": corpus.corpus_id,
        "name": corpus.name,
        "domain": corpus.domain.value,
        "thread_count": corpus.thread_count,
        "cross_thread_findings": [
            {
                "theme_key": finding.theme_key,
                "theme_label": finding.theme_label,
                "severity": finding.severity,
                "thread_count": finding.thread_count,
                "total_comment_count": finding.total_comment_count,
                "top_evidence": [
                    {
                        "thread_id": evidence.thread_id,
                        "thread_title": evidence.thread_title,
                        "finding_severity": evidence.finding_severity,
                        "comment_count": evidence.comment_count,
                        "quote": evidence.top_quote.body_excerpt,
                    }
                    for evidence in finding.top_evidence[:3]
                ],
            }
            for finding in corpus.cross_thread_findings[:5]
        ],
    }
    return json.dumps(payload, indent=2)
