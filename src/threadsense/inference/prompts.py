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
    thread: Thread | None = None,
    corpus: CorpusAnalysis | None = None,
    required: bool,
    repair_retries: int,
) -> InferenceRequest:
    if task is InferenceTask.ANALYSIS_SUMMARY:
        if analysis is None:
            raise ValueError("analysis payload is required for analysis summary")
        if thread is None:
            raise ValueError("thread payload is required for analysis summary")
        return build_analysis_summary_request(
            analysis=analysis,
            thread=thread,
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
    thread: Thread,
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
                    "Use the thread title and top comments to explain what the discussion is about. "
                    "Prioritize only the most decision-relevant findings. "
                    "Distinguish issues, requests, and mixed signals using the provided marker counts. "
                    "Convert findings into concrete owner-oriented actions instead of generic review steps. "
                    "If evidence is mixed, prefer investigate over fix. "
                    "Return only valid JSON with keys "
                    "headline, summary, priority, confidence, why_now, "
                    "cited_theme_keys, cited_comment_ids, next_steps, "
                    "recommended_owner, action_type, expected_outcome. "
                    "priority must be one of high, medium, low. "
                    "confidence must be a float between 0.0 and 1.0. "
                    "action_type must be one of fix, investigate, document, design, monitor. "
                    "Do not include markdown fences."
                ),
            ),
            InferenceMessage(
                role="user",
                content=(
                    "Use only the provided deterministic evidence. "
                    "Every cited theme key and comment id must come from the input. "
                    "Treat the thread title as the discussion framing when no separate post body is present. "
                    "Prefer actions a product, engineering, docs, or research owner could execute this week. "
                    "Do not restate a finding as a next step without making it actionable. "
                    f"Input:\n{render_analysis_summary_payload(analysis, thread)}"
                ),
            ),
        ],
        required=required,
        repair_retries=repair_retries,
        repair_instruction=(
            "Your previous response was invalid. Return only valid JSON with keys "
            "headline, summary, priority, confidence, why_now, cited_theme_keys, "
            "cited_comment_ids, next_steps, recommended_owner, action_type, expected_outcome. "
            "Use arrays of strings for cited_theme_keys, cited_comment_ids, and next_steps. "
            "priority must be high, medium, or low. "
            "confidence must be a float between 0.0 and 1.0. "
            "action_type must be fix, investigate, document, design, or monitor."
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
) -> str:
    payload: dict[str, object] = {
        "thread_id": analysis.thread_id,
        "title": analysis.title,
        "source_name": analysis.source_name,
        "top_phrases": analysis.top_phrases[:8],
        "analysis_overview": {
            "total_comments": analysis.total_comments,
            "filtered_comment_count": analysis.filtered_comment_count,
            "distinct_comment_count": analysis.distinct_comment_count,
            "duplicate_group_count": analysis.duplicate_group_count,
        },
        "conversation_structure": {
            "max_depth": analysis.conversation_structure.max_depth,
            "top_level_count": analysis.conversation_structure.top_level_count,
            "reply_chain_count": analysis.conversation_structure.reply_chain_count,
            "longest_chain_length": analysis.conversation_structure.longest_chain_length,
            "controversy_count": analysis.conversation_structure.controversy_count,
            "consensus_count": analysis.conversation_structure.consensus_count,
            "monologue_count": analysis.conversation_structure.monologue_count,
            "top_engagement_subtrees": [
                {
                    "root_comment_id": subtree.root_comment_id,
                    "root_author": subtree.root_author,
                    "subtree_size": subtree.subtree_size,
                    "max_depth_below": subtree.max_depth_below,
                    "engagement_score": subtree.engagement_score,
                }
                for subtree in analysis.conversation_structure.top_engagement_subtrees[:3]
            ],
        },
        "findings": [
            {
                "theme_key": finding.theme_key,
                "theme_label": finding.theme_label,
                "severity": finding.severity,
                "comment_count": finding.comment_count,
                "issue_marker_count": finding.issue_marker_count,
                "request_marker_count": finding.request_marker_count,
                "key_phrases": finding.key_phrases[:5],
                "evidence_comment_ids": finding.evidence_comment_ids[:5],
                "quotes": [
                    {
                        "comment_id": quote.comment_id,
                        "author": quote.author,
                        "score": quote.score,
                        "body_excerpt": quote.body_excerpt,
                        "permalink": quote.permalink,
                    }
                    for quote in finding.quotes[:2]
                ],
            }
            for finding in analysis.findings[:5]
        ],
    }
    if analysis.alignment_check is not None:
        payload["alignment"] = {
            "domain": analysis.alignment_check.domain,
            "domain_fit_score": analysis.alignment_check.domain_fit_score,
            "general_feedback_ratio": analysis.alignment_check.general_feedback_ratio,
            "suggested_domain": analysis.alignment_check.suggested_domain,
            "warning": analysis.alignment_check.warning,
        }
    return json.dumps(payload, indent=2)


def render_analysis_summary_payload(analysis: ThreadAnalysis, thread: Thread) -> str:
    payload = json.loads(render_analysis_payload(analysis))
    top_level = [comment for comment in thread.comments if comment.parent_comment_id is None]
    top_comments = sorted(top_level, key=lambda comment: comment.score, reverse=True)[:3]
    payload["thread_context"] = {
        "source_name": thread.source.source_name,
        "community": thread.source.community,
        "source_thread_id": thread.source.source_thread_id,
        "thread_url": thread.source.thread_url,
        "permalink": thread.permalink,
        "author": thread.author.username,
        "question_frame": thread.title,
        "post_body": thread.body,
    }
    payload["top_comments"] = [
        {
            "comment_id": comment.comment_id,
            "author": comment.author.username,
            "score": comment.score,
            "depth": comment.depth,
            "body_excerpt": comment.body[:300],
            "permalink": comment.permalink,
        }
        for comment in top_comments
    ]
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


def build_reclassification_request(
    thread: Thread,
    comment_ids: list[str],
    existing_themes: dict[str, tuple[str, ...]],
    *,
    required: bool = False,
    repair_retries: int = 1,
) -> InferenceRequest:
    comment_index = {c.comment_id: c for c in thread.comments}
    comment_lines = []
    for cid in comment_ids:
        comment = comment_index.get(cid)
        if comment is None:
            continue
        comment_lines.append(f"[id={cid}] {comment.body[:300]}")
    theme_lines = "\n".join(
        f"  {key}: {', '.join(keywords[:8])}" for key, keywords in existing_themes.items()
    )
    return InferenceRequest(
        task=InferenceTask.CATCH_ALL_RECLASSIFICATION,
        messages=[
            InferenceMessage(
                role="system",
                content=(
                    "You classify discussion comments into thematic categories. "
                    "For each comment, assign it to the most fitting existing theme "
                    "OR propose a new theme name (lowercase_snake_case) if none fit. "
                    "Return only valid JSON with key classifications. "
                    "classifications is a list of objects with "
                    "comment_id, theme, confidence (0.0-1.0). "
                    "Assign general_feedback only when the comment "
                    "genuinely has no thematic content. "
                    "Do not include markdown fences."
                ),
            ),
            InferenceMessage(
                role="user",
                content=(
                    f"Thread title: {thread.title}\n\n"
                    f"Existing themes:\n{theme_lines}\n\n"
                    f"Unclassified comments:\n"
                    + "\n---\n".join(comment_lines)
                    + "\n\nClassify each comment."
                ),
            ),
        ],
        required=required,
        repair_retries=repair_retries,
        repair_instruction=(
            "Return only valid JSON with key classifications. "
            "Each item must have comment_id (string), theme (lowercase_snake_case string), "
            "and confidence (float 0.0-1.0)."
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
