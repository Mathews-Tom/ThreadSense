from __future__ import annotations

from threadsense.inference.contracts import InferenceResponse
from threadsense.models.corpus import CorpusAnalysis


def render_corpus_markdown(
    corpus: CorpusAnalysis,
    synthesis_response: InferenceResponse | None = None,
) -> str:
    lines = [
        f"# {corpus.name}",
        "",
        "## Overview",
        "",
        f"- Corpus ID: `{corpus.corpus_id}`",
        f"- Domain: `{corpus.domain.value}`",
        f"- Threads: `{corpus.thread_count}`",
        f"- Total Comments: `{corpus.total_comments}`",
    ]
    if synthesis_response is not None:
        output = synthesis_response.output
        lines.extend(
            [
                "",
                "## Synthesis",
                "",
                f"**Headline:** {output['headline']}",
                "",
                "### Key Patterns",
            ]
        )
        for pattern in output["key_patterns"]:
            lines.append(f"- {pattern}")
        lines.extend(["", "### Recommended Actions"])
        for action in output["recommended_actions"]:
            lines.append(f"- {action}")
        lines.extend(
            [
                "",
                "### Confidence Note",
                "",
                str(output["confidence_note"]),
                "",
                f"Cited Threads: {', '.join(output['cited_thread_ids']) or 'None'}",
            ]
        )

    lines.extend(["", "## Cross-Thread Findings", ""])
    if not corpus.cross_thread_findings:
        lines.append("- None.")
    for finding in corpus.cross_thread_findings:
        lines.extend(
            [
                f"### {finding.theme_label.title()}",
                "",
                f"- Severity: `{finding.severity}`",
                f"- Threads: `{finding.thread_count}`",
                f"- Comment Count: `{finding.total_comment_count}`",
                "",
                "#### Evidence",
            ]
        )
        if finding.top_evidence:
            for evidence in finding.top_evidence:
                lines.append(
                    f"- `{evidence.thread_id}` {evidence.thread_title}: "
                    f"`{evidence.finding_severity}` severity, "
                    f"`{evidence.comment_count}` comments, "
                    f"[{evidence.top_quote.comment_id}]({evidence.top_quote.permalink}) "
                    f"{evidence.top_quote.body_excerpt}"
                )
        else:
            lines.append("- No representative quote available.")
        lines.append("")

    lines.extend(["## Temporal Trends", ""])
    if not corpus.temporal_trends:
        lines.append("- None.")
    else:
        for trend in corpus.temporal_trends:
            distribution = ", ".join(
                f"{severity}={count}" for severity, count in trend.severity_distribution.items()
            )
            lines.append(
                f"- `{trend.period}` `{trend.theme_key}`: "
                f"`{trend.thread_count}` threads, {distribution}"
            )
    return "\n".join(lines).strip() + "\n"
