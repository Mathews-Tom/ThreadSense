from __future__ import annotations

import json

from threadsense.models.report import ThreadReport


def render_report_json(report: ThreadReport) -> str:
    return json.dumps(report.to_dict(), indent=2)


def render_report_markdown(report: ThreadReport) -> str:
    lines = [
        f"# {report.title}",
        "",
        "## Executive Summary",
        "",
        f"**Headline:** {report.executive_summary.headline}",
        "",
        report.executive_summary.summary,
        "",
        "### Next Steps",
    ]
    for step in report.executive_summary.next_steps:
        lines.append(f"- {step}")
    lines.extend(["", "## Conversation Structure", ""])
    lines.append(f"- Max Depth: `{report.conversation_structure.max_depth}`")
    lines.append(f"- Top-Level Comments: `{report.conversation_structure.top_level_count}`")
    lines.append(
        f"- Reply Chains (3+ comments): `{report.conversation_structure.reply_chain_count}`"
    )
    lines.append(f"- Longest Chain Length: `{report.conversation_structure.longest_chain_length}`")
    lines.append(f"- Controversy Count: `{report.conversation_structure.controversy_count}`")
    lines.append(f"- Consensus Count: `{report.conversation_structure.consensus_count}`")
    lines.append(f"- Monologue Count: `{report.conversation_structure.monologue_count}`")
    lines.extend(["", "### Top Engagement Subtrees"])
    if report.conversation_structure.top_engagement_subtrees:
        for subtree in report.conversation_structure.top_engagement_subtrees:
            lines.append(
                "- "
                f"`{subtree.root_comment_id}` by `{subtree.root_author}`: "
                f"`{subtree.subtree_size}` comments, "
                f"depth `{subtree.max_depth_below}`, "
                f"engagement `{subtree.engagement_score}`"
            )
    else:
        lines.append("- None.")
    lines.extend(["", "## Findings", ""])
    for finding in report.findings:
        key_phrases = ", ".join(finding.key_phrases) if finding.key_phrases else "None"
        lines.extend(
            [
                f"### {finding.theme_label.title()}",
                "",
                f"- Severity: `{finding.severity}`",
                f"- Comment Count: `{finding.comment_count}`",
                f"- Key Phrases: {key_phrases}",
                f"- Evidence IDs: {', '.join(finding.evidence_comment_ids)}",
                "",
                "#### Representative Quotes",
            ]
        )
        for quote in finding.quotes:
            lines.append(
                f"- [{quote.comment_id}]({quote.permalink}) `{quote.author}`: {quote.body_excerpt}"
            )
        lines.append("")
    lines.extend(["## Caveats", ""])
    if report.caveats:
        for caveat in report.caveats:
            lines.append(f"- {caveat}")
    else:
        lines.append("- None.")
    lines.extend(["", "## Quality Checks", ""])
    if report.quality_checks:
        for check in report.quality_checks:
            lines.append(f"- `{check.level}` `{check.code}`: {check.message}")
    else:
        lines.append("- No quality issues detected.")
    return "\n".join(lines).strip() + "\n"
