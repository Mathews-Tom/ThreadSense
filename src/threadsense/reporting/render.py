from __future__ import annotations

import json
from html import escape

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
        f"- Priority: `{report.executive_summary.priority}`",
        f"- Confidence: `{report.executive_summary.confidence:.2f}`",
        f"- Why Now: {report.executive_summary.why_now}",
        f"- Recommended Owner: `{report.executive_summary.recommended_owner}`",
        f"- Action Type: `{report.executive_summary.action_type}`",
        f"- Expected Outcome: {report.executive_summary.expected_outcome}",
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
    lines.extend(_render_metadata_section(report))
    return "\n".join(lines).strip() + "\n"


def _render_metadata_section(report: ThreadReport) -> list[str]:
    general_feedback_ratio = _compute_general_feedback_ratio(report)
    lines = [
        "",
        "## Metadata",
        "",
        f"- Domain: `{report.provenance.report_version}`",
        f"- Summary Provider: `{report.provenance.summary_provider}`",
        f"- General Feedback Ratio: `{general_feedback_ratio:.0%}`",
        f"- Engine: `{report.provenance.report_version}`",
    ]
    return lines


def _compute_general_feedback_ratio(report: ThreadReport) -> float:
    total = sum(f.comment_count for f in report.findings)
    if total == 0:
        return 0.0
    feedback = sum(
        f.comment_count for f in report.findings if f.theme_key.startswith("general_feedback")
    )
    return feedback / total


def render_report_html(report: ThreadReport) -> str:
    finding_cards = []
    for finding in report.findings:
        quotes = "".join(
            (
                f'<li><a href="{escape(quote.permalink)}">{escape(quote.comment_id)}</a> '
                f"<strong>{escape(quote.author)}</strong>: {escape(quote.body_excerpt)}</li>"
            )
            for quote in finding.quotes
        )
        finding_cards.append(
            f"""
            <details class="finding-card">
              <summary>
                {escape(finding.theme_label.title())}
                <span class="severity">{escape(finding.severity)}</span>
              </summary>
              <p><strong>Comments:</strong> {finding.comment_count}</p>
              <p>
                <strong>Evidence IDs:</strong>
                {escape(", ".join(finding.evidence_comment_ids))}
              </p>
              <p>
                <strong>Key Phrases:</strong>
                {escape(", ".join(finding.key_phrases) or "None")}
              </p>
              <ul>{quotes or "<li>No quotes.</li>"}</ul>
            </details>
            """
        )
    caveats = "".join(f"<li>{escape(caveat)}</li>" for caveat in report.caveats) or "<li>None.</li>"
    next_steps = (
        "".join(f"<li>{escape(step)}</li>" for step in report.executive_summary.next_steps)
        or "<li>None.</li>"
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(report.title)}</title>
  <style>
    :root {{
      --bg:#f5f1e8; --panel:#fffaf1; --ink:#1d1d1d; --accent:#8b4513; --muted:#665e52;
    }}
    body {{
      margin:0;
      font-family: Georgia, 'Iowan Old Style', serif;
      background:linear-gradient(180deg,#f0e7d8,#faf5ec);
      color:var(--ink);
    }}
    main {{ max-width:960px; margin:0 auto; padding:32px 20px 60px; }}
    .hero {{
      padding:24px;
      background:rgba(255,250,241,.88);
      border:1px solid #d7c6aa;
      border-radius:18px;
      box-shadow:0 10px 30px rgba(0,0,0,.06);
    }}
    .finding-card {{
      background:var(--panel);
      border:1px solid #d7c6aa;
      border-radius:14px;
      padding:14px 16px;
      margin:14px 0;
    }}
    .severity {{ color:var(--accent); font-weight:700; margin-left:10px; }}
    .grid {{
      display:grid;
      grid-template-columns:repeat(auto-fit,minmax(180px,1fr));
      gap:12px;
      margin:20px 0;
    }}
    .metric {{
      background:var(--panel);
      border:1px solid #d7c6aa;
      border-radius:12px;
      padding:12px;
    }}
    ul {{ padding-left:20px; }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <h1>{escape(report.title)}</h1>
      <p><strong>{escape(report.executive_summary.headline)}</strong></p>
      <p>{escape(report.executive_summary.summary)}</p>
      <div class="grid">
        <div class="metric">
          <strong>Priority</strong><br>{escape(report.executive_summary.priority)}
        </div>
        <div class="metric">
          <strong>Confidence</strong><br>{report.executive_summary.confidence:.2f}
        </div>
        <div class="metric">
          <strong>Owner</strong><br>{escape(report.executive_summary.recommended_owner)}
        </div>
        <div class="metric">
          <strong>Action</strong><br>{escape(report.executive_summary.action_type)}
        </div>
        <div class="metric">
          <strong>Top Phrases</strong><br>{escape(", ".join(report.top_phrases) or "None")}
        </div>
        <div class="metric">
          <strong>Reply Chains</strong><br>{report.conversation_structure.reply_chain_count}
        </div>
        <div class="metric">
          <strong>Controversy</strong><br>{report.conversation_structure.controversy_count}
        </div>
        <div class="metric">
          <strong>Consensus</strong><br>{report.conversation_structure.consensus_count}
        </div>
      </div>
      <p><strong>Why Now:</strong> {escape(report.executive_summary.why_now)}</p>
      <p><strong>Expected Outcome:</strong> {escape(report.executive_summary.expected_outcome)}</p>
    </section>
    <section><h2>Next Steps</h2><ul>{next_steps}</ul></section>
    <section><h2>Findings</h2>{"".join(finding_cards)}</section>
    <section><h2>Caveats</h2><ul>{caveats}</ul></section>
  </main>
</body>
</html>
"""
