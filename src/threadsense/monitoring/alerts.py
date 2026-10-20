"""Present durable, comparable evidence alerts without fetching or analyzing sources."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from threadsense.config import AppConfig
from threadsense.errors import MonitoringStateError, RunClaimError
from threadsense.monitoring.alert_records import PendingAlertReport
from threadsense.monitoring.issues import issue_policy
from threadsense.monitoring.state import MonitoringStore, RunClaim
from threadsense.monitoring.sweep import _error_payload, _publish_bytes, _verify_stored_artifact
from threadsense.observability import TraceContext, emit_log


def render_alert_json(payload: dict[str, Any]) -> bytes:
    """Render only frozen inputs, so interrupted publication reproduces identical bytes."""
    return (
        json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    ).encode("utf-8")


def _md(value: Any) -> str:
    text = "".join(
        f"\\u{ord(char):04x}" if ord(char) < 32 or ord(char) == 127 else char for char in str(value)
    )
    return "".join("\\" + char if char in "\\`*_[]<>#|!" else char for char in text)


def _json_block(value: Any) -> list[str]:
    # Repository/source text cannot terminate a fence or inject active HTML.
    text = json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False)
    for char in ("`", "<", ">", "&"):
        text = text.replace(char, f"\\u{ord(char):04x}")
    return ["```json", text, "```"]


def render_alert_markdown(payload: dict[str, Any]) -> bytes:
    lines = [
        f"# Watch evidence alerts: {_md(payload['watch_id'])}",
        "",
        f"- Report: {_md(payload['report_id'])}",
        f"- Status: {_md(payload['status'])}",
        f"- Cohort: {_md(payload['cohort_id'])}",
        f"- Window: {_md(payload['window_id'])}",
        f"- Baseline window: {_md(payload['baseline_window_id'])}",
        "",
        "These are bounded, observed evidence changes, not population demand or trend estimates.",
        "Source creation is distinct from first observation and edit observation; "
        "source edit times are unknown.",
        "Copies are not independent people. "
        "Reproduction and resolution are unverified source claims.",
        "Absence does not establish deletion, resolution, or reduced demand.",
        "",
        "## Durable baseline",
        "",
        *_json_block(payload["baseline"]),
        "",
        "## Exact profile, collection, policies and feedback basis",
        "",
        *_json_block(payload["basis"]),
        "",
        "## Window and coverage",
        "",
        *_json_block(payload["window"]),
        *_json_block(payload["coverage"]),
        "",
        "## Bounded denominators (not population denominators)",
        "",
        *_json_block(payload["bounded_denominators"]),
        "",
        "## Refusals",
        "",
    ]
    lines.extend(f"- {_md(reason)}" for reason in payload["refusal_reasons"])
    lines.extend(["", "## Suppressions", ""])
    lines.extend(f"- {_md(reason)}" for reason in payload["suppression_reasons"])
    lines.extend(["", "## Cited changes", ""])
    if not payload["alerts"]:
        lines.append(
            "No new alert events. A baseline establishes prior evidence, not increased demand."
        )
    for event in payload["alerts"]:
        lines.extend(
            [
                f"### {_md(event['kind'])}: {_md(event['alert_id'])}",
                "",
                f"- Interpretation: {_md(event['interpretation'])}",
                "- Claims verified: false",
                "",
            ]
        )
        for citation in event["evidence"]:
            lines.extend(_citation_lines(citation))
    lines.extend(["", "## Baseline and comparison evidence citations", ""])
    for citation in payload["evidence"]:
        lines.extend(_citation_lines(citation))
    return ("\n".join(lines) + "\n").encode("utf-8")


def _citation_lines(citation: dict[str, Any]) -> list[str]:
    return [
        f"- Source report: {_md(citation['source_report_id'])}; "
        f"comment: {_md(citation['comment_id'])}",
        f"  - Community: {_md(citation['community'])}; lineage: {_md(citation['lineage'])}",
        f"  - Citation: {_md(citation['permalink'])}",
        f"  - Source creation UTC: {_md(citation['source_created_utc'])}",
        f"  - Source thread creation UTC: {_md(citation['source_thread_created_utc'])}",
        f"  - First observation UTC: {_md(citation['first_observed_at_utc'])}",
        f"  - Edit observation UTC: {_md(citation['edit_observed_at_utc'])}; "
        "source edit UTC: unknown",
        f"  - Evidence hash: {_md(citation['text_sha256'])}",
        f"  - Entry: {_md(citation['entry_id'])}; revision: {_md(citation['revision_id'])}",
        f"  - Snapshot: {_md(citation['snapshot_path'])} ({_md(citation['snapshot_sha256'])})",
        f"  - Analysis: {_md(citation['analysis_path'])} ({_md(citation['analysis_sha256'])})",
        f"  - Excerpt: {_md(citation['excerpt'])}",
    ]


def _artifact_path(root: Path, relative: str) -> Path:
    path = root / relative
    if not path.resolve().is_relative_to(root.resolve()):
        raise MonitoringStateError(
            "alert artifact is outside monitoring storage",
            details={"reason": "artifact_outside_state", "path": relative},
        )
    return path


def _publish_report(store: MonitoringStore, claim: RunClaim, report: PendingAlertReport) -> None:
    root = store.monitoring_root().absolute()
    verified: set[tuple[str, str]] = set()
    for citation in report.payload["evidence"]:
        for path_key, hash_key in (
            ("snapshot_path", "snapshot_sha256"),
            ("analysis_path", "analysis_sha256"),
        ):
            key = (str(citation[path_key]), str(citation[hash_key]))
            if key not in verified:
                _verify_stored_artifact(_artifact_path(root, key[0]), key[1])
                verified.add(key)
    with store.fenced_artifact_write(claim):
        json_sha256 = _publish_bytes(
            _artifact_path(root, report.json_path), render_alert_json(report.payload)
        )
    with store.fenced_artifact_write(claim):
        markdown_sha256 = _publish_bytes(
            _artifact_path(root, report.markdown_path), render_alert_markdown(report.payload)
        )
    store.complete_alert_report(
        claim, str(report.payload["report_id"]), json_sha256, markdown_sha256
    )


def _result(
    store: MonitoringStore, watch_id: str, report: PendingAlertReport | None
) -> dict[str, Any]:
    if report is None:
        return {
            "artifact_type": "watch_alert_result",
            "watch_id": watch_id,
            "status": "unavailable",
            "report_id": None,
            "cohort_id": None,
            "window_id": None,
            "baseline_window_id": None,
            "alert_ids": [],
            "alerts": [],
            "new_alert_ids": [],
            "suppression_reasons": [],
            "refusal_reasons": ["no_durable_sweep"],
            "json_path": None,
            "markdown_path": None,
            "resumed": False,
        }
    payload = report.payload
    root = store.monitoring_root().absolute()
    return {
        **{
            key: payload[key]
            for key in (
                "watch_id",
                "status",
                "report_id",
                "cohort_id",
                "window_id",
                "baseline_window_id",
                "alert_ids",
                "alerts",
                "suppression_reasons",
                "refusal_reasons",
            )
        },
        "artifact_type": "watch_alert_result",
        "json_path": str(root / report.json_path),
        "markdown_path": str(root / report.markdown_path),
        "resumed": report.resumed,
        "new_alert_ids": [] if report.resumed else list(payload["alert_ids"]),
    }


def present_watch_alerts(
    *,
    config: AppConfig,
    store: MonitoringStore,
    watch_id: str,
    lease_seconds: int,
    logger: logging.Logger,
    trace: TraceContext,
) -> dict[str, Any]:
    """Compare the latest durable sweep under the existing exclusive watch run fence.

    Re-querying a window reuses its fixed events, not a repeated delivery. A recovered
    pending older window is completed before the latest sweep is presented.
    """
    claim = store.claim_run(watch_id, lease_seconds=lease_seconds)
    policy = issue_policy(config.issues)
    basis = {
        "issue_policy": {"policy_sha256": policy.sha256(), "policy": policy.to_dict()},
        "invocation_collection_basis": {
            "source": "reddit",
            "source_policy": config.source_policy.model_dump(mode="json"),
            "reddit": config.reddit.model_dump(mode="json"),
            "analysis": config.analysis.model_dump(mode="json"),
        },
    }
    try:
        report = store.begin_alert_report(claim, basis)
        while report is not None:
            _publish_report(store, claim, report)
            next_report = store.begin_alert_report(claim, basis)
            if (
                next_report is None
                or next_report.payload["report_id"] == report.payload["report_id"]
            ):
                break
            report = next_report
        result = _result(store, watch_id, report)
    except RunClaimError:
        raise
    except Exception as error:
        claim_error: RunClaimError | None = None
        try:
            store.finalize_run(claim.run_id, claim.owner_token, "failed")
        except RunClaimError as failure:
            claim_error = failure
        emit_log(
            logger,
            "watch_alerts_failed",
            trace,
            level=logging.ERROR,
            watch_run_id=claim.run_id,
            error=_error_payload(error),
            claim_error=None if claim_error is None else claim_error.to_dict(),
        )
        raise
    store.finalize_run(claim.run_id, claim.owner_token, "succeeded")
    emit_log(
        logger,
        "watch_alerts_completed",
        trace,
        watch_id=watch_id,
        watch_run_id=claim.run_id,
        report_id=result["report_id"],
        status=result["status"],
        resumed=result["resumed"],
    )
    return result
