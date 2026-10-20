"""Offline alert journey, using the monitoring smoke's recorded CLI subprocess harness."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from smoke_annotations import check, file_sha256


def run_alerts(scenario: Any) -> None:
    """Exercise persisted comparisons, not a mocked alert presenter."""
    store = scenario.journey
    scenario.create_watch(store, "alerts-create")
    code, missing = scenario.watch(store, "alerts", label="alerts-before-sweep")
    check(code == 0 and missing["status"] == "unavailable", f"no sweep: {missing}")
    check(missing["report_id"] is None and not missing["alert_ids"], "invented initial evidence")

    reports: dict[str, dict[str, Any]] = {}
    hashes: dict[str, str] = {}
    for phase in (
        "initial",
        "unchanged",
        "score_only",
        "search_omits_known",
        "edit",
        "partial",
        "reappear",
    ):
        code, payload = scenario.watch(store, "run", label=f"alerts-{phase}", phase=phase)
        check(code == (1 if phase == "partial" else 0), f"{phase}: {payload}")
        result = payload["alerts"]
        reports[phase] = result
        check(result["artifact_type"] == "watch_alert_result", f"{phase}: missing alerts")
        for key in ("json_path", "markdown_path"):
            path = Path(result[key])
            check(path.is_relative_to(store.state_path.parent), f"{phase}: escaped artifact")
            check(path.is_file(), f"{phase}: absent {key}")
            hashes[str(path)] = file_sha256(path)
        report = json.loads(Path(result["json_path"]).read_text(encoding="utf-8"))
        check(report["artifact_type"] == "watch_alert_report", f"{phase}: wrong durable report")
        check(report["report_id"] == result["report_id"], f"{phase}: different persisted id")
        check(result["refusal_reasons"], f"{phase}: bounded source incorrectly permits trends")
        if phase == "initial":
            check(
                result["status"] == "baseline" and not result["alert_ids"], "baseline became demand"
            )
        elif phase in ("unchanged", "score_only", "partial", "backlog"):
            check(not result["new_alert_ids"], f"{phase}: repeated or absent evidence alerted")
        elif phase == "search_omits_known":
            citations = [citation for event in result["alerts"] for citation in event["evidence"]]
            check(
                any(citation["comment_id"] == "reddit:ka4" for citation in citations),
                "new reply not cited",
            )
            check(result["new_alert_ids"], "new reply produced no alert")
        for event in result["alerts"]:
            check(event["claims_are_verified"] is False, "claim presented as verified")
            check(event["evidence"], "uncited alert")
            for citation in event["evidence"]:
                check(citation["permalink"] and citation["text_sha256"], "untraceable citation")
                check(citation["first_observed_at_utc"] is not None, "missing observation time")
        code, restarted = scenario.watch(store, "alerts", label=f"alerts-{phase}-restart")
        check(code == 0, f"{phase}: {restarted}")
        check(
            restarted["resumed"] and not restarted["new_alert_ids"],
            "restart delivered events again",
        )
        check(restarted["report_id"] == result["report_id"], "restart changed report id")
        check(restarted["alert_ids"] == result["alert_ids"], "restart changed event identities")
        scenario.record(
            f"alerts-{phase}",
            status=result["status"],
            report_id=result["report_id"],
            alert_ids=result["alert_ids"],
            refusal_reasons=result["refusal_reasons"],
        )

    profile = Path(__file__).resolve().parents[1] / "tests/fixtures/watch/incremental/profile.toml"
    revised = scenario.work_dir / "alert-cohort.toml"
    revised.write_text(
        profile.read_text(encoding="utf-8").replace(
            "presented_findings = 3", "presented_findings = 4"
        ),
        encoding="utf-8",
    )
    scenario.create_watch(store, "alerts-revise-cohort", revised)
    code, changed = scenario.watch(store, "run", label="alerts-new-cohort", phase="backlog")
    check(code == 0, f"new cohort: {changed}")
    cohort = changed["alerts"]
    check(cohort["cohort_id"] != reports["initial"]["cohort_id"], "limits did not bind cohort")
    check(
        cohort["status"] == "baseline" and not cohort["new_alert_ids"],
        "cohort rebranded old demand",
    )
    code, digest = scenario.watch(store, "digest", label="alerts-digest-reuse")
    check(
        code == 0 and digest["alerts"]["report_id"] == cohort["report_id"],
        "digest changed comparison",
    )
    check(not digest["alerts"]["new_alert_ids"], "digest redelivered alerts")
    check(
        all(file_sha256(Path(path)) == digest for path, digest in hashes.items()),
        "history rewritten",
    )
    scenario.record(
        "alerts-cohort-baseline",
        cohort_id=cohort["cohort_id"],
        report_id=cohort["report_id"],
        historical_artifacts_unchanged=True,
    )
