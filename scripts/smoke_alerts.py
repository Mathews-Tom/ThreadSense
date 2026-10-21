"""Offline alert journey, using the monitoring smoke's recorded CLI subprocess harness."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from smoke_annotations import check, file_sha256

from threadsense.monitoring.alert_budget import AlertBudget
from threadsense.monitoring.alert_reader import read_alert_page


def _collection(store: Any, result: dict[str, Any], collection: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    cursor = None
    for _ in range(4096):
        page = read_alert_page(
            root=store.monitoring_root,
            report_path=Path(result["json_path"]),
            collection=collection,
            cursor=cursor,
        )
        check(page["status"] != "resource_refused", f"reader refused: {page}")
        items.extend(page["items"])
        if page["complete"]:
            return items
        check(page["cursor"] is not None, "reader lost its continuation")
        cursor = page["cursor"]
    raise AssertionError("reader did not finish bounded continuation")


def _drain(
    scenario: Any,
    store: Any,
    result: dict[str, Any],
    *,
    label: str,
    config: Path | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """The acceptance driver explicitly invokes separate CLI processes for backlog."""
    invocations: list[dict[str, Any]] = []
    delivered: set[str] = set()
    extra = () if config is None else ("--config", str(config))
    for index in range(4096):
        invocations.append(result)
        check(delivered.isdisjoint(result["new_alert_ids"]), "durable event delivered twice")
        delivered.update(result["new_alert_ids"])
        accounting = result["accounting"]
        for dimension, used in accounting["used"].items():
            check(0 <= used <= accounting["limits"][dimension], f"unbounded {dimension}")
        check(result["status"] != "resource_refused", f"unexpected refusal: {result}")
        if result["status"] != "backlog":
            events = _collection(store, result, "changes")
            event_ids = [event["alert_id"] for event in events]
            check(len(event_ids) == len(set(event_ids)), "duplicate durable event IDs")
            check(delivered == set(event_ids), "continuation lost durable event delivery")
            return {
                **result,
                "alerts": events,
                "alert_ids": event_ids,
                "new_alert_ids": sorted(delivered),
            }, invocations
        check(bool(result.get("continuation")), "backlog lacks durable continuation")
        code, result = scenario.watch(
            store,
            "alerts",
            *extra,
            label=f"{label}-continuation-{index:04d}",
        )
        check(code == 0, f"continuation failed: {result}")
    raise AssertionError("alert target did not finish bounded continuation")


def run_alerts(scenario: Any) -> None:
    """Exercise persisted comparisons, not a mocked alert presenter."""
    store = scenario.journey
    scenario.create_watch(store, "alerts-create")
    code, missing = scenario.watch(store, "alerts", label="alerts-before-sweep")
    check(code == 0 and missing["status"] == "unavailable", f"no sweep: {missing}")
    check(missing["report_id"] is None and not missing["alert_ids"], "invented initial evidence")

    reports: dict[str, dict[str, Any]] = {}
    hashes: dict[str, str] = {}
    durable_ids: set[str] = set()
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
        result, _ = _drain(scenario, store, payload["alerts"], label=f"alerts-{phase}")
        check(durable_ids.isdisjoint(result["alert_ids"]), "watch-global event IDs repeated")
        durable_ids.update(result["alert_ids"])
        reports[phase] = result
        check(result["artifact_type"] == "watch_alert_result", f"{phase}: missing alerts")
        for key in ("json_path", "markdown_path"):
            path = Path(result[key])
            check(path.is_relative_to(store.state_path.parent), f"{phase}: escaped artifact")
            check(path.is_file(), f"{phase}: absent {key}")
            hashes.setdefault(str(path), file_sha256(path))
        report = json.loads(Path(result["json_path"]).read_text(encoding="utf-8"))
        check(report["artifact_type"] == "watch_alert_report", f"{phase}: wrong durable report")
        check(report["report_id"] == result["report_id"], f"{phase}: different persisted id")
        check(result["refusal_reasons"], f"{phase}: bounded source incorrectly permits trends")
        if phase == "initial":
            check(
                result["status"] == "baseline" and not result["alert_ids"], "baseline became demand"
            )
        elif phase in ("unchanged", "score_only", "partial"):
            check(not result["new_alert_ids"], f"{phase}: repeated or absent evidence alerted")
        elif phase in ("search_omits_known", "edit", "reappear"):
            expected = {
                "search_omits_known": "reddit:ka4",
                "edit": "reddit:kb2",
                "reappear": "reddit:ka5",
            }[phase]
            citations = [citation for event in result["alerts"] for citation in event["evidence"]]
            check(
                any(item["comment_id"] == expected for item in citations),
                f"{phase}: missing citation",
            )
            check(result["new_alert_ids"], f"{phase}: changed evidence produced no alert")
            if phase == "reappear":
                check(
                    not any(item["comment_id"] == "reddit:ka2" for item in citations),
                    "known reappearing evidence alerted",
                )
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
    cohort, _ = _drain(scenario, store, changed["alerts"], label="alerts-new-cohort")
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
    _resource_proof(scenario)
    for boundary in ("before", "after"):
        _crash_proof(scenario, boundary)


def _resource_proof(scenario: Any) -> None:
    """Leave actual subprocess accounting/refusal/recovery evidence beside the journey."""
    store = scenario.new_store("alert-resources")
    scenario.create_watch(store, "alerts-resource-create")
    config = scenario.work_dir / "alerts-low-budget.toml"
    config.write_text("[alerts]\nsource_rows = 32\nderivations = 16\n", encoding="utf-8")
    code, first = scenario.watch(
        store,
        "run",
        "--config",
        str(config),
        label="alerts-resource-start",
        phase="initial",
    )
    check(code == 0 and first["alerts"]["status"] == "backlog", f"resource start: {first}")
    baseline, invocations = _drain(
        scenario,
        store,
        first["alerts"],
        label="alerts-resource-baseline",
        config=config,
    )
    check(len(invocations) >= 8, "low-budget proof did not exercise many fresh continuations")
    check(baseline["status"] == "baseline", "bounded bootstrap invented a comparison")
    original = {
        str(path): file_sha256(path)
        for path in store.monitoring_root.rglob("*")
        if path.is_file() and path.suffix in (".json", ".md")
    }
    code, next_window = scenario.watch(
        store,
        "run",
        "--config",
        str(config),
        label="alerts-resource-change",
        phase="search_omits_known",
    )
    check(code == 0, f"resource changed sweep: {next_window}")
    changed, continuations = _drain(
        scenario,
        store,
        next_window["alerts"],
        label="alerts-resource-change",
        config=config,
    )
    check(changed["new_alert_ids"], "bounded changed window lost additions")
    check(changed["cohort_id"] == baseline["cohort_id"], "resource limits altered cohort")
    invocations.extend(continuations)
    for path in store.monitoring_root.rglob("*"):
        if path.is_file() and path.suffix in (".json", ".md"):
            original.setdefault(str(path), file_sha256(path))

    oversized = scenario.work_dir / "alerts-oversized-unit.toml"
    oversized.write_text("[alerts]\ninput_bytes = 1\n", encoding="utf-8")
    code, refused = scenario.watch(
        store,
        "alerts",
        "--config",
        str(oversized),
        label="alerts-resource-refused",
    )
    check(code == 0 and refused["status"] == "resource_refused", f"oversized unit: {refused}")
    check(
        refused["resource"]["required"] > refused["resource"]["configured"],
        "refusal did not identify an indivisible full-envelope overflow",
    )
    check(refused["new_alert_ids"] == [], "refused reuse redelivered events")
    reader_refusal = read_alert_page(
        root=store.monitoring_root,
        report_path=Path(changed["json_path"]),
        budget=AlertBudget(input_bytes=1),
    )
    check(reader_refusal["status"] == "resource_refused", "reader bypassed byte admission")
    check(not reader_refusal["complete"] and not reader_refusal["items"], "false empty success")
    code, recovered = scenario.watch(
        store,
        "alerts",
        "--config",
        str(config),
        label="alerts-resource-recovered",
    )
    check(code == 0 and recovered["report_id"] == changed["report_id"], "recovery changed target")
    check(not recovered["new_alert_ids"], "recovery repeated durable event delivery")
    check(
        all(file_sha256(Path(path)) == digest for path, digest in original.items()),
        "bounded continuation rewrote retained source/history",
    )
    report = json.loads(Path(changed["json_path"]).read_bytes())
    evidence = _collection(store, changed, "evidence")
    events = _collection(store, changed, "changes")
    check(report["schema_version"] == 2, "resource proof did not publish schema2")
    check(report["evidence_count"] == len(evidence), "cumulative evidence count lost a page")
    check(report["event_count"] == len(events), "target-specific change count lost a page")
    proof = {
        "artifact_type": "alerts_resource_acceptance",
        "schema_version": 1,
        "invocations": invocations,
        "refused": refused,
        "reader_refused": reader_refusal,
        "recovered": recovered,
        "historical_sha256": original,
        "evidence_count": len(evidence),
        "event_ids": [event["alert_id"] for event in events],
    }
    path = scenario.work_dir / "alerts-resource-proof.json"
    path.write_text(json.dumps(proof, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    scenario.record(
        "alerts-resource-proof",
        path=str(path),
        invocations=len(invocations),
        historical_artifacts_unchanged=True,
        event_ids=proof["event_ids"],
    )


_CRASH_CHILD = """
import os, sys
from threadsense import cli
from threadsense.monitoring import alert_artifacts

boundary, *argv = sys.argv[1:]
original = alert_artifacts.publish_segment

def refuse_source(*args, **kwargs):
    raise RuntimeError("alert restart must not acquire a source")

def publish(*args, **kwargs):
    if kwargs["chain"] != "change":
        return original(*args, **kwargs)
    if boundary == "before":
        os._exit(75)
    original(*args, **kwargs)
    os._exit(75)

cli.build_reddit_connector = refuse_source
alert_artifacts.publish_segment = publish
sys.exit(cli.main(argv))
"""


def _crash_proof(scenario: Any, boundary: str) -> None:
    """Kill a real CLI interpreter on either side of durable event-segment bytes."""
    store = scenario.new_store(f"alert-crash-{boundary}")
    scenario.create_watch(store, f"alerts-crash-{boundary}-create")
    code, first = scenario.watch(
        store,
        "run",
        label=f"alerts-crash-{boundary}-baseline-start",
        phase="initial",
    )
    check(code == 0, f"crash baseline: {first}")
    baseline, _ = _drain(
        scenario,
        store,
        first["alerts"],
        label=f"alerts-crash-{boundary}-baseline",
    )
    old = {
        str(path): file_sha256(path)
        for path in store.monitoring_root.rglob("*")
        if path.is_file() and path.suffix in (".json", ".md")
    }
    config = scenario.work_dir / f"alerts-crash-{boundary}-hold.toml"
    config.write_text("[alerts]\nartifact_bytes = 1\n", encoding="utf-8")
    code, changed = scenario.watch(
        store,
        "run",
        "--config",
        str(config),
        phase="search_omits_known",
        label=f"alerts-crash-{boundary}-change-start",
    )
    check(code == 0, f"crash target creation: {changed}")
    check(changed["alerts"]["status"] in ("backlog", "resource_refused"), "unheld target")
    argv = (
        "--output-format",
        "json",
        "watch",
        "alerts",
        "--id",
        baseline["watch_id"],
        "--lease-seconds",
        "1",
    )
    for index in range(4096):
        completed = subprocess.run(
            [sys.executable, "-c", _CRASH_CHILD, boundary, *argv],
            cwd=store.cwd,
            env={**scenario.base_env, "THREADSENSE_STORAGE_ROOT": str(store.root)},
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
        scenario.persist(
            f"alerts-crash-{boundary}-{index:04d}",
            {
                "argv": list(argv),
                "boundary": boundary,
                "exit_code": completed.returncode,
                "stdout": completed.stdout,
                "stderr": completed.stderr,
            },
        )
        if completed.returncode == 75:
            break
        check(completed.returncode == 0, f"crash driver failed: {completed.stderr}")
        continuation = json.loads(completed.stdout)
        check(continuation["status"] == "backlog", f"crash seam not reached: {continuation}")
        check(not continuation["new_alert_ids"], "event committed before first crash seam")
    else:
        raise AssertionError("event-segment crash seam was never reached")
    check(not completed.stdout.strip(), "killed interpreter claimed command completion")
    retained = {
        str(path): file_sha256(path)
        for path in store.monitoring_root.rglob("*")
        if path.is_file() and path.suffix in (".json", ".md")
    }
    time.sleep(1.1)  # the dead interpreter's actual one-second exclusive claim must expire
    code, restarted = scenario.watch(store, "alerts", label=f"alerts-crash-{boundary}-restart")
    check(code == 0, f"crash recovery failed: {restarted}")
    result, invocations = _drain(
        scenario,
        store,
        restarted,
        label=f"alerts-crash-{boundary}-recovered",
    )
    events = _collection(store, result, "changes")
    ids = [event["alert_id"] for event in events]
    check(bool(ids) and len(ids) == len(set(ids)), "crash lost or repeated durable event IDs")
    check(
        any(item["comment_id"] == "reddit:ka4" for event in events for item in event["evidence"]),
        "crash lost changed source evidence",
    )
    code, reused = scenario.watch(store, "alerts", label=f"alerts-crash-{boundary}-reuse")
    check(code == 0 and reused["report_id"] == result["report_id"], "recovery changed report ID")
    check(not reused["new_alert_ids"], "restart redelivered events")
    check(
        all(file_sha256(Path(path)) == digest for path, digest in {**old, **retained}.items()),
        "recovery overwrote old or staged durable artifact bytes",
    )
    proof_path = scenario.work_dir / f"alerts-crash-{boundary}-proof.json"
    proof_path.write_text(
        json.dumps(
            {
                "boundary": boundary,
                "exit_code": completed.returncode,
                "report_id": result["report_id"],
                "target_id": result["target_id"],
                "event_ids": ids,
                "retained_sha256": retained,
                "invocations": invocations,
            },
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    scenario.record(
        f"alerts-crash-{boundary}",
        proof_path=str(proof_path),
        event_ids=ids,
        staged_bytes_unchanged=True,
    )
