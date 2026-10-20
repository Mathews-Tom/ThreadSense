"""Materialize a digest's ranking from its recorded basis and explain any entry's rank.

A ranking is a pure function of immutable inputs: the recorded profile revision, the
content-addressed policy, preference events up to the recorded sequence, the stored entry
payloads, the stored issue references and the hash-verified analysis artifacts. Explaining
an entry recomputes that ranking and refuses unless it equals the one written in the digest
artifact, so the historical order and reasons are proven rather than restated.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from threadsense.errors import MonitoringStateError
from threadsense.models.analysis import load_analysis_artifact_file
from threadsense.monitoring.ranking import FindingFacts, RankingBasis, rank_entries
from threadsense.monitoring.ranking_records import EntryPresentation
from threadsense.monitoring.state import MonitoringStore
from threadsense.monitoring.sweep import _verify_stored_artifact


def load_finding_facts(
    monitoring_root: Path, entries: Sequence[Mapping[str, Any]]
) -> dict[str, FindingFacts]:
    """Marker and comment counts of each finding entry, from its hash-verified analysis."""
    facts: dict[str, FindingFacts] = {}
    analyses: dict[str, dict[str, FindingFacts]] = {}
    for entry in entries:
        if entry.get("kind") != "finding":
            continue
        relative = str(entry["analysis_path"])
        if relative not in analyses:
            path = monitoring_root / relative
            _verify_stored_artifact(path, str(entry["analysis_sha256"]))
            analysis = load_analysis_artifact_file(path)
            analyses[relative] = {
                finding.theme_key: FindingFacts(
                    issue_marker_count=finding.issue_marker_count,
                    request_marker_count=finding.request_marker_count,
                    comment_count=finding.comment_count,
                )
                for finding in analysis.findings
            }
        theme_key = str(entry["theme_key"])
        if theme_key not in analyses[relative]:
            raise MonitoringStateError(
                "finding entry's theme is missing from its analysis",
                details={
                    "reason": "ranking_input_unreadable",
                    "entry_id": entry["entry_id"],
                    "theme_key": theme_key,
                },
            )
        facts[str(entry["entry_id"])] = analyses[relative][theme_key]
    return facts


def issue_refs(issue_view: Mapping[str, Any] | None) -> dict[str, str]:
    if issue_view is None:
        return {}
    return {str(ref["entry_id"]): str(ref["issue_id"]) for ref in issue_view["entry_refs"]}


def rank_digest(
    store: MonitoringStore,
    watch_id: str,
    entries: Sequence[Mapping[str, Any]],
    issue_view: Mapping[str, Any] | None,
    basis: RankingBasis,
) -> dict[str, Any]:
    inputs = store.ranking_inputs(watch_id, basis)
    facts = load_finding_facts(store.monitoring_root().absolute(), entries)
    return rank_entries(entries, inputs, facts, issue_refs(issue_view))


def _written_document(presentation: EntryPresentation) -> Any:
    if presentation.json_path is None or presentation.json_sha256 is None:
        raise MonitoringStateError(
            "written digest has no recorded artifact",
            details={"reason": "digest_state_unreadable", "digest_id": presentation.digest_id},
        )
    _verify_stored_artifact(presentation.json_path, presentation.json_sha256)
    return json.loads(presentation.json_path.read_text(encoding="utf-8"))


def _recorded_ranking(presentation: EntryPresentation) -> dict[str, Any]:
    document = _written_document(presentation)
    ranking = document.get("ranking") if isinstance(document, dict) else None
    if not isinstance(ranking, dict):
        raise MonitoringStateError(
            "ranked digest artifact has no ranking",
            details={"reason": "digest_state_unreadable", "digest_id": presentation.digest_id},
        )
    return ranking


def _ranked_item(ranking: Mapping[str, Any], entry_id: str) -> dict[str, Any]:
    for item in ranking["order"]:
        if item["entry_id"] == entry_id:
            return dict(item)
    raise MonitoringStateError(
        "entry is missing from its digest ranking",
        details={"reason": "digest_state_unreadable", "entry_id": entry_id},
    )


def _current(
    store: MonitoringStore,
    watch_id: str,
    entry_id: str,
    entries: Sequence[Mapping[str, Any]],
    issue_view: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """How the same entries would rank now; derived on demand and never recorded."""
    basis = store.current_ranking_basis(watch_id)
    ranking = rank_digest(store, watch_id, entries, issue_view, basis)
    return {"recorded": False, "basis": ranking["basis"], **_ranked_item(ranking, entry_id)}


def _require_unranked_artifact(presentation: EntryPresentation) -> None:
    """A written digest without a recorded basis must also have been written without a ranking."""
    if presentation.status != "written":
        return
    document = _written_document(presentation)
    if not isinstance(document, dict) or "ranking" in document:
        raise MonitoringStateError(
            "ranked digest artifact has no recorded ranking basis",
            details={"reason": "ranking_basis_missing", "digest_id": presentation.digest_id},
        )


def explain_digest_entry(
    store: MonitoringStore, watch_id: str, entry_id: str, *, include_current: bool
) -> dict[str, Any]:
    """Explain an entry's rank in the digest that presented (or is presenting) it."""
    presentation = store.entry_presentation(watch_id, entry_id)
    explanation: dict[str, Any] = {"watch_id": watch_id, "entry_id": entry_id}
    if presentation.digest_id is None:
        explanation.update(status="unpresented", digest=None, ranking=None)
        if include_current:
            # Ranked alone, so its rank among the next digest's entries is not known yet.
            current = _current(store, watch_id, entry_id, (presentation.entry,), None)
            del current["rank"]
            explanation["current"] = {**current, "rank_scope": "entry_only"}
        return explanation
    digest = {
        "digest_id": presentation.digest_id,
        "sequence": presentation.sequence,
        "status": presentation.status,
        "json_path": None if presentation.json_path is None else str(presentation.json_path),
        "entry_count": len(presentation.entries),
    }
    explanation["digest"] = digest
    if presentation.basis is None:
        _require_unranked_artifact(presentation)
        position = [str(entry["entry_id"]) for entry in presentation.entries].index(entry_id) + 1
        explanation.update(
            status="unranked",
            ranking=None,
            position=position,
            reason="digest was recorded before ranking existed and presents selection order",
        )
    else:
        replayed = rank_digest(
            store, watch_id, presentation.entries, presentation.issue_view, presentation.basis
        )
        replay = "pending_digest"
        if presentation.status == "written":
            if _recorded_ranking(presentation) != replayed:
                raise MonitoringStateError(
                    "replayed ranking does not match the written digest",
                    details={
                        "reason": "ranking_replay_mismatch",
                        "digest_id": presentation.digest_id,
                    },
                )
            replay = "matches_written_digest"
        explanation.update(
            status="ranked",
            replay=replay,
            basis=replayed["basis"],
            semantic=replayed["semantic"],
            ranking=_ranked_item(replayed, entry_id),
            order=[
                {"rank": item["rank"], "entry_id": item["entry_id"], "score": item["score"]}
                for item in replayed["order"]
            ],
        )
    if include_current:
        explanation["current"] = _current(
            store, watch_id, entry_id, presentation.entries, presentation.issue_view
        )
    return explanation
