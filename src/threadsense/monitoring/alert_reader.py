"""Bounded schema-1/2 alert pages with report-bound resumable integrity checks.

A complete page verifies its accessed leaves, not every historical leaf. Evidence
provenance is resolved only for the retained page, never a lifetime global map.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from threadsense.monitoring.alert_artifacts import (
    _admit_value,
    artifact_path,
    fail,
    read_bytes,
    read_json,
    read_reference_data,
    reference_from_dict,
)
from threadsense.monitoring.alert_budget import AdmissionStop, AlertBudget, ArtifactReference

_PAGE_ITEMS = 16


def _report(value: dict[str, Any]) -> int:
    version = value.get("schema_version")
    # Schema is deliberately checked before any version-specific interpretation.
    if type(version) is not int or version not in (1, 2):
        raise fail("artifact_schema_unsupported", schema_version=version)
    if value.get("artifact_type") != "watch_alert_report":
        raise fail("report_artifact_type")
    for key in ("watch_id", "report_id"):
        if not isinstance(value.get(key), str) or not value[key]:
            raise fail("report_identity", field=key)
    for key in ("evidence", "alerts"):
        if not isinstance(value.get(key), list) or any(
            not isinstance(item, dict) for item in value[key]
        ):
            raise fail("report_collection", field=key)
    if version == 2:
        if not isinstance(value.get("target_id"), str) or not value["target_id"]:
            raise fail("report_target")
        if not isinstance(value.get("basis"), dict) or not isinstance(
            value.get("baseline"), (dict, type(None))
        ):
            raise fail("report_basis")
        for key in ("evidence_count", "event_count"):
            if type(value.get(key)) is not int or value[key] < 0:
                raise fail("report_count", field=key)
        for field, chain in (
            ("evidence_head", "evidence"),
            ("change_head", "change"),
            ("provenance_head", "provenance"),
        ):
            if field not in value:
                raise fail("report_head_missing", field=field)
            if value[field] is not None:
                reference = reference_from_dict(value[field])
                if reference.watch_id != value["watch_id"] or reference.chain != chain:
                    raise fail("report_head_identity", field=field)
        if value["evidence_count"] and value["evidence_head"] is None:
            raise fail("report_evidence_head_missing")
        if value["event_count"] and value["change_head"] is None:
            raise fail("report_change_head_missing")
    return version


def _evidence_key(citation: dict[str, Any]) -> str:
    report = citation.get("source_report_id")
    digest = citation.get("text_sha256")
    if not isinstance(report, str) or not isinstance(digest, str):
        raise fail("citation_identity")
    key = "evidence:" + report + ":" + digest
    if "evidence_key" in citation and citation["evidence_key"] != key:
        raise fail("citation_identity")
    return key


def _citations(item: dict[str, Any], collection: str) -> list[dict[str, Any]]:
    if collection == "evidence":
        _evidence_key(item)
        return [item]
    citations = item.get("evidence")
    if not isinstance(item.get("alert_id"), str) or not isinstance(citations, list):
        raise fail("event_schema")
    if any(not isinstance(citation, dict) for citation in citations):
        raise fail("event_citation_schema")
    for citation in citations:
        _evidence_key(citation)
    return citations


def _head_field(collection: str) -> str:
    return "evidence_head" if collection == "evidence" else "change_head"


def _reference(
    value: Any, report: dict[str, Any], chain: str, maximum: int | None = None
) -> ArtifactReference:
    reference = reference_from_dict(value)
    if reference.watch_id != report["watch_id"] or reference.chain != chain:
        raise fail("cursor_reference_identity")
    if maximum is not None and reference.generation > maximum:
        raise fail("cursor_reference_generation")
    return reference


def _segment(
    root: Path, budget: AlertBudget, reference: ArtifactReference, report: dict[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    manifest = read_reference_data(root, budget, reference)
    if manifest["schema_version"] == 1:
        return manifest["evidence"], None
    if manifest["artifact_type"] != "watch_alert_manifest":
        raise fail("reader_manifest_required", path=reference.path)
    if reference.chain == "change" and manifest["target_id"] != report["target_id"]:
        raise fail("change_target_mismatch", path=reference.path)
    segment_ref = reference_from_dict(manifest["segment"])
    segment = read_reference_data(root, budget, segment_ref)
    if (
        segment["artifact_type"] != "watch_alert_segment"
        or segment["target_id"] != manifest["target_id"]
    ):
        raise fail("reader_segment_identity", path=segment_ref.path)
    prior = manifest.get("prior")
    return segment["items"], prior


def _integer(state: dict[str, Any], key: str) -> int:
    value = state.get(key)
    if type(value) is not int or value < 0:
        raise fail("cursor_position", field=key)
    return value


def _initial(
    report: dict[str, Any], digest: str, relative: str, collection: str, version: int
) -> dict[str, Any]:
    head = report.get(_head_field(collection)) if version == 2 else None
    return {
        "cursor_version": 1,
        "report_path": relative,
        "report_sha256": digest,
        "report_id": report["report_id"],
        "watch_id": report["watch_id"],
        "target_id": report.get("target_id"),
        "schema_version": version,
        "collection": collection,
        "head": head,
        "reference": head,
        "offset": 0,
        "phase": "collect",
        "page": [],
        "next_reference": None,
        "next_offset": 0,
        "chain_complete": False,
        "overlay_reference": None,
        "overlay_offset": 0,
        "resolved": [],
        "leaf_item": 0,
        "leaf_citation": 0,
        "leaf_kind": 0,
        "emitted": 0,
    }


def _validate_cursor(
    state: dict[str, Any],
    report: dict[str, Any],
    digest: str,
    relative: str,
    collection: str,
    version: int,
) -> None:
    expected = _initial(report, digest, relative, collection, version)
    if set(state) != set(expected):
        raise fail("cursor_schema")
    if type(state.get("cursor_version")) is not int or type(state.get("schema_version")) is not int:
        raise fail("cursor_schema")
    _integer(state, "emitted")
    for key in (
        "cursor_version",
        "report_path",
        "report_sha256",
        "report_id",
        "watch_id",
        "target_id",
        "schema_version",
        "collection",
        "head",
    ):
        if state.get(key) != expected[key]:
            raise fail("cursor_report_identity", field=key)
    if state.get("phase") not in ("collect", "overlay", "leaves"):
        raise fail("cursor_phase")
    page = state.get("page")
    if (
        not isinstance(page, list)
        or len(page) > _PAGE_ITEMS
        or any(not isinstance(item, dict) for item in page)
    ):
        raise fail("cursor_page")
    if not isinstance(state.get("resolved"), list) or any(
        not isinstance(key, str) for key in state["resolved"]
    ):
        raise fail("cursor_overlay_keys")
    keys = {_evidence_key(citation) for item in page for citation in _citations(item, collection)}
    if len(state["resolved"]) != len(set(state["resolved"])) or not set(state["resolved"]).issubset(
        keys
    ):
        raise fail("cursor_overlay_keys")
    for key in (
        "offset",
        "next_offset",
        "overlay_offset",
        "leaf_item",
        "leaf_citation",
        "leaf_kind",
    ):
        _integer(state, key)
    if (
        state["leaf_kind"] > 1
        or state["leaf_item"] > len(page)
        or type(state.get("chain_complete")) is not bool
    ):
        raise fail("cursor_leaf_position")
    if version == 2:
        head = report[_head_field(collection)]
        for key in ("reference", "next_reference"):
            if state.get(key) is not None:
                if head is None:
                    raise fail("cursor_reference_identity")
                _reference(
                    state[key],
                    report,
                    "evidence" if collection == "evidence" else "change",
                    reference_from_dict(head).generation,
                )
        if state.get("overlay_reference") is not None:
            head = report["provenance_head"]
            if head is None:
                raise fail("cursor_overlay_identity")
            _reference(
                state["overlay_reference"],
                report,
                "provenance",
                reference_from_dict(head).generation,
            )


def _collect(
    root: Path,
    report: dict[str, Any],
    state: dict[str, Any],
    budget: AlertBudget,
    version: int,
    collection: str,
) -> None:
    offset = state["offset"]
    prior = None
    if version == 1:
        candidates = report["evidence" if collection == "evidence" else "alerts"]
    elif state["reference"] is None:
        candidates = []
    else:
        reference = _reference(
            state["reference"], report, "evidence" if collection == "evidence" else "change"
        )
        candidates, prior = _segment(root, budget, reference, report)
    if offset > len(candidates):
        raise fail("cursor_segment_offset")
    # One immutable segment is a bounded input unit, but pages may subdivide it.
    if offset < len(candidates) and not budget.remaining("derivations"):
        budget.admit("reader:page", derivations=1)
    end = min(len(candidates), offset + _PAGE_ITEMS, offset + budget.remaining("derivations"))
    while True:
        page = [dict(item) for item in candidates[offset:end]]
        for item in page:
            _citations(item, collection)
        preview = {
            **state,
            "page": page,
            "phase": "overlay",
            "next_reference": state["reference"] if end < len(candidates) else prior,
            "next_offset": end if end < len(candidates) else 0,
            "overlay_reference": report.get("provenance_head") if version == 2 else None,
            "resolved": sorted(
                {
                    _evidence_key(citation)
                    for item in page
                    for citation in _citations(item, collection)
                }
            ),
        }
        try:
            _admit_value(
                {
                    "items": [],
                    "cursor": preview,
                    "accounting": budget.accounting(),
                    "complete": False,
                    "status": "backlog",
                    "integrity": "accessed_page_leaves_only",
                    "refusal": {
                        "status": "backlog",
                        "unit": "reader:continuation_control",
                        "reason": "input_bytes",
                        "required": 0,
                        "configured": 0,
                    },
                },
                budget,
                "reader:page_control",
                "response_bytes",
                reserve=False,
            )
        except AdmissionStop:
            if end - offset <= 1:
                raise
            end -= 1
            continue
        break
    budget.admit("reader:page", derivations=end - offset)
    state["page"] = page
    if end < len(candidates):
        state["next_reference"] = state["reference"]
        state["next_offset"] = end
        state["chain_complete"] = False
    else:
        state["next_reference"] = prior
        state["next_offset"] = 0
        state["chain_complete"] = version == 1 or prior is None
    state["overlay_reference"] = report.get("provenance_head") if version == 2 else None
    state["overlay_offset"] = 0
    state["resolved"] = []
    state["leaf_item"] = state["leaf_citation"] = state["leaf_kind"] = 0
    state["phase"] = "overlay"


def _overlay(
    root: Path, report: dict[str, Any], state: dict[str, Any], budget: AlertBudget, collection: str
) -> None:
    citations = [citation for item in state["page"] for citation in _citations(item, collection)]
    keys = {_evidence_key(citation) for citation in citations}
    resolved = set(state["resolved"])
    while state["overlay_reference"] is not None and keys - resolved:
        reference = _reference(state["overlay_reference"], report, "provenance")
        decisions, prior = _segment(root, budget, reference, report)
        position = state["overlay_offset"]
        if position > len(decisions):
            raise fail("cursor_overlay_offset")
        # Newer decisions win within the segment as well as across links.
        while position < len(decisions):
            budget.admit("reader:provenance_decision", derivations=1)
            decision = decisions[len(decisions) - 1 - position]
            key = decision.get("key")
            citation = decision.get("citation")
            if (
                not isinstance(key, str)
                or not isinstance(citation, dict)
                or _evidence_key(citation) != key
            ):
                raise fail("provenance_decision_schema", path=reference.path)
            if key in keys and key not in resolved:
                for item in state["page"]:
                    if collection == "evidence":
                        if _evidence_key(item) == key:
                            item.clear()
                            item.update(citation)
                    else:
                        item["evidence"] = [
                            dict(citation) if _evidence_key(old) == key else old
                            for old in item["evidence"]
                        ]
                resolved.add(key)
                state["resolved"] = sorted(resolved)
            position += 1
            state["overlay_offset"] = position
        state["overlay_reference"] = prior
        state["overlay_offset"] = 0
    state["phase"] = "leaves"


def _leaves(root: Path, state: dict[str, Any], budget: AlertBudget, collection: str) -> None:
    while state["leaf_item"] < len(state["page"]):
        citations = _citations(state["page"][state["leaf_item"]], collection)
        if state["leaf_citation"] > len(citations):
            raise fail("cursor_leaf_position")
        while state["leaf_citation"] < len(citations):
            citation = citations[state["leaf_citation"]]
            while state["leaf_kind"] < 2:
                kind = ("snapshot", "analysis")[state["leaf_kind"]]
                relative = citation.get(kind + "_path")
                digest = citation.get(kind + "_sha256")
                if (
                    not isinstance(relative, str)
                    or not isinstance(digest, str)
                    or len(digest) != 64
                ):
                    raise fail("citation_leaf_identity", kind=kind)
                read_bytes(root, relative, budget, "reader:leaf:" + relative, digest)
                state["leaf_kind"] += 1
            state["leaf_kind"] = 0
            state["leaf_citation"] += 1
        state["leaf_citation"] = 0
        state["leaf_item"] += 1


def _response(
    items: list[dict[str, Any]],
    cursor: dict[str, Any] | None,
    complete: bool,
    budget: AlertBudget,
    status: str,
    refusal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "items": items,
        "cursor": cursor,
        "complete": complete,
        "status": status,
        "accounting": budget.accounting(),
        "integrity": "accessed_page_leaves_only",
    }
    if refusal is not None:
        value["refusal"] = refusal
    # Output generation has its own envelope. Reserve a native-length upper
    # bound before the counting encoder; no response-sized string is allocated.
    bound = _admit_value(value, budget, "reader:response_preflight", "response_bytes")
    size = 0
    encoder = json.JSONEncoder(
        sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )
    for chunk in encoder.iterencode(value):
        size += len(chunk.encode("utf-8"))
        if size > bound:
            raise fail("response_bound_mismatch")
    value["accounting"] = budget.accounting()
    return value


def read_alert_page(
    *,
    root: Path,
    report_path: Path,
    budget: AlertBudget | None = None,
    cursor: dict[str, Any] | None = None,
    collection: str = "evidence",
) -> dict[str, Any]:
    """Read one immutable page or return honest, bounded continuation/refusal.

    The caller persists the returned cursor unchanged. Cursors are continuation
    state, not authentication credentials. No historical chain is eagerly
    rehashed; every link and raw leaf actually accessed is admitted and verified.
    """
    budget = budget if budget is not None else AlertBudget()
    collection = "changes" if collection == "change" else collection
    if collection not in ("evidence", "changes"):
        raise fail("reader_collection", collection=collection)
    try:
        absolute = (
            report_path.resolve() if report_path.is_absolute() else (root / report_path).resolve()
        )
        relative = absolute.relative_to(root.resolve()).as_posix()
        artifact_path(root, relative)
    except (ValueError, OSError) as error:
        raise fail("artifact_outside_state", path=str(report_path)) from error
    state: dict[str, Any] | None = None
    before = dict(budget.used)
    try:
        if cursor is not None:
            _admit_value(cursor, budget, "reader:cursor")
            state = copy.deepcopy(cursor)
        expected_hash = None if cursor is None else cursor.get("report_sha256")
        if cursor is not None and (not isinstance(expected_hash, str) or len(expected_hash) != 64):
            raise fail("cursor_report_identity")
        report, digest = read_json(root, relative, budget, "reader:report", expected_hash)
        version = _report(report)
        if state is None:
            state = _initial(report, digest, relative, collection, version)
        else:
            _validate_cursor(state, report, digest, relative, collection, version)
        while True:
            if state["phase"] == "collect":
                _collect(root, report, state, budget, version, collection)
            if state["phase"] == "overlay":
                _overlay(root, report, state, budget, collection)
            _leaves(root, state, budget, collection)
            items = state["page"]
            complete = state["chain_complete"]
            emitted = state["emitted"] + len(items)
            expected_count = (
                report["evidence_count" if collection == "evidence" else "event_count"]
                if version == 2
                else len(report["evidence" if collection == "evidence" else "alerts"])
            )
            if emitted > expected_count or (complete and emitted != expected_count):
                raise fail("report_count_mismatch", expected=expected_count, observed=emitted)
            if not items and not complete:
                state["reference"] = state["next_reference"]
                state["offset"] = state["next_offset"]
                state["phase"] = "collect"
                continue
            next_cursor = None
            if not complete:
                next_cursor = _initial(report, digest, relative, collection, version)
                next_cursor["reference"] = state["next_reference"]
                next_cursor["offset"] = state["next_offset"]
                next_cursor["emitted"] = emitted
            return _response(
                items, next_cursor, complete, budget, "complete" if complete else "backlog"
            )
    except AdmissionStop as stop:
        # Pending page, overlay offset and leaf offsets are retained. Nothing
        # unverified is returned as an item or represented as empty success.
        # A repeated fixed-prefix read is not progress. If the same continuation
        # cannot reach its next indivisible unit in a fresh envelope, report the
        # aggregate required allowance instead of promising endless backlog.
        unchanged = state is None or (cursor is not None and _position(state) == _position(cursor))
        if unchanged and stop.details["status"] == "backlog":
            dimension = str(stop.details["reason"])
            required = budget.used[dimension] - before[dimension] + int(stop.details["required"])
            configured = int(getattr(budget, dimension))
            if required > configured:
                stop = AdmissionStop(
                    "resource_refused", str(stop.details["unit"]), dimension, required, configured
                )
        try:
            return _response([], state, False, budget, str(stop.details["status"]), stop.details)
        except AdmissionStop as response_stop:
            # The page-control unit itself cannot fit. Preserve the caller's
            # original cursor rather than echo an oversized control payload;
            # an adequate finite envelope can resume that exact old position.
            details = {
                **response_stop.details,
                "status": "resource_refused",
                "unit": "reader:continuation_control",
            }
            try:
                return _response([], None, False, budget, "resource_refused", details)
            except AdmissionStop as minimum_stop:
                raise fail(
                    "reader_control_resource_refused", refusal=minimum_stop.details
                ) from minimum_stop


def _position(state: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(
        state.get(key)
        for key in (
            "phase",
            "reference",
            "offset",
            "overlay_reference",
            "overlay_offset",
            "leaf_item",
            "leaf_citation",
            "leaf_kind",
            "emitted",
        )
    )
