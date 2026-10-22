"""Immutable bounded alert artifacts; publication never holds a SQLite writer fence."""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from pathlib import Path
from typing import Any

from threadsense.errors import MonitoringStateError
from threadsense.monitoring.alert_budget import (
    AdmissionStop,
    AlertBudget,
    AlertSession,
    ArtifactReference,
)

CHAINS = frozenset({"evidence", "change", "provenance"})


def fail(reason: str, **details: Any) -> MonitoringStateError:
    return MonitoringStateError("alert artifact is invalid", details={"reason": reason, **details})


def artifact_path(root: Path, relative: str) -> Path:
    """Resolve only storage-local paths, including symlink destinations."""
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise fail("artifact_outside_state", path=relative)
    try:
        path = (root / relative).resolve()
        if not path.is_relative_to(root.resolve()):
            raise fail("artifact_outside_state", path=relative)
        return path
    except (OSError, ValueError, UnicodeError, RuntimeError) as error:
        raise fail("artifact_unreadable", path=relative) from error


def reference_from_dict(value: Any) -> ArtifactReference:
    """Do not coerce booleans, strings or absent fields into valid identities."""
    if not isinstance(value, dict) or set(value) != {
        "path",
        "sha256",
        "generation",
        "watch_id",
        "chain",
    }:
        raise fail("reference_schema")
    if any(
        not isinstance(value[key], str) or not value[key]
        for key in ("path", "sha256", "watch_id", "chain")
    ):
        raise fail("reference_identity")
    digest = value["sha256"]
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise fail("reference_hash")
    if (
        type(value["generation"]) is not int
        or value["generation"] < 0
        or (value["generation"] == 0 and value["chain"] != "evidence")
    ):
        raise fail("reference_generation")
    if value["chain"] not in CHAINS:
        raise fail("reference_chain")
    return ArtifactReference(
        value["path"], digest, value["generation"], value["watch_id"], value["chain"]
    )


def _admit_value(
    value: Any,
    budget: AlertBudget,
    unit: str,
    dimension: str = "input_bytes",
    *,
    reserve: bool = True,
) -> int:
    """Admit an aggregate native-length envelope before a JSON payload pass."""
    total = 0
    active: set[int] = set()

    def measure(item: Any) -> None:
        nonlocal total
        if isinstance(item, str):
            cost = len(item) * (6 if dimension == "response_bytes" else 4) + 2
        elif type(item) is int:
            cost = max(32, (item.bit_length() * 30103 // 100000) + 3)
        elif item is None or type(item) is bool:
            cost = 32
        elif isinstance(item, float):
            if not math.isfinite(item):
                raise fail("json_nonfinite")
            cost = 32
        elif isinstance(item, (list, tuple, dict)):
            cost = len(item) * (16 if isinstance(item, dict) else 8) + 2
        else:
            raise fail("json_value_type", type=type(item).__name__)
        total += cost
        configured = int(getattr(budget, dimension))
        if total > configured:
            raise AdmissionStop("resource_refused", unit, dimension, total, configured)
        if isinstance(item, (list, tuple, dict)):
            identifier = id(item)
            if identifier in active:
                raise fail("json_cycle")
            active.add(identifier)
            if isinstance(item, dict):
                for key, child in item.items():
                    if not isinstance(key, str):
                        raise fail("json_key_type")
                    measure(key)
                    measure(child)
            else:
                for child in item:
                    measure(child)
            active.remove(identifier)

    try:
        measure(value)
    except RecursionError as error:
        raise fail("json_nesting") from error
    if reserve:
        budget.admit(unit, **{dimension: total})
    else:
        _check_capacity(budget, unit, dimension, total)
    return total


def _check_capacity(budget: AlertBudget, unit: str, dimension: str, required: int) -> None:
    configured = int(getattr(budget, dimension))
    if required > configured:
        raise AdmissionStop("resource_refused", unit, dimension, required, configured)
    if required > budget.remaining(dimension):
        raise AdmissionStop("backlog", unit, dimension, required, configured)


def preflight_json(value: Any, budget: AlertBudget, unit: str) -> bytes:
    """Bound encoder chunks and aggregate output before allocating the final bytes.

    Native scalar lengths admit the counting pass first. The counting encoder
    retains no chunks; its total is checked at each step. Rendering is a second,
    separately admitted application pass. This is not a streaming legacy parser.
    """
    try:
        preflight_cost = _admit_value(value, budget, unit + ":preflight")
        encoder = json.JSONEncoder(
            sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        size = 1
        for chunk in encoder.iterencode(value):
            size += len(chunk.encode("utf-8"))
            _check_capacity(budget, unit, "artifact_bytes", size)
        if preflight_cost + size > budget.input_bytes:
            raise AdmissionStop(
                "resource_refused", unit, "input_bytes", preflight_cost + size, budget.input_bytes
            )
        budget.admit(unit + ":render", input_bytes=size, artifact_bytes=size)
        return (encoder.encode(value) + "\n").encode("utf-8")
    except (ValueError, TypeError, RecursionError, UnicodeError) as error:
        raise fail("json_unreadable", unit=unit) from error


def read_bytes(
    root: Path,
    relative: str,
    budget: AlertBudget,
    unit: str,
    expected_sha256: str | None = None,
    *,
    decode: bool = False,
) -> tuple[bytes, str]:
    """Stat and admit the aggregate read/hash/(optional) decode before reading."""
    path = artifact_path(root, relative)
    try:
        before = path.stat()
        budget.admit(unit, source_rows=1, input_bytes=before.st_size * (3 if decode else 2) + 1)
        budget.check()
        with path.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            if (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns) != (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
            ):
                raise fail("artifact_changed", path=relative)
            data = stream.read(before.st_size + 1)
            after = os.fstat(stream.fileno())
        if len(data) != before.st_size or (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ) != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns):
            raise fail("artifact_changed", path=relative)
        digest = hashlib.sha256(data).hexdigest()
        if expected_sha256 is not None and digest != expected_sha256:
            raise fail("artifact_hash_mismatch", path=relative, expected=expected_sha256)
        budget.check()
        return data, digest
    except (OSError, ValueError, UnicodeError) as error:
        raise fail("artifact_unreadable", path=relative) from error


def read_json(
    root: Path, relative: str, budget: AlertBudget, unit: str, expected_sha256: str | None = None
) -> tuple[dict[str, Any], str]:
    data, digest = read_bytes(root, relative, budget, unit, expected_sha256, decode=True)
    try:
        value = json.loads(data, parse_constant=_invalid_constant, object_pairs_hook=_unique_object)
    except (ValueError, TypeError, UnicodeError, RecursionError) as error:
        raise fail("artifact_json_unreadable", path=relative) from error
    if not isinstance(value, dict):
        raise fail("artifact_object_required", path=relative)
    return value, digest


def _invalid_constant(value: str) -> Any:
    raise ValueError(value)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON member")
        result[key] = value
    return result


def read_reference_data(
    root: Path, budget: AlertBudget, reference: ArtifactReference
) -> dict[str, Any]:
    reference = reference_from_dict(reference.to_dict())
    value, _ = read_json(
        root, reference.path, budget, "reference:" + reference.path, reference.sha256
    )
    version = value.get("schema_version")
    if type(version) is not int or version not in (1, 2):
        raise fail("artifact_schema_unsupported", path=reference.path, schema_version=version)
    if version == 1:
        if (
            reference.generation != 0
            or reference.chain != "evidence"
            or value.get("artifact_type") != "watch_alert_report"
            or value.get("watch_id") != reference.watch_id
            or not isinstance(value.get("report_id"), str)
            or not isinstance(value.get("evidence"), list)
            or any(not isinstance(item, dict) for item in value["evidence"])
        ):
            raise fail("legacy_reference_identity", path=reference.path)
        return value
    if reference.generation == 0:
        raise fail("reference_generation", path=reference.path)
    artifact_type = value.get("artifact_type")
    if artifact_type not in {"watch_alert_manifest", "watch_alert_segment"}:
        raise fail("artifact_type", path=reference.path)
    fields = {
        "schema_version",
        "artifact_type",
        "watch_id",
        "target_id",
        "chain",
        "generation",
        "prior",
        "items" if artifact_type == "watch_alert_segment" else "segment",
    }
    if set(value) != fields:
        raise fail("artifact_schema_fields", path=reference.path)
    if (value.get("watch_id"), value.get("chain"), value.get("generation")) != (
        reference.watch_id,
        reference.chain,
        reference.generation,
    ) or type(value.get("generation")) is not int:
        raise fail("reference_identity_mismatch", path=reference.path)
    if not isinstance(value.get("target_id"), str) or not value["target_id"]:
        raise fail("artifact_target", path=reference.path)
    prior = value.get("prior")
    if prior is not None:
        previous = reference_from_dict(prior)
        if (
            previous.watch_id != reference.watch_id
            or previous.chain != reference.chain
            or previous.generation >= reference.generation
        ):
            raise fail("reference_generation_or_identity", path=reference.path)
    if value["artifact_type"] == "watch_alert_segment":
        if prior is not None or not isinstance(value.get("items"), list):
            raise fail("segment_schema", path=reference.path)
        if any(not isinstance(item, dict) for item in value["items"]):
            raise fail("segment_item_schema", path=reference.path)
    else:
        segment = reference_from_dict(value.get("segment"))
        if (segment.watch_id, segment.chain, segment.generation) != (
            reference.watch_id,
            reference.chain,
            reference.generation,
        ):
            raise fail("segment_identity_mismatch", path=reference.path)
    return value


def read_reference(
    session: AlertSession, budget: AlertBudget, reference: ArtifactReference
) -> dict[str, Any]:
    session.check()
    if session.conn.in_transaction:
        raise fail("artifact_io_inside_writer")
    if reference.watch_id != session.watch_id:
        raise fail("reference_watch_mismatch", path=reference.path)
    value = read_reference_data(session.root, budget, reference)
    session.check()
    return value


def _publish(
    session: AlertSession, budget: AlertBudget, relative: str, data: bytes, digest: str
) -> None:
    session.check()
    if session.conn.in_transaction:
        raise fail("artifact_io_inside_writer")
    path = artifact_path(session.root, relative)
    temporary: str | None = None
    try:
        _check_capacity(budget, "publication:" + relative, "input_bytes", len(data) * 2 + 1)
        try:
            path.stat()
        except FileNotFoundError:
            pass
        else:
            read_bytes(session.root, relative, budget, "publication_existing:" + relative, digest)
            session.check()
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=".alert-", dir=path.parent)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        session.check()
        try:
            os.link(temporary, path)
        except FileExistsError:
            # Never replace a conflict, even when resuming the same staged batch.
            read_bytes(session.root, relative, budget, "publication_conflict:" + relative, digest)
            session.check()
            return
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        read_bytes(session.root, relative, budget, "published:" + relative, digest)
        session.check()
    except (OSError, ValueError, UnicodeError) as error:
        raise fail("artifact_publication_failed", path=relative) from error
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            except OSError as error:
                raise fail("artifact_temporary_cleanup_failed", path=relative) from error


def _publish_segment(
    session: AlertSession,
    budget: AlertBudget,
    *,
    chain: str,
    generation: int,
    items: list[dict[str, Any]],
    prior: ArtifactReference | None,
    target_id: str,
) -> ArtifactReference:
    if (
        chain not in CHAINS
        or type(generation) is not int
        or generation < 1
        or not isinstance(target_id, str)
        or not target_id
    ):
        raise fail("segment_identity")
    if not items or any(not isinstance(item, dict) for item in items):
        raise fail("segment_items")
    session.check()
    if session.conn.in_transaction:
        raise fail("artifact_io_inside_writer")
    from threadsense.monitoring.alert_reader import _citations, _evidence_key

    for item in items:
        if chain == "provenance":
            citation = item.get("citation")
            if (
                set(item) != {"key", "citation"}
                or not isinstance(citation, dict)
                or item["key"] != _evidence_key(citation)
            ):
                raise fail("provenance_decision_schema")
        else:
            _citations(item, "evidence" if chain == "evidence" else "changes")
    if prior is not None:
        prior = reference_from_dict(prior.to_dict())
        if prior.chain != chain or prior.generation >= generation:
            raise fail("reference_generation_or_identity")
        previous = read_reference(session, budget, prior)
        if previous["artifact_type"] != "watch_alert_manifest" and not (
            chain == "evidence" and previous["schema_version"] == 1
        ):
            raise fail("prior_manifest_required")
        if chain == "change" and previous["target_id"] != target_id:
            raise fail("change_target_mismatch")
    base = {
        "schema_version": 2,
        "watch_id": session.watch_id,
        "target_id": target_id,
        "chain": chain,
        "generation": generation,
    }
    segment = {**base, "artifact_type": "watch_alert_segment", "items": items, "prior": None}
    data = preflight_json(segment, budget, "segment")
    budget.admit("segment:hash", input_bytes=len(data))
    digest = hashlib.sha256(data).hexdigest()
    relative = f"alert-artifacts/{digest}.segment.json"
    segment_ref = ArtifactReference(relative, digest, generation, session.watch_id, chain)
    manifest = {
        **base,
        "artifact_type": "watch_alert_manifest",
        "segment": segment_ref.to_dict(),
        "prior": prior.to_dict() if prior else None,
    }
    manifest_data = preflight_json(manifest, budget, "manifest")
    budget.admit("manifest:hash", input_bytes=len(manifest_data))
    manifest_hash = hashlib.sha256(manifest_data).hexdigest()
    manifest_path = f"alert-artifacts/{manifest_hash}.manifest.json"
    # Preflight the aggregate verify work before either durable file exists.
    _check_capacity(
        budget, "segment:publication", "input_bytes", (len(data) + len(manifest_data)) * 2 + 2
    )
    _check_capacity(budget, "segment:publication", "source_rows", 2)
    _publish(session, budget, relative, data, digest)
    _publish(session, budget, manifest_path, manifest_data, manifest_hash)
    return ArtifactReference(manifest_path, manifest_hash, generation, session.watch_id, chain)


def _publish_report(
    session: AlertSession,
    budget: AlertBudget,
    payload: dict[str, Any],
    json_path: str,
    markdown_path: str,
) -> tuple[str, str]:
    from threadsense.monitoring.alerts import render_alert_json, render_alert_markdown

    session.check()
    if session.conn.in_transaction:
        raise fail("artifact_io_inside_writer")
    if payload.get("watch_id") != session.watch_id:
        raise fail("report_watch_mismatch")
    version = payload.get("schema_version")
    if type(version) is not int or version not in (1, 2):
        raise fail("artifact_schema_unsupported", schema_version=version)
    # Validate only immediate heads. Historical links and raw leaves are verified
    # on admitted dereference, never recursively during report finalization.
    from threadsense.monitoring.alert_reader import _report

    _report(payload)
    if version == 2:
        for field in ("evidence_head", "change_head", "provenance_head"):
            if payload[field] is not None:
                reference = reference_from_dict(payload[field])
                head = read_reference(session, budget, reference)
                if reference.chain == "change" and head["target_id"] != payload["target_id"]:
                    raise fail("change_target_mismatch")
    # Preserve the existing pretty schema1 bytes and old Markdown meaning.
    _admit_value(payload, budget, "report:preflight")
    encoder = json.JSONEncoder(sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False)
    size = 1
    for chunk in encoder.iterencode(payload):
        size += len(chunk.encode("utf-8"))
        _check_capacity(budget, "report:json", "artifact_bytes", size)
    # Escaped Markdown and its JSON blocks are bounded before the renderer runs.
    markdown_bound = size * 6 + 4096
    budget.admit(
        "report:render", input_bytes=size + markdown_bound, artifact_bytes=size + markdown_bound
    )
    json_data = render_alert_json(payload)
    markdown_data = render_alert_markdown(payload)
    if len(json_data) != size or len(markdown_data) > markdown_bound:
        raise fail("render_bound_mismatch")
    budget.admit("report:hash", input_bytes=len(json_data) + len(markdown_data))
    json_hash = hashlib.sha256(json_data).hexdigest()
    markdown_hash = hashlib.sha256(markdown_data).hexdigest()
    _check_capacity(
        budget, "report:publication", "input_bytes", (len(json_data) + len(markdown_data)) * 2 + 2
    )
    _check_capacity(budget, "report:publication", "source_rows", 2)
    _publish(session, budget, json_path, json_data, json_hash)
    _publish(session, budget, markdown_path, markdown_data, markdown_hash)
    return json_hash, markdown_hash


def _combined_stop(
    budget: AlertBudget, before: dict[str, int], stop: AdmissionStop
) -> AdmissionStop:
    dimension = str(stop.details["reason"])
    configured = int(getattr(budget, dimension))
    required = budget.used[dimension] - before[dimension] + int(stop.details["required"])
    if required > configured:
        return AdmissionStop(
            "resource_refused", str(stop.details["unit"]), dimension, required, configured
        )
    return stop


def publish_segment(
    session: AlertSession,
    budget: AlertBudget,
    *,
    chain: str,
    generation: int,
    items: list[dict[str, Any]],
    prior: ArtifactReference | None,
    target_id: str,
) -> ArtifactReference:
    """Publish a bounded complete unit or refuse its aggregate envelope."""
    before = dict(budget.used)
    try:
        return _publish_segment(
            session,
            budget,
            chain=chain,
            generation=generation,
            items=items,
            prior=prior,
            target_id=target_id,
        )
    except AdmissionStop as stop:
        combined = _combined_stop(budget, before, stop)
        if combined is stop:
            raise
        raise combined from stop


def publish_report(
    session: AlertSession,
    budget: AlertBudget,
    payload: dict[str, Any],
    json_path: str,
    markdown_path: str,
) -> tuple[str, str]:
    """Publish frozen schema1 bytes or a bounded schema2 completion report."""
    before = dict(budget.used)
    try:
        return _publish_report(session, budget, payload, json_path, markdown_path)
    except AdmissionStop as stop:
        combined = _combined_stop(budget, before, stop)
        if combined is stop:
            raise
        raise combined from stop
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError) as error:
        raise fail("report_render_invalid") from error
