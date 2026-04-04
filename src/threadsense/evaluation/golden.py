from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from threadsense.contracts import DomainType
from threadsense.errors import SchemaBoundaryError
from threadsense.evaluation.metrics import (
    EvaluationMetrics,
    compute_precision,
    compute_ratio,
    compute_recall,
)
from threadsense.models.analysis import ThreadAnalysis

_SEVERITY_ORDER = {"low": 1, "medium": 2, "high": 3}


@dataclass(frozen=True)
class GoldenFindingExpectation:
    theme_key: str
    min_severity: str
    must_contain_comment_ids: list[str]
    must_not_contain_comment_ids: list[str]


@dataclass(frozen=True)
class GoldenDuplicateExpectation:
    min_size: int
    must_contain_comment_ids: list[str]


@dataclass(frozen=True)
class GoldenDataset:
    golden_version: int
    thread_fixture: str
    domain: DomainType
    expected_findings: list[GoldenFindingExpectation]
    expected_duplicate_groups: list[GoldenDuplicateExpectation]
    expected_absent_themes: list[str]


@dataclass(frozen=True)
class GoldenValidationResult:
    metrics: EvaluationMetrics
    missing_themes: list[str]
    unexpected_themes: list[str]
    missing_evidence_comment_ids: list[str]
    duplicate_misses: list[list[str]]


def load_golden_manifest(path: Path) -> list[Path]:
    payload = read_json_file(path)
    datasets = payload.get("datasets")
    if not isinstance(datasets, list):
        raise SchemaBoundaryError("golden manifest datasets field is invalid")
    resolved: list[Path] = []
    for item in datasets:
        if not isinstance(item, str) or not item:
            raise SchemaBoundaryError("golden manifest item is invalid")
        resolved.append((path.parent / item).resolve())
    return resolved


def load_golden_dataset(path: Path) -> GoldenDataset:
    payload = read_json_file(path)
    findings_data = required_list(payload, "expected_findings")
    duplicate_data = required_list(payload, "expected_duplicate_groups")
    return GoldenDataset(
        golden_version=required_int(payload, "golden_version"),
        thread_fixture=required_str(payload, "thread_fixture"),
        domain=DomainType(required_str(payload, "domain")),
        expected_findings=[golden_finding_from_dict(item) for item in findings_data],
        expected_duplicate_groups=[golden_duplicate_from_dict(item) for item in duplicate_data],
        expected_absent_themes=required_str_list(payload, "expected_absent_themes"),
    )


def validate_against_golden(
    analysis: ThreadAnalysis,
    golden: GoldenDataset,
) -> GoldenValidationResult:
    reported_themes = {finding.theme_key for finding in analysis.findings}
    expected_themes = {finding.theme_key for finding in golden.expected_findings}
    absent_themes = set(golden.expected_absent_themes)

    missing_themes = sorted(expected_themes - reported_themes)
    unexpected_themes = sorted((reported_themes - expected_themes) & absent_themes)

    missing_evidence_comment_ids: list[str] = []
    matched_evidence = 0
    total_required_evidence = 0
    severity_distances: list[int] = []
    for expected in golden.expected_findings:
        total_required_evidence += len(expected.must_contain_comment_ids)
        actual = next(
            (finding for finding in analysis.findings if finding.theme_key == expected.theme_key),
            None,
        )
        if actual is None:
            missing_evidence_comment_ids.extend(expected.must_contain_comment_ids)
            continue
        matched = set(actual.evidence_comment_ids) & set(expected.must_contain_comment_ids)
        matched_evidence += len(matched)
        for comment_id in expected.must_contain_comment_ids:
            if comment_id not in actual.evidence_comment_ids:
                missing_evidence_comment_ids.append(comment_id)
        actual_level = _SEVERITY_ORDER.get(actual.severity, 0)
        expected_level = _SEVERITY_ORDER.get(expected.min_severity, 0)
        severity_distances.append(max(0, expected_level - actual_level))

    duplicate_misses: list[list[str]] = []
    matched_duplicate_groups = 0
    for expected_group in golden.expected_duplicate_groups:
        actual_group = next(
            (
                group
                for group in analysis.duplicate_groups
                if group.count >= expected_group.min_size
                and all(
                    comment_id in group.comment_ids
                    for comment_id in expected_group.must_contain_comment_ids
                )
            ),
            None,
        )
        if actual_group is None:
            duplicate_misses.append(expected_group.must_contain_comment_ids)
        else:
            matched_duplicate_groups += 1

    severity_alignment = 1.0
    if severity_distances:
        severity_alignment = 1.0 - (sum(severity_distances) / (len(severity_distances) * 2))

    metrics = EvaluationMetrics(
        theme_precision=compute_precision(reported_themes, expected_themes),
        theme_recall=compute_recall(reported_themes, expected_themes),
        evidence_accuracy=compute_ratio(matched_evidence, total_required_evidence),
        severity_alignment=max(0.0, severity_alignment),
        duplicate_recall=compute_ratio(
            matched_duplicate_groups,
            len(golden.expected_duplicate_groups),
        ),
    )
    return GoldenValidationResult(
        metrics=metrics,
        missing_themes=missing_themes,
        unexpected_themes=unexpected_themes,
        missing_evidence_comment_ids=sorted(missing_evidence_comment_ids),
        duplicate_misses=duplicate_misses,
    )


def golden_finding_from_dict(payload: Mapping[str, Any]) -> GoldenFindingExpectation:
    return GoldenFindingExpectation(
        theme_key=required_str(payload, "theme_key"),
        min_severity=required_str(payload, "min_severity"),
        must_contain_comment_ids=required_str_list(payload, "must_contain_comment_ids"),
        must_not_contain_comment_ids=required_str_list(payload, "must_not_contain_comment_ids"),
    )


def golden_duplicate_from_dict(payload: Mapping[str, Any]) -> GoldenDuplicateExpectation:
    return GoldenDuplicateExpectation(
        min_size=required_int(payload, "min_size"),
        must_contain_comment_ids=required_str_list(payload, "must_contain_comment_ids"),
    )


def read_json_file(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise SchemaBoundaryError(
            "golden dataset path does not exist",
            details={"path": str(path)},
        ) from error
    if not isinstance(payload, dict):
        raise SchemaBoundaryError("golden dataset must decode to an object")
    return payload


def required_str(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise SchemaBoundaryError("golden string field is invalid", details={"key": key})
    return value


def required_int(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int):
        raise SchemaBoundaryError("golden integer field is invalid", details={"key": key})
    return value


def required_str_list(payload: Mapping[str, Any], key: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise SchemaBoundaryError("golden string list field is invalid", details={"key": key})
    return value


def required_list(payload: Mapping[str, Any], key: str) -> list[dict[str, Any]]:
    value = payload.get(key)
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise SchemaBoundaryError("golden list field is invalid", details={"key": key})
    return value
