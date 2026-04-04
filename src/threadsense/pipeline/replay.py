from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from threadsense.contracts import AnalysisContract
from threadsense.models.analysis import ThreadAnalysis, load_analysis_artifact_file
from threadsense.pipeline.analyze import analyze_thread_file


def replay_analysis(analysis_artifact_path: Path) -> dict[str, Any]:
    original = load_analysis_artifact_file(analysis_artifact_path)
    contract = AnalysisContract.from_dict(original.provenance.contract)
    replayed = analyze_thread_file(
        Path(original.provenance.normalized_artifact_path),
        contract=contract,
    )
    comparison = compare_analysis_outputs(original, replayed)
    return {
        "status": "ready" if comparison["identical"] else "drifted",
        "analysis_artifact_path": str(analysis_artifact_path),
        "normalized_artifact_path": original.provenance.normalized_artifact_path,
        "identical": comparison["identical"],
        "contract": original.provenance.contract,
        "differences": comparison["differences"],
    }


def compare_analysis_outputs(
    original: ThreadAnalysis,
    replayed: ThreadAnalysis,
) -> dict[str, Any]:
    original_payload = _normalize_analysis_payload(original.to_dict())
    replayed_payload = _normalize_analysis_payload(replayed.to_dict())
    if original_payload == replayed_payload:
        return {"identical": True, "differences": []}

    differences: list[dict[str, Any]] = []
    original_findings = _findings_by_theme(original_payload)
    replayed_findings = _findings_by_theme(replayed_payload)
    for theme_key in sorted(set(original_findings) | set(replayed_findings)):
        left = original_findings.get(theme_key)
        right = replayed_findings.get(theme_key)
        if left != right:
            differences.append(
                {
                    "type": "finding_changed",
                    "theme_key": theme_key,
                    "original": left,
                    "replayed": right,
                }
            )

    if not differences:
        differences.append(
            {"type": "artifact_changed", "original": original_payload, "replayed": replayed_payload}
        )
    return {"identical": False, "differences": differences}


def _normalize_analysis_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    analysis = dict(normalized["analysis"])
    provenance = dict(analysis["provenance"])
    provenance["analyzed_at_utc"] = 0.0
    contract = provenance.get("contract")
    if isinstance(contract, dict):
        normalized_contract = dict(contract)
        if "created_at_utc" in normalized_contract:
            normalized_contract["created_at_utc"] = 0.0
        provenance["contract"] = normalized_contract
    analysis["provenance"] = provenance
    normalized["analysis"] = analysis
    return normalized


def _findings_by_theme(payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    analysis = payload["analysis"]
    findings = analysis["findings"]
    assert isinstance(findings, list)
    return {
        finding["theme_key"]: finding
        for finding in findings
        if isinstance(finding, dict) and "theme_key" in finding
    }
