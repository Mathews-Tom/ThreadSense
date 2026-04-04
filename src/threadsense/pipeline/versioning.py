from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import gmtime, strftime
from typing import Any

from threadsense.errors import AnalysisBoundaryError
from threadsense.models.analysis import ThreadAnalysis


@dataclass(frozen=True)
class VersionedArtifactSave:
    version_number: int
    version_path: Path
    latest_path: Path


def save_versioned_artifact(base_path: Path, payload: dict[str, Any]) -> VersionedArtifactSave:
    version_dir = base_path.with_suffix("")
    version_dir.mkdir(parents=True, exist_ok=True)
    version_number = next_version_number(version_dir)
    timestamp = strftime("%Y-%m-%dT%H-%M-%SZ", gmtime())
    version_path = version_dir / f"v{version_number}_{timestamp}.json"
    latest_path = version_dir / "latest.json"
    version_path.write_text(__import__("json").dumps(payload, indent=2), encoding="utf-8")
    latest_path.write_text(version_path.read_text(encoding="utf-8"), encoding="utf-8")
    return VersionedArtifactSave(
        version_number=version_number,
        version_path=version_path,
        latest_path=latest_path,
    )


def load_latest(versioned_base_path: Path) -> Path:
    latest_path = versioned_base_path.with_suffix("") / "latest.json"
    if not latest_path.exists():
        raise AnalysisBoundaryError(
            "latest analysis artifact does not exist",
            details={"path": str(latest_path)},
        )
    return latest_path


def load_version(versioned_base_path: Path, version_number: int) -> Path:
    version_dir = versioned_base_path.with_suffix("")
    matches = sorted(version_dir.glob(f"v{version_number}_*.json"))
    if not matches:
        raise AnalysisBoundaryError(
            "requested analysis version does not exist",
            details={"path": str(version_dir), "version_number": version_number},
        )
    return matches[-1]


def next_version_number(version_dir: Path) -> int:
    highest = 0
    for candidate in version_dir.glob("v*_*.json"):
        prefix = candidate.stem.split("_", 1)[0]
        if prefix.startswith("v") and prefix[1:].isdigit():
            highest = max(highest, int(prefix[1:]))
    return highest + 1


def diff_analyses(
    left: ThreadAnalysis,
    right: ThreadAnalysis,
) -> dict[str, Any]:
    left_payload = _normalize_analysis_payload(left.to_dict())
    right_payload = _normalize_analysis_payload(right.to_dict())
    if left_payload == right_payload:
        return {"identical": True, "differences": []}

    differences: list[dict[str, Any]] = []
    left_findings = _findings_by_theme(left_payload)
    right_findings = _findings_by_theme(right_payload)
    for theme_key in sorted(set(left_findings) | set(right_findings)):
        left_finding = left_findings.get(theme_key)
        right_finding = right_findings.get(theme_key)
        if left_finding != right_finding:
            differences.append(
                {
                    "type": "finding_changed",
                    "theme_key": theme_key,
                    "original": left_finding,
                    "replayed": right_finding,
                }
            )

    if not differences:
        differences.append(
            {
                "type": "artifact_changed",
                "original": left_payload,
                "replayed": right_payload,
            }
        )
    return {"identical": False, "differences": differences}


def _normalize_analysis_payload(payload: dict[str, Any]) -> dict[str, Any]:
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


def _findings_by_theme(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    findings = payload["analysis"]["findings"]
    if not isinstance(findings, list):
        return {}
    return {
        finding["theme_key"]: finding
        for finding in findings
        if isinstance(finding, dict) and "theme_key" in finding
    }
