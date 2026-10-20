"""Process failures must reach the annotations smoke's failed-evidence consumer."""

from __future__ import annotations

import errno
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


@pytest.fixture
def smoke_modules(monkeypatch: pytest.MonkeyPatch) -> tuple[ModuleType, ModuleType]:
    scripts = Path(__file__).resolve().parents[2] / "scripts"
    modules = []
    for name in ("smoke_annotations", "smoke_monitoring"):
        spec = importlib.util.spec_from_file_location(name, scripts / f"{name}.py")
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module)
        spec.loader.exec_module(module)
        modules.append(module)
    return modules[0], modules[1]


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("core_without_model", ()),
        ("cli", ("decisions", "annotate")),
        ("server_memory_observation", ()),
        ("ps_field", ("rss=", "123")),
    ],
)
@pytest.mark.parametrize("failure_kind", ["timeout", "launch"])
def test_process_failure_writes_redacted_failed_evidence(
    smoke_modules: tuple[ModuleType, ModuleType],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    method: str,
    args: tuple[str, ...],
    failure_kind: str,
) -> None:
    annotations, monitoring = smoke_modules
    private_path = "/tmp/private-smoke-workspace/missing-executable"
    error: subprocess.TimeoutExpired | OSError
    if failure_kind == "timeout":
        error = subprocess.TimeoutExpired([private_path], 120)
    else:
        error = FileNotFoundError(errno.ENOENT, "No such file or directory", private_path)

    def fail_process(*argv: Any, **kwargs: Any) -> Any:
        raise error

    def run_boundary(scenario: Any) -> None:
        getattr(scenario, method)(*args)

    monkeypatch.setattr(annotations.subprocess, "run", fail_process)
    monkeypatch.setattr(annotations.AnnotationsScenario, "run", run_boundary)
    work_dir = tmp_path / "smoke"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "smoke_monitoring.py",
            "--scenario",
            "annotations",
            "--work-dir",
            str(work_dir),
            "--endpoint",
            "http://127.0.0.1:8123",
        ],
    )

    assert monitoring.main() == 1

    evidence_text = (work_dir / "annotations-evidence.json").read_text(encoding="utf-8")
    evidence = json.loads(evidence_text)
    assert evidence["status"] == "failed"
    assert evidence["error"]
    assert evidence["steps"] == []
    assert "<local-path>" in evidence["error"]
    assert private_path not in evidence_text
    output = capsys.readouterr().out
    assert "status=failed" in output
    assert private_path not in output


def test_missing_lsof_preserves_cause_without_memory_fallback(
    smoke_modules: tuple[ModuleType, ModuleType],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    annotations, _ = smoke_modules
    error = FileNotFoundError(errno.ENOENT, "No such file or directory", "lsof")

    def missing_lsof(*argv: Any, **kwargs: Any) -> Any:
        raise error

    monkeypatch.setattr(annotations.subprocess, "run", missing_lsof)
    scenario = annotations.AnnotationsScenario(tmp_path, "http://127.0.0.1:8123")

    with pytest.raises(annotations.SmokeFailure) as caught:
        scenario.server_memory_observation()

    assert caught.value.__cause__ is error
    assert scenario.evidence == []
