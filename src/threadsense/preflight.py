from __future__ import annotations

import os
import shutil
import sys
from dataclasses import dataclass
from urllib import error, request

from threadsense.config import AppConfig


@dataclass(frozen=True)
class DiagnosticCheck:
    name: str
    status: str  # "pass", "fail", "warn"
    message: str

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "status": self.status, "message": self.message}


_MIN_PYTHON = (3, 11)


def check_python_version() -> DiagnosticCheck:
    version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    info = sys.version_info[:2]
    if info >= _MIN_PYTHON:
        return DiagnosticCheck("python_version", "pass", f"Python {version}")
    return DiagnosticCheck(
        "python_version",
        "fail",
        f"Python {version} — requires >={_MIN_PYTHON[0]}.{_MIN_PYTHON[1]}",
    )


def check_storage_directory(config: AppConfig) -> DiagnosticCheck:
    root = config.storage.root_dir
    try:
        root.mkdir(parents=True, exist_ok=True)
        if not os.access(root, os.W_OK):
            return DiagnosticCheck(
                "storage_directory",
                "fail",
                f"{root} is not writable",
            )
    except OSError as err:
        return DiagnosticCheck(
            "storage_directory",
            "fail",
            f"{root} cannot be created: {err}",
        )
    disk_usage = shutil.disk_usage(root)
    free_mb = disk_usage.free / (1024 * 1024)
    if free_mb < 100:
        return DiagnosticCheck(
            "storage_directory",
            "warn",
            f"{root} has {free_mb:.0f}MB free — less than 100MB recommended",
        )
    return DiagnosticCheck(
        "storage_directory",
        "pass",
        f"{root} writable, {free_mb:.0f}MB free",
    )


def check_reddit_reachability() -> DiagnosticCheck:
    url = "https://www.reddit.com/r/test.json"
    try:
        http_request = request.Request(
            url,
            method="HEAD",
            headers={"User-Agent": "threadsense/preflight"},
        )
        with request.urlopen(http_request, timeout=10) as response:
            if response.status < 400:
                return DiagnosticCheck(
                    "reddit_reachable",
                    "pass",
                    f"reddit.com reachable (HTTP {response.status})",
                )
            return DiagnosticCheck(
                "reddit_reachable",
                "warn",
                f"reddit.com returned HTTP {response.status}",
            )
    except (error.URLError, error.HTTPError, TimeoutError) as err:
        return DiagnosticCheck(
            "reddit_reachable",
            "warn",
            f"reddit.com unreachable: {err}",
        )


def run_diagnostic_checks(
    config: AppConfig,
    skip_network: bool = False,
) -> list[DiagnosticCheck]:
    checks = [
        check_python_version(),
        check_storage_directory(config),
    ]
    if not skip_network:
        checks.append(check_reddit_reachability())
    return checks
