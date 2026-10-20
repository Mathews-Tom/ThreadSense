"""Runtime identity: strict manifest parsing and client-side checkpoint verification.

The service reports only a local checkpoint directory. Identity is proven by hashing the
manifest's files there, never by trusting a revision string the service might omit.
"""

from __future__ import annotations

import hashlib
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from threadsense.decisions.contracts import (
    CheckpointFile,
    CheckpointIdentity,
    CheckpointVerification,
    CodeIdentity,
    RuntimeManifest,
    WireContract,
)
from threadsense.errors import DecisionIdentityError

SUPPORTED_MANIFEST_VERSION = 1
SUPPORTED_BACKEND = "laya"
VERIFICATION_BASIS = "client_file_sha256"
_HASH_CHUNK_BYTES = 1024 * 1024
_COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")

_TOP_KEYS = frozenset({"manifest_version", "backend", "code", "checkpoint", "wire"})
_CODE_KEYS = frozenset({"repository", "commit", "package", "version", "license", "identity_basis"})
_CHECKPOINT_KEYS = frozenset({"repository", "revision", "license", "files", "identity_basis"})
_WIRE_KEYS = frozenset({"health_path", "batch_path", "model", "language"})
_FILE_KEYS = frozenset({"path", "bytes", "sha256"})


def _invalid(field: str, problem: str, **extra: Any) -> DecisionIdentityError:
    return DecisionIdentityError(
        f"runtime manifest field {field} is invalid: {problem}",
        {"reason": "manifest_invalid", "field": field, "problem": problem, **extra},
    )


def _require_mapping(value: Any, field: str, keys: frozenset[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _invalid(field, "not an object", observed=type(value).__name__)
    missing = sorted(keys - set(value))
    unexpected = sorted(set(value) - keys)
    if missing or unexpected:
        raise _invalid(field, "key set mismatch", missing=missing, unexpected=unexpected)
    return value


def _require_text(mapping: Mapping[str, Any], key: str, prefix: str) -> str:
    value = mapping[key]
    if not isinstance(value, str) or not value:
        raise _invalid(f"{prefix}.{key}", "must be a non-empty string")
    return value


def _require_pattern(
    mapping: Mapping[str, Any], key: str, prefix: str, pattern: re.Pattern[str]
) -> str:
    value = _require_text(mapping, key, prefix)
    if pattern.fullmatch(value) is None:
        raise _invalid(f"{prefix}.{key}", "must be lowercase hex of the pinned length")
    return value


def _require_http_path(mapping: Mapping[str, Any], key: str) -> str:
    value = _require_text(mapping, key, "wire")
    if not value.startswith("/") or "?" in value or "#" in value:
        raise _invalid(f"wire.{key}", "must start with / and carry no query or fragment")
    return value


def _require_relative_posix(path: str, field: str) -> None:
    parts = path.split("/")
    if path.startswith("/") or "\\" in path or any(part in ("", ".", "..") for part in parts):
        raise _invalid(field, "must be a relative POSIX path without empty or dot parts")


def _parse_file(value: Any, index: int, seen: set[str]) -> CheckpointFile:
    prefix = f"checkpoint.files[{index}]"
    item = _require_mapping(value, prefix, _FILE_KEYS)
    path = _require_text(item, "path", prefix)
    _require_relative_posix(path, f"{prefix}.path")
    if path in seen:
        raise _invalid(f"{prefix}.path", "duplicate path", path=path)
    seen.add(path)
    size = item["bytes"]
    if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
        raise _invalid(f"{prefix}.bytes", "must be a positive integer")
    digest = _require_pattern(item, "sha256", prefix, _SHA256_PATTERN)
    return CheckpointFile(path=path, bytes=size, sha256=digest)


def _parse_code(value: Any) -> CodeIdentity:
    code = _require_mapping(value, "code", _CODE_KEYS)
    return CodeIdentity(
        repository=_require_text(code, "repository", "code"),
        commit=_require_pattern(code, "commit", "code", _COMMIT_PATTERN),
        package=_require_text(code, "package", "code"),
        version=_require_text(code, "version", "code"),
        license=_require_text(code, "license", "code"),
        identity_basis=_require_text(code, "identity_basis", "code"),
    )


def _parse_checkpoint(value: Any) -> CheckpointIdentity:
    checkpoint = _require_mapping(value, "checkpoint", _CHECKPOINT_KEYS)
    raw_files = checkpoint["files"]
    if not isinstance(raw_files, list) or not raw_files:
        raise _invalid("checkpoint.files", "must be a non-empty list")
    seen: set[str] = set()
    files = tuple(_parse_file(item, index, seen) for index, item in enumerate(raw_files))
    return CheckpointIdentity(
        repository=_require_text(checkpoint, "repository", "checkpoint"),
        revision=_require_pattern(checkpoint, "revision", "checkpoint", _COMMIT_PATTERN),
        license=_require_text(checkpoint, "license", "checkpoint"),
        files=files,
        identity_basis=_require_text(checkpoint, "identity_basis", "checkpoint"),
    )


def _parse_wire(value: Any) -> WireContract:
    wire = _require_mapping(value, "wire", _WIRE_KEYS)
    return WireContract(
        health_path=_require_http_path(wire, "health_path"),
        batch_path=_require_http_path(wire, "batch_path"),
        model=_require_text(wire, "model", "wire"),
        language=_require_text(wire, "language", "wire"),
    )


def parse_runtime_manifest(payload: Mapping[str, Any]) -> RuntimeManifest:
    top = _require_mapping(payload, "manifest", _TOP_KEYS)
    version = top["manifest_version"]
    if (
        isinstance(version, bool)
        or not isinstance(version, int)
        or version != SUPPORTED_MANIFEST_VERSION
    ):
        raise _invalid(
            "manifest_version", "unsupported version", expected=SUPPORTED_MANIFEST_VERSION
        )
    if top["backend"] != SUPPORTED_BACKEND:
        raise _invalid("backend", "unsupported backend", expected=SUPPORTED_BACKEND)
    return RuntimeManifest(
        manifest_version=version,
        backend=SUPPORTED_BACKEND,
        code=_parse_code(top["code"]),
        checkpoint=_parse_checkpoint(top["checkpoint"]),
        wire=_parse_wire(top["wire"]),
    )


def _verification_error(
    reason: str, message: str, repo: str, path: str | None, **extra: Any
) -> DecisionIdentityError:
    return DecisionIdentityError(message, {"reason": reason, "repo": repo, "path": path, **extra})


def _resolve(path: Path, repo: str, relative: str | None) -> Path:
    try:
        return path.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        # Symlink loops raise RuntimeError before Python 3.13 and OSError afterwards.
        raise _verification_error(
            "checkpoint_file_unreadable",
            "checkpoint path could not be resolved",
            repo,
            relative,
            error_type=type(error).__name__,
            error=str(error),
        ) from error


def _sha256_of(candidate: Path) -> str:
    digest = hashlib.sha256()
    with candidate.open("rb") as handle:
        while chunk := handle.read(_HASH_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


class CheckpointVerifier:
    """Verifies the manifest's checkpoint files under the service-reported directory.

    Results, including failures, are cached per repo string so a large checkpoint is hashed
    at most once per verifier lifetime.
    """

    def __init__(self, manifest: RuntimeManifest) -> None:
        self._manifest = manifest
        self._cache: dict[str, CheckpointVerification | DecisionIdentityError] = {}
        self._files_hashed = 0

    @property
    def files_hashed(self) -> int:
        return self._files_hashed

    def verify(self, repo: str) -> CheckpointVerification:
        if repo not in self._cache:
            try:
                self._cache[repo] = self._verify_uncached(repo)
            except DecisionIdentityError as error:
                self._cache[repo] = error
        cached = self._cache[repo]
        if isinstance(cached, DecisionIdentityError):
            raise cached
        return cached

    def _verify_uncached(self, repo: str) -> CheckpointVerification:
        root = Path(repo)
        if not root.is_absolute():
            raise _verification_error(
                "checkpoint_repo_not_absolute", "checkpoint repo must be absolute", repo, None
            )
        if not root.is_dir():
            raise _verification_error(
                "checkpoint_repo_missing", "checkpoint repo is not a directory", repo, None
            )
        resolved_root = _resolve(root, repo, None)
        observed = tuple(
            self._verify_file(repo, root, resolved_root, expected)
            for expected in self._manifest.checkpoint.files
        )
        return CheckpointVerification(repo=repo, files=observed, basis=VERIFICATION_BASIS)

    def _verify_file(
        self, repo: str, root: Path, resolved_root: Path, expected: CheckpointFile
    ) -> CheckpointFile:
        candidate = root / expected.path
        if not candidate.is_file():
            raise _verification_error(
                "checkpoint_file_missing",
                "checkpoint file is not a regular file",
                repo,
                expected.path,
            )
        if not _resolve(candidate, repo, expected.path).is_relative_to(resolved_root):
            raise _verification_error(
                "checkpoint_file_escapes_repo",
                "checkpoint file resolves outside the repo",
                repo,
                expected.path,
            )
        try:
            size = os.stat(candidate).st_size
            if size != expected.bytes:
                raise _verification_error(
                    "checkpoint_file_size_mismatch",
                    "checkpoint file size differs from the manifest",
                    repo,
                    expected.path,
                    expected=expected.bytes,
                    observed=size,
                )
            digest = _sha256_of(candidate)
        except OSError as error:
            raise _verification_error(
                "checkpoint_file_unreadable",
                "checkpoint file could not be read",
                repo,
                expected.path,
                error_type=type(error).__name__,
                error=str(error),
            ) from error
        self._files_hashed += 1
        if digest != expected.sha256:
            raise _verification_error(
                "checkpoint_file_digest_mismatch",
                "checkpoint file digest differs from the manifest",
                repo,
                expected.path,
                expected=expected.sha256,
                observed=digest,
            )
        return CheckpointFile(path=expected.path, bytes=size, sha256=digest)
