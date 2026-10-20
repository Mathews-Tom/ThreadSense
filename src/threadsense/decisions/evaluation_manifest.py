"""Load a frozen evaluation dataset: manifest, disclosed labels and the bound corpus.

Every file is read once and bound by the sha256 of its exact bytes. The corpus is
checked for partition leaks so that copies of a thread, of a comment or of one authored
repeated issue never straddle development, calibration and evaluation.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from threadsense.contracts.analysis_contract import DomainType, ObjectiveType
from threadsense.decisions.contracts import (
    ChoiceQuestion,
    Question,
    QuestionSet,
    ScoreQuestion,
    decode_strict_json_object,
)
from threadsense.decisions.evaluation_contracts import (
    EVALUATION_LABELS_KIND,
    EVALUATION_MANIFEST_KIND,
    EVALUATION_SCHEMA_VERSION,
    CaseCategory,
    CorpusComment,
    CorpusThread,
    EvaluationManifest,
    FileBinding,
    IdentityBindings,
    IntegrityCheck,
    IssueGroup,
    Label,
    LabelOrigin,
    LabelOriginKind,
    LabelSet,
    ManifestThread,
    Partition,
    PartitionIntegrity,
    SamplingLimits,
)
from threadsense.errors import DecisionEvaluationError
from threadsense.models.canonical import canonical_thread_from_payload
from threadsense.monitoring.observations import comment_text_sha256, thread_text_sha256

_MANIFEST_KEYS = frozenset(
    {
        "artifact_kind",
        "schema_version",
        "dataset_id",
        "description",
        "objective",
        "domain",
        "bindings",
        "supported_languages",
        "sampling",
        "labels",
        "threads",
        "issue_groups",
    }
)
_BINDING_KEYS = frozenset(
    {
        "runtime_manifest_sha256",
        "checkpoint_revision",
        "question_set_sha256",
        "settings_sha256",
        "context_policy_sha256",
        "language",
    }
)
_SAMPLING_KEYS = frozenset({"method", "selection", "max_threads", "max_comments_per_thread"})
_FILE_KEYS = frozenset({"path", "sha256"})
_THREAD_KEYS = frozenset(
    {"thread_id", "path", "sha256", "partition", "source_language", "comment_languages"}
)
_GROUP_KEYS = frozenset({"group_id", "members", "note"})
_MEMBER_KEYS = frozenset({"thread_id", "comment_id"})
_LABELS_KEYS = frozenset({"artifact_kind", "schema_version", "origin", "labels"})
_ORIGIN_KEYS = frozenset({"kind", "authored_by", "method", "disclosure"})
_LABEL_KEYS = frozenset({"thread_id", "comment_id", "question_id", "expected", "case", "rationale"})
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_REVISION_PATTERN = re.compile(r"[0-9a-f]{40}")
_LANGUAGE_PATTERN = re.compile(r"[a-z][a-z0-9-]*")

Invalid = Callable[..., DecisionEvaluationError]
# A copy-group member: its thread id and, for comment copies, its comment id.
Member = dict[str, str]


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _refusal(reason: str, message: str, **details: Any) -> DecisionEvaluationError:
    return DecisionEvaluationError(message, {"reason": reason, **details})


def _manifest_invalid(field: str, message: str, **extra: Any) -> DecisionEvaluationError:
    return _refusal(
        "manifest_invalid", f"evaluation manifest {field} {message}", field=field, **extra
    )


def _labels_invalid(field: str, message: str, **extra: Any) -> DecisionEvaluationError:
    return _refusal("labels_invalid", f"evaluation labels {field} {message}", field=field, **extra)


def _read_bytes(path: Path, what: str) -> bytes:
    try:
        return path.read_bytes()
    except OSError as error:
        raise _refusal(
            "input_unreadable",
            f"{what} could not be read",
            path=str(path),
            error_type=type(error).__name__,
        ) from error


def _require_object(
    value: Any, field: str, keys: frozenset[str], invalid: Invalid
) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise invalid(field, "must be a JSON object")
    missing = sorted(keys - value.keys())
    unexpected = sorted(value.keys() - keys)
    if missing or unexpected:
        raise invalid(field, "has an invalid key set", missing=missing, unexpected=unexpected)
    return value


def _require_list(value: Any, field: str, invalid: Invalid) -> list[Any]:
    if not isinstance(value, list):
        raise invalid(field, "must be a JSON list")
    return value


def _require_text(mapping: Mapping[str, Any], key: str, prefix: str, invalid: Invalid) -> str:
    field = f"{prefix}.{key}" if prefix else key
    value = mapping[key]
    if not isinstance(value, str) or not value.strip():
        raise invalid(field, "must be a non-empty string")
    return value


def _require_pattern(
    mapping: Mapping[str, Any], key: str, prefix: str, pattern: re.Pattern[str]
) -> str:
    value = _require_text(mapping, key, prefix, _manifest_invalid)
    if pattern.fullmatch(value) is None:
        raise _manifest_invalid(f"{prefix}.{key}", "has an invalid format")
    return value


def _require_language(value: Any, field: str) -> str:
    if not isinstance(value, str) or _LANGUAGE_PATTERN.fullmatch(value) is None:
        raise _manifest_invalid(field, "must be a non-empty lowercase language code")
    return value


def _require_positive_int(mapping: Mapping[str, Any], key: str, prefix: str) -> int:
    value = mapping[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise _manifest_invalid(f"{prefix}.{key}", "must be a positive integer")
    count: int = value
    if count < 1:
        raise _manifest_invalid(f"{prefix}.{key}", "must be a positive integer")
    return count


def _require_relative_path(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise _manifest_invalid(field, "must be a non-empty relative POSIX path")
    if value.startswith("/") or any(part in ("", ".") for part in value.split("/")):
        raise _manifest_invalid(field, "must be a relative POSIX path without empty or . parts")
    return value


def _require_kind_and_version(
    payload: Mapping[str, Any], kind: str, invalid: Invalid, unsupported_reason: str
) -> None:
    if payload.get("artifact_kind") != kind:
        raise invalid("artifact_kind", "is not the expected kind", expected=kind)
    version = payload.get("schema_version")
    if (
        isinstance(version, bool)
        or not isinstance(version, int)
        or version != EVALUATION_SCHEMA_VERSION
    ):
        raise _refusal(
            unsupported_reason,
            "schema version is unsupported",
            field="schema_version",
            schema_version=version,
            supported=[EVALUATION_SCHEMA_VERSION],
        )


def _parse_file_binding(value: Any, field: str) -> FileBinding:
    binding = _require_object(value, field, _FILE_KEYS, _manifest_invalid)
    return FileBinding(
        path=_require_relative_path(binding["path"], f"{field}.path"),
        sha256=_require_pattern(binding, "sha256", field, _SHA256_PATTERN),
    )


def _parse_bindings(value: Any) -> IdentityBindings:
    bindings = _require_object(value, "bindings", _BINDING_KEYS, _manifest_invalid)

    def digest(key: str) -> str:
        return _require_pattern(bindings, key, "bindings", _SHA256_PATTERN)

    return IdentityBindings(
        runtime_manifest_sha256=digest("runtime_manifest_sha256"),
        checkpoint_revision=_require_pattern(
            bindings, "checkpoint_revision", "bindings", _REVISION_PATTERN
        ),
        question_set_sha256=digest("question_set_sha256"),
        settings_sha256=digest("settings_sha256"),
        context_policy_sha256=digest("context_policy_sha256"),
        language=_require_language(bindings["language"], "bindings.language"),
    )


def _parse_sampling(value: Any) -> SamplingLimits:
    sampling = _require_object(value, "sampling", _SAMPLING_KEYS, _manifest_invalid)
    return SamplingLimits(
        method=_require_text(sampling, "method", "sampling", _manifest_invalid),
        selection=_require_text(sampling, "selection", "sampling", _manifest_invalid),
        max_threads=_require_positive_int(sampling, "max_threads", "sampling"),
        max_comments_per_thread=_require_positive_int(
            sampling, "max_comments_per_thread", "sampling"
        ),
    )


def _parse_supported_languages(value: Any) -> tuple[str, ...]:
    raw = _require_list(value, "supported_languages", _manifest_invalid)
    languages = tuple(
        _require_language(item, f"supported_languages[{index}]") for index, item in enumerate(raw)
    )
    if not languages or len(set(languages)) != len(languages):
        raise _manifest_invalid("supported_languages", "must be a non-empty list without repeats")
    return languages


def _parse_objective(value: Any) -> ObjectiveType:
    if not isinstance(value, str) or value not in {item.value for item in ObjectiveType}:
        raise _manifest_invalid("objective", "is not a supported objective", objective=value)
    return ObjectiveType(value)


def _parse_domain(value: Any) -> str:
    known = {item.value for item in DomainType if item is not DomainType.CUSTOM}
    if not isinstance(value, str) or value not in known:
        raise _manifest_invalid("domain", "is not a built-in domain", domain=value)
    return value


def _parse_partition(value: Any, field: str) -> Partition:
    if not isinstance(value, str) or value not in {item.value for item in Partition}:
        raise _manifest_invalid(field, "is not a known partition", partition=value)
    return Partition(value)


def _parse_comment_languages(value: Any, field: str) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, dict):
        raise _manifest_invalid(field, "must be a JSON object")
    pairs: list[tuple[str, str]] = []
    for comment_id, language in value.items():
        if not comment_id.strip():
            raise _manifest_invalid(field, "has an empty comment id")
        pairs.append((comment_id, _require_language(language, f"{field}.{comment_id}")))
    return tuple(pairs)


def _parse_thread(value: Any, index: int) -> ManifestThread:
    field = f"threads[{index}]"
    entry = _require_object(value, field, _THREAD_KEYS, _manifest_invalid)
    return ManifestThread(
        thread_id=_require_text(entry, "thread_id", field, _manifest_invalid),
        file=FileBinding(
            path=_require_relative_path(entry["path"], f"{field}.path"),
            sha256=_require_pattern(entry, "sha256", field, _SHA256_PATTERN),
        ),
        partition=_parse_partition(entry["partition"], f"{field}.partition"),
        source_language=_require_language(entry["source_language"], f"{field}.source_language"),
        comment_languages=_parse_comment_languages(
            entry["comment_languages"], f"{field}.comment_languages"
        ),
    )


def _parse_threads(value: Any) -> tuple[ManifestThread, ...]:
    raw = _require_list(value, "threads", _manifest_invalid)
    if not raw:
        raise _manifest_invalid("threads", "must list at least one thread")
    threads = tuple(_parse_thread(item, index) for index, item in enumerate(raw))
    seen: set[str] = set()
    for thread in threads:
        if thread.thread_id in seen:
            raise _manifest_invalid("threads", "repeats a thread id", thread_id=thread.thread_id)
        seen.add(thread.thread_id)
    return threads


def _parse_group(value: Any, index: int, thread_ids: set[str]) -> IssueGroup:
    field = f"issue_groups[{index}]"
    group = _require_object(value, field, _GROUP_KEYS, _manifest_invalid)
    raw_members = _require_list(group["members"], f"{field}.members", _manifest_invalid)
    members: list[tuple[str, str]] = []
    for position, item in enumerate(raw_members):
        prefix = f"{field}.members[{position}]"
        member = _require_object(item, prefix, _MEMBER_KEYS, _manifest_invalid)
        thread_id = _require_text(member, "thread_id", prefix, _manifest_invalid)
        if thread_id not in thread_ids:
            raise _manifest_invalid(prefix, "names an unlisted thread", thread_id=thread_id)
        members.append((thread_id, _require_text(member, "comment_id", prefix, _manifest_invalid)))
    if len(members) < 2:
        raise _manifest_invalid(f"{field}.members", "must list at least two comments")
    return IssueGroup(
        group_id=_require_text(group, "group_id", field, _manifest_invalid),
        members=tuple(members),
        note=_require_text(group, "note", field, _manifest_invalid),
    )


def _parse_issue_groups(value: Any, threads: Sequence[ManifestThread]) -> tuple[IssueGroup, ...]:
    raw = _require_list(value, "issue_groups", _manifest_invalid)
    thread_ids = {thread.thread_id for thread in threads}
    groups = tuple(_parse_group(item, index, thread_ids) for index, item in enumerate(raw))
    group_ids: set[str] = set()
    owner: dict[tuple[str, str], str] = {}
    for group in groups:
        if group.group_id in group_ids:
            raise _manifest_invalid("issue_groups", "repeats a group id", group_id=group.group_id)
        group_ids.add(group.group_id)
        for thread_id, comment_id in group.members:
            if (thread_id, comment_id) in owner:
                raise _manifest_invalid(
                    "issue_groups",
                    "places a comment in more than one group or twice in one group",
                    thread_id=thread_id,
                    comment_id=comment_id,
                    group_id=group.group_id,
                )
            owner[(thread_id, comment_id)] = group.group_id
    return groups


def load_evaluation_manifest(path: Path) -> EvaluationManifest:
    raw = _read_bytes(path, "evaluation manifest")
    payload = decode_strict_json_object(raw, DecisionEvaluationError, "evaluation manifest")
    _require_kind_and_version(
        payload, EVALUATION_MANIFEST_KIND, _manifest_invalid, "manifest_version_unsupported"
    )
    top = _require_object(payload, "manifest", _MANIFEST_KEYS, _manifest_invalid)
    threads = _parse_threads(top["threads"])
    bindings = _parse_bindings(top["bindings"])
    supported_languages = _parse_supported_languages(top["supported_languages"])
    if supported_languages != (bindings.language,):
        raise _manifest_invalid(
            "supported_languages",
            "must contain only the bound inference language",
            language=bindings.language,
        )
    return EvaluationManifest(
        sha256=_sha256(raw),
        dataset_id=_require_text(top, "dataset_id", "", _manifest_invalid),
        description=_require_text(top, "description", "", _manifest_invalid),
        objective=_parse_objective(top["objective"]),
        domain=_parse_domain(top["domain"]),
        bindings=bindings,
        supported_languages=supported_languages,
        sampling=_parse_sampling(top["sampling"]),
        labels=_parse_file_binding(top["labels"], "labels"),
        threads=threads,
        issue_groups=_parse_issue_groups(top["issue_groups"], threads),
    )


def _parse_origin(value: Any) -> LabelOrigin:
    origin = _require_object(value, "origin", _ORIGIN_KEYS, _labels_invalid)
    kind = origin["kind"]
    if not isinstance(kind, str) or kind not in {item.value for item in LabelOriginKind}:
        raise _labels_invalid("origin.kind", "is not a known label origin", kind=kind)
    return LabelOrigin(
        kind=LabelOriginKind(kind),
        authored_by=_require_text(origin, "authored_by", "origin", _labels_invalid),
        method=_require_text(origin, "method", "origin", _labels_invalid),
        disclosure=_require_text(origin, "disclosure", "origin", _labels_invalid),
    )


def _parse_label(value: Any, index: int) -> Label:
    field = f"labels[{index}]"
    entry = _require_object(value, field, _LABEL_KEYS, _labels_invalid)
    expected = entry["expected"]
    if not isinstance(expected, str | int):
        raise _labels_invalid(f"{field}.expected", "must be a string, integer or boolean")
    case = entry["case"]
    if not isinstance(case, str) or case not in {item.value for item in CaseCategory}:
        raise _labels_invalid(f"{field}.case", "is not a known case category", case=case)
    return Label(
        thread_id=_require_text(entry, "thread_id", field, _labels_invalid),
        comment_id=_require_text(entry, "comment_id", field, _labels_invalid),
        question_id=_require_text(entry, "question_id", field, _labels_invalid),
        expected=expected,
        case=CaseCategory(case),
        rationale=_require_text(entry, "rationale", field, _labels_invalid),
    )


def _parse_labels(value: Any) -> tuple[Label, ...]:
    raw = _require_list(value, "labels", _labels_invalid)
    labels = tuple(_parse_label(item, index) for index, item in enumerate(raw))
    seen: set[tuple[str, str, str]] = set()
    for index, label in enumerate(labels):
        key = (label.thread_id, label.comment_id, label.question_id)
        if key in seen:
            raise _labels_invalid(
                f"labels[{index}]",
                "repeats a (thread_id, comment_id, question_id) label",
                thread_id=label.thread_id,
                comment_id=label.comment_id,
                question_id=label.question_id,
            )
        seen.add(key)
    return labels


def load_label_set(manifest: EvaluationManifest, manifest_dir: Path) -> LabelSet:
    raw = _read_bytes(manifest_dir / manifest.labels.path, "evaluation labels")
    actual = _sha256(raw)
    if actual != manifest.labels.sha256:
        raise _refusal(
            "labels_digest_mismatch",
            "evaluation labels bytes do not match the manifest digest",
            path=manifest.labels.path,
            expected=manifest.labels.sha256,
            actual=actual,
        )
    payload = decode_strict_json_object(raw, DecisionEvaluationError, "evaluation labels")
    _require_kind_and_version(payload, EVALUATION_LABELS_KIND, _labels_invalid, "labels_invalid")
    top = _require_object(payload, "labels document", _LABELS_KEYS, _labels_invalid)
    return LabelSet(
        sha256=actual, origin=_parse_origin(top["origin"]), labels=_parse_labels(top["labels"])
    )


def _normalized_text_sha256(body: str) -> str:
    return _sha256(" ".join(body.casefold().split()).encode("utf-8"))


def _load_thread(
    entry: ManifestThread, manifest_dir: Path, sampling: SamplingLimits
) -> CorpusThread:
    raw = _read_bytes(manifest_dir / entry.file.path, "corpus thread")
    actual = _sha256(raw)
    if actual != entry.file.sha256:
        raise _refusal(
            "corpus_snapshot_mismatch",
            f"corpus thread {entry.thread_id} bytes do not match the manifest digest",
            thread_id=entry.thread_id,
            path=entry.file.path,
            expected=entry.file.sha256,
            actual=actual,
        )
    payload = decode_strict_json_object(raw, DecisionEvaluationError, "corpus thread")
    thread = canonical_thread_from_payload(payload)
    if thread.thread_id != entry.thread_id:
        raise _refusal(
            "corpus_thread_mismatch",
            "corpus thread id differs from its manifest entry",
            thread_id=entry.thread_id,
            actual=thread.thread_id,
            path=entry.file.path,
        )
    if len(thread.comments) > sampling.max_comments_per_thread:
        raise _refusal(
            "sampling_limit_exceeded",
            f"corpus thread {entry.thread_id} exceeds the comment sampling limit",
            field="sampling.max_comments_per_thread",
            thread_id=entry.thread_id,
            limit=sampling.max_comments_per_thread,
            actual=len(thread.comments),
        )
    comments = tuple(
        CorpusComment(
            thread_id=entry.thread_id,
            comment_id=comment.comment_id,
            index=index,
            text_sha256=comment_text_sha256(comment.body),
            normalized_text_sha256=_normalized_text_sha256(comment.body),
            language=entry.language_of(comment.comment_id),
            chars=len(comment.body),
        )
        for index, comment in enumerate(thread.comments)
    )
    return CorpusThread(
        entry=entry,
        thread=thread,
        snapshot_sha256=actual,
        thread_text_sha256=thread_text_sha256(thread.title, thread.body),
        comments=comments,
    )


def _comment_keys(corpus: Sequence[CorpusThread]) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    for item in corpus:
        for comment in item.comments:
            key = (item.entry.thread_id, comment.comment_id)
            if key in keys:
                raise _refusal(
                    "duplicate_comment_id",
                    f"corpus thread {key[0]} repeats comment id {key[1]}",
                    thread_id=key[0],
                    comment_id=key[1],
                )
            keys.add(key)
    return keys


def _check_corpus_references(manifest: EvaluationManifest, keys: set[tuple[str, str]]) -> None:
    for index, entry in enumerate(manifest.threads):
        for comment_id, _language in entry.comment_languages:
            if (entry.thread_id, comment_id) not in keys:
                raise _manifest_invalid(
                    f"threads[{index}].comment_languages",
                    "names a comment that is not in the thread",
                    thread_id=entry.thread_id,
                    comment_id=comment_id,
                )
    for group in manifest.issue_groups:
        for thread_id, comment_id in group.members:
            if (thread_id, comment_id) not in keys:
                raise _manifest_invalid(
                    "issue_groups",
                    "names a comment that is not in the corpus",
                    group_id=group.group_id,
                    thread_id=thread_id,
                    comment_id=comment_id,
                )


def load_corpus(manifest: EvaluationManifest, manifest_dir: Path) -> tuple[CorpusThread, ...]:
    if len(manifest.threads) > manifest.sampling.max_threads:
        raise _refusal(
            "sampling_limit_exceeded",
            "evaluation manifest lists more threads than its sampling limit",
            field="sampling.max_threads",
            limit=manifest.sampling.max_threads,
            actual=len(manifest.threads),
        )
    corpus = tuple(
        _load_thread(entry, manifest_dir, manifest.sampling) for entry in manifest.threads
    )
    _check_corpus_references(manifest, _comment_keys(corpus))
    return corpus


def _expected_fits(question: Question, value: Any) -> bool:
    if isinstance(question, ChoiceQuestion):
        return isinstance(value, str) and value in question.labels
    if isinstance(question, ScoreQuestion):
        is_int = isinstance(value, int) and not isinstance(value, bool)
        return is_int and 0 <= value < len(question.levels)
    return isinstance(value, bool)


def validate_labels_against_corpus(
    labels: LabelSet, corpus: Sequence[CorpusThread], question_set: QuestionSet
) -> None:
    keys = {
        (item.entry.thread_id, comment.comment_id) for item in corpus for comment in item.comments
    }
    questions = question_set.by_id()
    for index, label in enumerate(labels.labels):
        field = f"labels[{index}]"
        identity = {
            "thread_id": label.thread_id,
            "comment_id": label.comment_id,
            "question_id": label.question_id,
        }
        if (label.thread_id, label.comment_id) not in keys:
            raise _labels_invalid(field, "references a comment outside the corpus", **identity)
        question = questions.get(label.question_id)
        if question is None:
            raise _labels_invalid(field, "references an unknown question", **identity)
        if not _expected_fits(question, label.expected):
            raise _labels_invalid(
                f"{field}.expected",
                f"does not fit the {question.question_type.value} question",
                expected=label.expected,
                **identity,
            )


def _partitions(thread_ids: Sequence[str], by_thread: Mapping[str, Partition]) -> list[str]:
    return sorted({by_thread[thread_id].value for thread_id in thread_ids})


def _check_thread_partition(
    manifest: EvaluationManifest, violations: list[dict[str, Any]]
) -> IntegrityCheck:
    assigned: dict[str, list[str]] = {}
    for entry in manifest.threads:
        assigned.setdefault(entry.thread_id, []).append(entry.partition.value)
    for thread_id, partitions in assigned.items():
        if len(partitions) > 1:
            violations.append(
                {"check": "thread_partition", "thread_id": thread_id, "partitions": partitions}
            )
    return IntegrityCheck(
        name="thread_partition",
        groups_checked=len(manifest.threads),
        detail="every thread id is assigned to exactly one partition",
    )


def _check_copy_groups(
    name: str,
    groups: Mapping[tuple[str, str], list[Member]],
    by_thread: Mapping[str, Partition],
    violations: list[dict[str, Any]],
) -> int:
    checked = 0
    for (basis, digest), members in groups.items():
        thread_ids = [member["thread_id"] for member in members]
        if len(set(thread_ids)) < 2:
            continue
        checked += 1
        partitions = _partitions(thread_ids, by_thread)
        if len(partitions) > 1:
            violations.append(
                {
                    "check": name,
                    "basis": basis,
                    "sha256": digest,
                    "members": members,
                    "partitions": partitions,
                }
            )
    return checked


def _check_thread_copies(
    corpus: Sequence[CorpusThread],
    by_thread: Mapping[str, Partition],
    violations: list[dict[str, Any]],
) -> IntegrityCheck:
    groups: dict[tuple[str, str], list[Member]] = {}
    for item in corpus:
        member = {"thread_id": item.entry.thread_id}
        groups.setdefault(("snapshot_sha256", item.snapshot_sha256), []).append(member)
        if item.thread.title or item.thread.body:
            key = ("thread_text_sha256", item.thread_text_sha256)
            groups.setdefault(key, []).append(member)
    checked = _check_copy_groups("thread_copies", groups, by_thread, violations)
    return IntegrityCheck(
        name="thread_copies",
        groups_checked=checked,
        detail="threads with identical bytes or identical title and body share one partition",
    )


def _check_comment_copies(
    corpus: Sequence[CorpusThread],
    by_thread: Mapping[str, Partition],
    violations: list[dict[str, Any]],
) -> IntegrityCheck:
    excluded_digests = {_normalized_text_sha256(body) for body in ("", "[deleted]", "[removed]")}
    groups: dict[tuple[str, str], list[Member]] = {}
    for item in corpus:
        for comment in item.comments:
            if comment.normalized_text_sha256 in excluded_digests:
                continue
            key = ("normalized_text_sha256", comment.normalized_text_sha256)
            member = {"thread_id": comment.thread_id, "comment_id": comment.comment_id}
            groups.setdefault(key, []).append(member)
    checked = _check_copy_groups("comment_copies", groups, by_thread, violations)
    return IntegrityCheck(
        name="comment_copies",
        groups_checked=checked,
        detail="comments copied across threads share one partition; in-thread copies are allowed",
    )


def _check_issue_groups(
    manifest: EvaluationManifest,
    by_thread: Mapping[str, Partition],
    violations: list[dict[str, Any]],
) -> IntegrityCheck:
    for group in manifest.issue_groups:
        partitions = _partitions([thread_id for thread_id, _ in group.members], by_thread)
        if len(partitions) > 1:
            violations.append(
                {
                    "check": "issue_groups",
                    "group_id": group.group_id,
                    "members": [
                        {"thread_id": thread_id, "comment_id": comment_id}
                        for thread_id, comment_id in group.members
                    ],
                    "partitions": partitions,
                }
            )
    return IntegrityCheck(
        name="issue_groups",
        groups_checked=len(manifest.issue_groups),
        detail="every authored repeated-issue group lies within one partition",
    )


def check_partition_integrity(
    manifest: EvaluationManifest, corpus: Sequence[CorpusThread]
) -> PartitionIntegrity:
    by_thread = {item.entry.thread_id: item.entry.partition for item in corpus}
    violations: list[dict[str, Any]] = []
    checks = (
        _check_thread_partition(manifest, violations),
        _check_thread_copies(corpus, by_thread, violations),
        _check_comment_copies(corpus, by_thread, violations),
        _check_issue_groups(manifest, by_thread, violations),
    )
    if violations:
        raise _refusal(
            "partition_leak",
            "evaluation dataset partitions share copies or repeated issues",
            violations=violations,
        )
    return PartitionIntegrity(checks=checks)
