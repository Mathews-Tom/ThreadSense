# Artifacts

ThreadSense persists artifacts at each major stage so runs are inspectable and rerunnable.

## Storage Root

Default root:

```text
.threadsense
```

Override with:

```text
THREADSENSE_STORAGE_ROOT
```

## Raw Artifacts

Source-specific fetched payloads.

Examples:

- `.threadsense/raw/reddit/<thread-id>.json`
- `.threadsense/raw/hackernews/<story-id>.json`
- `.threadsense/raw/github_discussions/<thread-id>.json`
- `.threadsense/raw/github_gist/<gist-id>.json`

Purpose:

- preserve source payloads
- support replay without refetching
- support regression testing against real payload shapes

## Normalized Artifacts

Canonical thread artifacts.

Examples:

- `.threadsense/normalized/reddit/<thread-id>.json`
- `.threadsense/normalized/hackernews/<thread-id>.json`
- `.threadsense/normalized/github_discussions/<thread-id>.json`
- `.threadsense/normalized/github_gist/<gist-id>.json`

Purpose:

- source-agnostic `Thread` model
- stable downstream analysis input
- preserved thread-level body when available

## Analysis Artifacts

Deterministic thread analysis outputs.

Examples:

- `.threadsense/analysis/reddit/<thread-id>.json`
- `.threadsense/analysis/hackernews/<thread-id>.json`
- `.threadsense/analysis/github_discussions/<thread-id>.json`
- `.threadsense/analysis/github_gist/<gist-id>.json`

Purpose:

- persisted findings
- duplicate and phrase analysis
- provenance for later report generation

## Single-Thread Reports

Rendered outputs for one analyzed thread.

Examples:

- `.threadsense/reports/reddit/<thread-id>.json`
- `.threadsense/reports/reddit/<thread-id>.md`
- `.threadsense/reports/reddit/<thread-id>.html`

Purpose:

- operator-ready report
- structured JSON report artifact
- richer executive summary fields

## Corpus Artifacts

Cross-thread analysis artifacts.

Example layout:

```text
.threadsense/corpora/<corpus-id>/manifest.json
.threadsense/corpora/<corpus-id>/analysis.json
.threadsense/corpora/<corpus-id>/report.md
```

Purpose:

- manifest of selected analysis artifacts
- cross-thread findings and trends
- corpus-level report and optional synthesis

## Batch Artifacts

Reproducible batch-run outputs.

Example:

```text
.threadsense/batches/<run-name>.json
```

## Monitoring State

Saved watches live in a single SQLite database, separate from the JSON artifacts above:

```text
.threadsense/monitoring/state.sqlite3
```

The directory name comes from `monitoring_dirname` under `[storage]` (environment override `THREADSENSE_STORAGE_MONITORING_DIR`, default `monitoring`). It must be a relative path that stays inside the storage root. The database is created by the first `watch create`; `watch list` and `watch inspect` open it read-only and never create it. A zero-byte or otherwise uninitialized file reads as an empty store.

The database carries a ThreadSense application id and a schema version, currently `3`. A version `1` or `2` database is upgraded in place by the first command that writes to it: the upgrade only creates the missing tables and index, in one transaction, and keeps every existing profile, revision, run and sweep record. Read-only commands read a version `1` or `2` database without modifying it; `watch inspect` then reports an empty `digest` summary. A database written by a newer schema version, a SQLite file that is not a ThreadSense monitoring database, and a file that is not SQLite are all refused with `monitoring_state_error` and left unmodified. There is no downgrade path. An existing completely empty SQLite file (no tables, no application id) is treated as uninitialized and receives the schema on the first write.

If a crash left `state.sqlite3-journal` next to the database, any watch command, including `watch list` and `watch inspect`, lets SQLite roll back the interrupted transaction, but only after verifying that a private recovered copy is a supported ThreadSense state. Otherwise the database and journal are left untouched.

Tables:

- `watches`: one row per watch with its current revision number and creation and update times
- `watch_revisions`: one row per saved profile revision with the canonical profile JSON and its SHA-256 content hash; earlier revisions are kept
- `watch_runs`: one row per run claim with the profile revision it used, status (`running`, `succeeded`, `failed`, `abandoned`), claim, lease-expiry and finish times, the SHA-256 of the owner token, and `recovered_by_run_id` when a later claim took over an expired run; a partial unique index allows at most one `running` row per watch

Sweep tables (schema version `2`):

- `watch_threads`: one row per discovered thread with its URL, subreddit, title, search `created_utc`, whether it is selected (tracked) or its `rejection_reason`, first and last discovery times, last attempted and last successful refresh times, its latest acquisition and its current revision
- `watch_discoveries`: every capped search hit of every sweep with its query, rank, score, comment count and selection result
- `watch_acquisitions`: one row per successful fetch with its observation time, relative paths and SHA-256 of the raw and normalized files, `completeness` (`truncated` or `unknown`), cache status, parsed and Reddit-reported comment counts, the number of known comments absent from it, and the revision it created, if any
- `watch_fetch_failures`: failed searches, fetches and analyses with their stage, target and error
- `watch_comments`: one row per known comment with its current text hash, source `created_utc`, `novelty` at first observation (`initial_observation`, `new_source_event` or `newly_observed_historical`), first-observed time, run and acquisition, and last-seen time and acquisition
- `watch_comment_texts`: the text-hash history of each comment, one row per first observation or detected edit with the acquisition and observation time
- `watch_thread_revisions`: immutable substantive revisions with their sequence number, previous revision, substantive and thread-text hashes, snapshot path and SHA-256, the acquisition and run that created them, and their comment changes
- `watch_revision_analyses`: the analysis artifact of each analyzed revision with its path and SHA-256
- `watch_sweeps`: one row per finished sweep with its outcome (`changes`, `no_changes`, `partial` or `failed`) and summary

Digest tables (schema version `3`):

- `watch_digests`: one row per digest with its per-watch `sequence`, the run that created it, `status` (`pending` until both files are verified, then `written`), the entry ids and backlog snapshot it was created with, the relative paths and SHA-256 of its JSON and Markdown files, and creation and written times; a partial unique index allows at most one `pending` digest per watch
- `watch_digest_entries`: one row per digest entry with its kind (`thread_update` or `finding`), source thread, revision, theme key, canonical payload and creation run; `digest_id` and `presented_at_utc` stay empty until the digest that presents the entry is written

Sweep files live under the monitoring directory and are never overwritten. Paths in the database are relative to the monitoring directory:

```text
watches/<watch_id>/threads/<thread_id>/acquisitions/<acquisition_id>.raw.json
watches/<watch_id>/threads/<thread_id>/acquisitions/<acquisition_id>.normalized.json
watches/<watch_id>/threads/<thread_id>/revisions/<revision_id>/<snapshot_sha256>.json
watches/<watch_id>/threads/<thread_id>/analyses/<revision_id>/<run_id>.json
```

Digest files are written beside the sweep files, are never overwritten, and are named by sequence and digest id:

```text
watches/<watch_id>/digests/<sequence>-<digest_id>.json
watches/<watch_id>/digests/<sequence>-<digest_id>.md
```

The sequence is zero-padded to six digits. A database row marks a digest `written`, and its entries presented, only after both files exist inside the monitoring directory with matching SHA-256. A crash between the file write and that commit leaves a `pending` digest; the next `watch digest` or `watch run` rewrites byte-identical files for the same digest or reuses them, and a file at the path with different bytes is refused as `monitoring_state_error` (reason `artifact_conflict`).

The digest JSON artifact (`artifact_type: watch_digest`, `schema_version: 1`) has `watch_id`, `digest_id`, `sequence`, `created_at_utc`, `entries` and `backlog`. It is written with sorted keys, two-space indentation and a trailing newline, and rendered only from stored digest data, so a resumed digest is byte-identical. Each entry has `kind`, `entry_id`, `watch_id`, `source_thread_id`, `thread_url`, `subreddit`, `title`, `revision_id`, `revision_sequence`, `snapshot_path`, `snapshot_sha256`, `analysis_path`, `analysis_sha256` and `thread_text_changed`; a `thread_update` entry adds `novel_comments` and `counts` (every change kind with its count, including zeros). Each novel comment has `comment_id`, `kind` (`initial_observation`, `new_source_event`, `newly_observed_historical` or `edited`), `text_sha256`, `previous_text_sha256`, `source_created_utc`, `source_edited_at_utc` (present only on `edited` comments and always `null`; no source edit time is recorded), `first_observed_at_utc` or, for edits, `observed_at_utc`, `permalink` and a whitespace-collapsed `excerpt` of at most 280 characters. A `finding` entry adds `theme_key`, `theme_label`, `severity`, `finding_comment_count`, `novel_evidence_comment_ids` and `novel_evidence` (the novel comments it cites, in the same shape). `backlog` has `presentation_limit`, `presented_in_digest`, `unpresented_entries_after`, `pending_analyses`, `tracked_threads`, `refresh_pending`, `latest_sweep_outcome` and `search_completeness` (always `unknown`). Snapshot and analysis paths are relative to the monitoring directory.

The Markdown artifact has a title with the watch id and sequence, a backlog section and one section per entry. Source text is untrusted: excerpts are block quotes with markdown control characters escaped.

The raw file is the connector's raw thread artifact and the acquisition id is derived from its SHA-256. A revision snapshot is a canonical thread artifact holding every comment known for the thread at that revision, including comments absent from the latest acquisition; its provenance points at the raw acquisition file that created it. An analysis artifact's provenance points at the immutable snapshot it analyzed, not at a mutable latest path. Files from an interrupted sweep that the database never referenced are kept, not deleted.

Timestamps are stored as Unix epoch seconds (UTC) floating-point values. The content hash excludes timestamps, so saving identical profile content again does not write a new revision.

Inspection payloads emitted by the CLI:

`watch create` (`artifact_type: watch_profile`): `status` (`created`, `revised` or `unchanged`), `watch_id`, `revision`, `content_sha256`, `state_path`.

`watch list` (`artifact_type: watch_list`): `status`, `state_path`, and `watches`, each with `watch_id`, `name`, `current_revision`, `content_sha256`, `created_at_utc`, `updated_at_utc`.

`watch inspect` (`artifact_type: watch_inspection`): `status`, `state_path`, `watch_id`, `current_revision`, `created_at_utc`, `updated_at_utc`, `profile`, `revisions` (each with `revision`, `content_sha256`, `created_at_utc`), `active_run`, `run_count`, `recent_runs` (newest 20, each with `run_id`, `profile_revision`, `status`, `claimed_at_utc`, `lease_expires_at_utc`, `finished_at_utc`, `recovered_by_run_id`), and `digest` (`entry_count`, `unpresented_entries`, `pending_digest_id`, `digest_count`, `latest_digest` with `digest_id`, `sequence`, absolute `json_path` and `markdown_path`, `entry_count` and `written_at_utc`, or `null`), and `sweep` with `tracked_threads`, `refresh_due_never_refreshed`, `pending_analyses`, `revision_count`, `comment_count` and `latest_sweep` (`run_id`, `outcome`, `finished_at_utc`, `summary`, or `null`).

`watch run` (`artifact_type: watch_run`): `status` (the sweep outcome), `state_path`, `sweep` and `digest`.

`watch digest` (`artifact_type: watch_digest_result`): `status` (`presented` or `no_entries`), `state_path`, `run_id`, `watch_id`, `digest_id`, `sequence`, `resumed`, `json_path`, `markdown_path`, `entry_ids`, `materialized_entries` and `backlog`.

`watch schedule` (`artifact_type: watch_schedule`): `status` (`rendered`), `format`, `watch_id`, `installed` and `enabled` (both `false`), `executable`, `workspace`, `storage_root`, `config_path`, `interval_seconds`, `lease_seconds`, `argv`, `log_path`, `overlap_policy`, `content` and `install_instructions`. It writes no file.

See [usage.md](usage.md) for the profile format and command behavior.

## Research Result Payloads

`research reddit` also returns runtime metadata in the emitted result payload, including:

- selected thread list
- manifest path
- corpus analysis path
- corpus report path
- terminal summary when available

This payload is emitted directly to stdout according to the selected output mode.

## Typed decision annotations

`threadsense.decisions.annotate.annotate_canonical_thread` reads one canonical thread artifact and writes shadow-only typed semantic annotations for every comment, produced by a separate loopback-only Laya service. The annotations never alter baseline findings, discovery membership, evidence counts or ranking. Every document carries `mode: shadow`, `policy: none` and `calibration: uncalibrated`. The six dimensions overlap by design, and none of them is a source-truth or factual-accuracy score. This section documents the library-level artifacts; the operator CLI is documented separately.

Layout, under a caller-chosen annotation root:

```text
<annotation-root>/<snapshot_sha256>/decisions/<decision_sha256>.json
<annotation-root>/<snapshot_sha256>/runs/<run_id>.json
```

The decision document (`artifact_kind: typed_decision_annotations`, `schema_version: 1`) holds deterministic content only and is content-addressed: `decision_sha256` is the SHA-256 of its bytes, so annotating an unchanged snapshot with an identical outcome reproduces the same file, and an identical existing file is reused. The run record (`artifact_kind: typed_decision_annotation_run`, `schema_version: 1`) holds everything that varies between runs. Both files are published atomically with no clobber: content is written to a synced temporary file and hard-linked into place, and an existing file with different bytes is refused as `artifact_conflict` and never overwritten. A run record at an existing path is always a conflict. The annotation root is refused when it lies inside a protected baseline or monitoring storage root (`output_inside_protected_storage`), when it is not a directory, or when it or any output path aliases the input artifact (`output_aliases_input`). The snapshot hash and decision hash must be 64 lowercase hex characters, and a run id must match `[0-9A-Za-z][0-9A-Za-z._-]{0,79}`; the default run id is a UTC timestamp plus 12 random hex characters.

### Decision document

The input is read exactly once and every later step works from those bytes. It must be a strict JSON object: duplicate keys, `NaN` and `Infinity` are refused.

| Field | Content |
| --- | --- |
| `artifact_kind`, `schema_version`, `mode`, `policy`, `calibration` | `typed_decision_annotations`, `1`, `shadow`, `none`, `uncalibrated` |
| `requirement` | `required` or `optional` |
| `input` | exact input byte hash (`sha256`) and `bytes`, canonical `artifact_kind`, `schema_version` and `normalization_version`, `thread_id`, `source` (`source_name`, `community`, `source_thread_id`, `thread_url`), `thread_text_sha256`, `comment_count` and `comments_present` |
| `backend` | `manifest_sha256`, the path-free `manifest`, `code_identity_basis`, `checkpoint_identity_basis` and `checkpoint_verification` |
| `route` | `model` (`english`), `language` (`value: en`, `basis: declared`) and `detection` (always `null`) |
| `questions` | `sha256` and the full question set: schema id and version, objective, domain vocabulary identity, and six questions with their wire form |
| `settings` | `sha256`, `model`, `lang`, `max_len`, `head_max_len` |
| `context_policy` | `sha256` and the character windows described below |
| `coverage` | aggregate `status`, `total`, `ok`, `refused`, `unavailable` |
| `items` | one entry per comment in snapshot order |
| `exchanges` | one entry per batch request, ordered by batch index |

Backend identity is never taken from the service's health revision, which may be `null`. The code identity is declared by the manifest and attested by the reuse procedure (`declared_by_manifest_attested_by_reuse_procedure`). The checkpoint identity is verified by the client: it hashes the files the manifest lists inside the checkpoint directory the service reports and records the observed digests (`client_file_sha256`). `checkpoint_verification` is `null` when no result passed checkpoint verification, including when no result was received or all checkpoints were refused; when set, `repo` records the first verified directory.

The six questions, in order: `objective_relevance` (score, three levels), `evidence_type` (choice), `request_intent` (choice), `actionability` (score, three levels), `promotional_framing` (noul) and `theme` (choice over the domain vocabulary themes plus `other_or_uncertain`; the vocabulary default theme is deliberately not an option). The question-set hash is order-sensitive: theme order, option order and question order all change it.

Each item has `index`, `comment_id`, `parent_comment_id`, `comment_text_sha256`, `parent_text_sha256`, `parent_context` (`none`, `included` or `missing_from_snapshot`), `status`, `reasons`, `failure`, `batch_index`, `state_sha256`, `state_chars`, `context_windows`, `usage`, `routing` and `answers`. An item has answers exactly when its status is `ok`; every other status states at least one reason. Run-varying values (timings, run ids, endpoint) never appear in the decision document, and the `wall_seconds` and `endpoint` details are stripped from recorded errors.

`exchanges` keeps the verbatim request body text, its SHA-256, the outcome (`response` or `error`), the status code and the verbatim response text with its SHA-256, or the error. These stay apart from the validated answers in `items`.

### Statuses and reasons

Item `status` is `ok`, `refused` or `unavailable`. The aggregate `coverage.status` is `complete` when every item is `ok`, `unavailable` when none is, and `partial` otherwise.

Refused (`reasons` explain why no answer is used; a refused item that reached the service keeps its `usage` and `routing`, and its raw answers remain only in the exchange):

- `comment_exceeds_client_char_budget`: the comment is longer than 2000 characters; nothing is sent
- `state_exceeds_server_char_limit`: the serialized state is longer than 50,000 characters; nothing is sent
- `server_dropped_state_tokens`: the service reported dropped state tokens
- `server_reported_truncation`: the service reported `truncated`
- `server_truncated_questions`: the service listed truncated questions
- `server_reported_option_usage`: the service reported option usage

Unavailable (`failure` carries the reason, message and details):

- `runtime_health_unavailable`: the health check failed or did not match the manifest, so no batch is sent
- `runtime_request_failed`: the batch request failed
- `invalid_response`: the batch response is not a well-formed batch, or its result count differs from the request
- `invalid_typed_answer`: a result failed validation
- `checkpoint_identity_unverified`: the checkpoint files the service reported cannot be inspected or read, or do not match the manifest digests; filesystem access failures are typed identity failures and are cached for the run
- `checkpoint_repo_changed`: the service answered from a different checkpoint directory than earlier in the same run

A single comment whose state alone exceeds the 2 MiB request body is not an item status; it aborts the run with `state_exceeds_request_body_limit`.

`requirement: required` runs whose coverage is not `complete` are unsatisfied, and callers exit nonzero. For `optional` runs `satisfied` is always `true`; unavailability and refusals stay visible in the document, with no answers on the affected items.

### Answer semantics

Each answer has `question_id`, `type`, one value field, `probabilities`, `confidence_quantities` and `calibration: uncalibrated`. Validation is internal consistency only: the server's values are never recomputed, coerced or dropped.

- **choice**: `choice` is the server's choice and must be a most probable option. `probabilities` is the full distribution as an ordered list of `[label, probability]` pairs in the requested option order.
- **score**: `score` is the expected ordinal level in `[0, k-1]` for `k` levels, not a probability. `probabilities` is the distribution over levels `"0"` to `"k-1"` in order.
- **noul**: `noul` is `P(true)`, and `probabilities` is empty.
- **Confidence quantities** are named and type-specific, never merged. `answer_confidence` is the reported probability of the selected option (max p) for choice and score, and `max(P(true), 1 - P(true))` for noul. For choice and score the separate `confidence` quantity is a normalized-entropy concentration of the distribution and is not on the `answer_confidence` scale; for noul `confidence` shares the `answer_confidence` definition. All of them are uncalibrated.
- Raw `action` and `x_jev_confidence` fields are accepted only as optional evidence. They appear solely in the verbatim exchange and are unused.

The service rounds every probability and confidence to four decimals, so checks allow rounding slack with `q = 5e-5` (plus `1e-9`): probability mass may differ from one by `k * q`, a score may differ from the expected level of its distribution by `(sum_i i + 1) * q` with `i` over `0..k-1`, and each confidence may differ from its target by `q`.

### Context and request shape

Attribution is comment-local: every instruction tells the service to judge only the `comment` field, with `thread_title`, `thread_body` and `parent_comment` as context written by others. Context fields are prefix windows `[0, included_chars)` of title, thread body and parent comment, limited to 300, 600 and 400 characters, and each item records `field`, `source_chars`, `included_chars` and `complete` for every window it used. The comment itself is never windowed or trimmed: above 2000 characters it is refused before sending.

Requests are bounded by the pinned service limits, enforced by the client before sending: at most 64 states per request, 64 questions, 50,000 characters per state and a 2 MiB request body. Larger comment sets are split into several batches. Token settings are `max_len: 512` and `head_max_len: 256`, with `0 < head_max_len <= max_len <= 512`. The `sort_by_length`, `batch_size` and `min_confidence` controls are never sent, so only ordinary batching applies. Results are associated with comments strictly by index within the batch, never by content, and requests are not retried.

### Run record

The run record holds `run_id`, `decision_sha256`, the relative `decision_path`, `endpoint`, `started_at_utc` and `completed_at_utc` (Unix epoch seconds), the `health` observation (or the health error), `requests` (per call `batch_index`, `outcome`, `status_code` and client-observed `wall_seconds`, with `batch_index: null` for the health call), `total_http_wall_seconds`, `coverage`, `requirement` and `satisfied`. Its `observation_scope` states that only client-side HTTP wall time is recorded and that model-server memory is not observed by this client.

### Typed decision evaluation

`threadsense decisions evaluate` (library entry point `threadsense.decisions.evaluate.evaluate_frozen_assessment`) assesses saved decision documents against a frozen dataset manifest and its labels. Typed replay is offline: it never contacts Laya, re-annotates or alters a saved decision. Fresh generative theme comparison may contact the configured local chat runtime; supplying `--generative-capture PATH` instead replays an immutable capture offline. Frozen assessment bytes are deterministic for the same bound inputs and capture, not for repeated fresh model acquisitions.

Layout, under the evaluation output root (by default the annotation root):

```text
<output-root>/evaluations/<manifest_sha256>/<evaluation_sha256>.json
<output-root>/evaluations/<manifest_sha256>/runs/<run_id>.json
<output-root>/evaluations/<manifest_sha256>/generative-captures/<capture_file_sha256>.json
```

`manifest_sha256` is the SHA-256 of the manifest bytes and `evaluation_sha256` the SHA-256 of the evaluation bytes (`json.dumps(indent=2)` plus a trailing newline). Publication follows the annotation rules: atomic and no-clobber, an identical existing evaluation is reused, and a run record at an existing path is a conflict (`artifact_conflict`). Both hashes must be 64 lowercase hex characters and the run id must match the annotation run-id pattern (`artifact_name_invalid`). The `evaluations`, `<manifest_sha256>` and `runs` directories must not be symbolic links (`artifact_directory_symlink`). The output root is refused inside or around protected storage (`output_inside_protected_storage`, `output_contains_protected_storage`), and no output path may alias the manifest, the label file, a thread file or an annotation (`output_aliases_input`).

#### Dataset manifest

An authored strict JSON object with exactly these keys; any unknown or missing key is `manifest_invalid` with the offending `field`.

| Field | Content |
| --- | --- |
| `artifact_kind`, `schema_version` | `typed_decision_evaluation_manifest`, `1` (another version is `manifest_version_unsupported`) |
| `dataset_id`, `description` | Free text identifying the dataset |
| `objective`, `domain` | An analysis objective and a non-custom domain; together they rebuild the question set |
| `bindings` | `runtime_manifest_sha256`, `checkpoint_revision`, `question_set_sha256`, `settings_sha256`, `context_policy_sha256`, `language` |
| `supported_languages` | Exactly the language in `bindings.language`; other declared comment languages are excluded |
| `sampling` | `method`, `selection`, positive `max_threads` and `max_comments_per_thread`; a corpus beyond either limit is `sampling_limit_exceeded` |
| `labels` | `path` and `sha256` of the label file |
| `threads` | Per thread: `thread_id`, `path`, `sha256`, `partition` (`development`, `calibration` or `evaluation`), `source_language` and `comment_languages` (comment id to declared language) |
| `issue_groups` | Authored repeated-issue groups: `group_id`, at least two `members` (`thread_id`, `comment_id`) and a `note`; a comment belongs to at most one group |

Paths are relative POSIX paths resolved against the manifest's directory; `..` is allowed, absolute paths and empty or `.` parts are not. Every bound file is read once and its SHA-256 must match the manifest (`corpus_snapshot_mismatch`, `labels_digest_mismatch`). A thread file must be a canonical thread artifact whose thread id equals the manifest entry (`corpus_thread_mismatch`), and a comment id may appear only once per thread (`duplicate_comment_id`). Languages are declared, never detected: a comment's language is its `comment_languages` entry or else the thread's `source_language`.

#### Labels

| Field | Content |
| --- | --- |
| `artifact_kind`, `schema_version` | `typed_decision_evaluation_labels`, `1` |
| `origin` | `kind` (`engineering_authored`, `model_generated`, `independent_adjudication` or `constructed_test_fixture`), `authored_by`, `method`, `disclosure` |
| `labels` | Per label: `thread_id`, `comment_id`, `question_id`, `expected`, `case`, `rationale` |

`expected` is an option label for a choice question, an integer level (not a boolean) for a score question and a boolean for the noul question. `case` is one of `baseline`, `negation`, `quoted_claim`, `mixed_intent`, `repetition`, `long_context_boundary`, `option_relabeling`, `unsupported_language`. A label must reference a corpus comment and a question id, and there is at most one label per comment and question; violations, and a label file with another `artifact_kind` or `schema_version`, are `labels_invalid`.

Label provenance is carried into the document unchanged. Only `independent_adjudication` sets `independent_adjudication: true`. Engineering-authored labels, such as the shipped dataset under `tests/fixtures/decisions/evaluation/`, are expectations written for boundary cases: they are not independent adjudication, not representative of any population and not ground truth.

#### Identity and annotation binding

Before assessment the manifest `bindings` must equal the current pinned identities: the runtime manifest SHA-256, the checkpoint revision, the question set rebuilt from `objective` and `domain`, the pinned settings and context policy hashes, and the settings language. Any difference is `stale_binding` with `field`, `bound` and `current`.

Each `--annotation` is strictly re-validated: its file name must be the SHA-256 of its bytes (`annotation_digest_mismatch`), its bytes must be the canonical serialization (`annotation_not_canonical`), and its shape, coverage and route must be consistent (`annotation_invalid`, `annotation_version_unsupported`, `annotation_coverage_mismatch`). Malformed exchange records, and `settings` or `context_policy` objects whose recomputed digest differs from their saved `sha256`, are `annotation_invalid`. Annotations are matched to manifest threads by snapshot SHA-256: an annotation for an unlisted snapshot is `annotation_unbound`, two for one snapshot are `annotation_duplicate`, and a manifest thread without one is `annotation_missing`. Each annotation's identities, objective and domain must equal the manifest (`annotation_identity_mismatch` with `field`); an annotation with at least one `ok` item whose `backend.checkpoint_verification` is absent or does not equal the pinned checkpoint files is `annotation_identity_mismatch` with `field: checkpoint_verification` (a run with no `ok` items, such as one whose health check failed, may carry a null verification), and its items' comment ids and comment text hashes must equal the corpus thread in order (`annotation_corpus_mismatch`).

Every item that received a recorded `200` response is replayed offline: the recorded request and response digests are re-checked (`replay_exchange_digest_mismatch`), the recorded response is re-validated with the shipped validators, and the re-derived status, reasons, usage, routing and answers must equal the saved item (`replay_mismatch`; `replay_identity_mismatch` when the question set or settings differ, including a saved `questions` object that differs from the question set rebuilt from the manifest, with `field: questions`). Items never sent, or whose batch failed, are listed as not replayable with their saved reason.

#### Partition integrity

Checks run in order and every violation is collected; any violation refuses the run as `partition_leak` with `details.violations`.

| Check | Rule |
| --- | --- |
| `thread_partition` | Each thread id appears in exactly one partition |
| `thread_copies` | Threads sharing snapshot bytes or a non-empty title and body share a partition |
| `comment_copies` | Comments with the same casefolded, whitespace-collapsed text in different threads share a partition; copies inside one thread are allowed |
| `issue_groups` | All members of an authored repeated-issue group share a partition |

The document records each passed check with `groups_checked` and a short `detail`.

#### Evaluation document

`artifact_kind: typed_decision_evaluation`, `schema_version: 1`, `mode: shadow`, `calibration: uncalibrated`, `policy: none`. It holds no timestamps, run ids, endpoints, wall times or local paths. Floats are rounded to 6 decimals; lists follow manifest thread order, then comment index, then question order.

| Field | Content |
| --- | --- |
| `claims` | `result_scope: engineering_assessment`, `population_accuracy: not_claimed`, `label_origin`, `independent_adjudication`, `calibration_fitted: false` and a note |
| `dataset` | Manifest hash and descriptive fields, label hash, origin, count and `by_case`, per-thread partition, hashes, comment count and declared languages, and issue groups with their partition |
| `identity` | The bound identities plus `language_basis: declared` |
| `decision_rules` | Score, noul, abstention and language-exclusion rules |
| `partition_integrity` | The passed checks above |
| `annotations` | Per thread: snapshot and decision hashes, requirement, saved coverage and the replay report |
| `partitions` | `development`, `calibration` and `evaluation`, always all three |
| `audit` | `rejected`, `excluded` and `abstained` item lists |
| `engineering_cases` | Per labeled case: outcome counts and per-label items |
| `comparators` | `heuristic` and `generative` |
| `option_stability` | Always `vacuous`; see below |

Decision rules: a choice answer's decision is its `choice`, and `other_or_uncertain` is an abstention, not a decision. A score answer's decision is the argmax level of its reported distribution with ties to the lowest level; the reported `score` is an expected ordinal level and is never used as the decision. A noul answer decides `true` when the reported P(true) is at least `0.5`. None of these values is a probability of correctness.

Each partition has a `population` and per-question metrics. Population counts: `threads`, `comments`, `ok`, `refused`, `unavailable`, `excluded_language` and `eligible`, with `comments = ok + refused + unavailable` and `eligible = ok - excluded_language`. Excluded items are `ok` items whose declared language is not in `supported_languages`; they are never scored.

Per question:

| Field | Definition |
| --- | --- |
| `population`, `eligible` | Partition comments and eligible comments |
| `abstained`, `decided` | Eligible answers that abstained or decided; `decision_rate = decided / population` |
| `labels.labeled` | Labels on this question in the partition |
| `labels.labeled_not_covered` | Labels on refused or unavailable items |
| `labels.labeled_excluded` | Labels on language-excluded items |
| `labels.labeled_abstained` | Labels on abstained items, including labels that are themselves `other_or_uncertain` |
| `labels.labeled_decided`, `correct`, `incorrect` | Labels on decided items, split by whether the decision equals `expected` |
| `labels.error_rate` | `incorrect / labeled_decided` |
| `labels.incorrect_items` | `thread_id`, `comment_id`, `expected`, `actual` |
| `confidence` | Mean `answer_confidence` over correct, incorrect and decided items; uncalibrated |
| `score_diagnostics` | Score questions: mean absolute error of the expected ordinal level against the labeled level over labeled decided items |
| `ranking` | Score questions: pairwise order agreement of expected levels over labeled decided pairs with different labeled levels (ties count 0.5) |
| `relevant_evidence_misses` | `objective_relevance` only: labeled-relevant (level at least 1) comments predicted not relevant, not covered or excluded |

A rate is `null` when its denominator is 0. `labels.status` describes whether labels are present, not whether labeled decisions exist. With no labels for a question it is `unavailable` with reason `no_labels_for_question`; even with labels present, error rate stays `null` if `labeled_decided` is zero. `ranking` is `unavailable` with `no_comparable_labeled_pairs` when no pair qualifies. Unavailable is not zero error. Labels on never-covered items are counted as not covered, never as errors.

The typed client's 2000-character budget is a preflight limit, not a service-success guarantee. In the frozen fixture, exactly-2000-character `fixture:lc-3` fits that budget but may be refused when the pinned checkpoint reports context truncation. Its labels then remain `not_covered`; the authored rationale does not promise acceptance or authorize scoring a refused item as an error.

Audit lists: `rejected` holds every refused or unavailable item with its `status` and `reasons`; `excluded` every language-excluded item with its `language` and reason `language_unsupported`; `abstained` every eligible choice abstention per question. Count reconciliation checks structural invariants among these lists and counts, refusing as `reconciliation_failed` on inconsistency. It does not independently prove input coverage: corpus/annotation alignment is separately validated against the bound corpus. Neither check measures prediction quality.

`engineering_cases` has one entry per case category that has labels, with `labels`, outcome counts (`correct`, `incorrect`, `abstained`, `not_covered`, `excluded`) and the per-label items; `actual` is `null` unless the item decided or abstained.

#### Comparators

`heuristic` compares the `theme` question with the existing keyword heuristic, offline and deterministically, on identical items: the typed-eligible comments (ok and supported language). The heuristic's default theme maps to `other_or_uncertain`, marker fallbacks keep their fallback theme, and empty or noise-filtered comments make no decision; `other_or_uncertain` and no decision count as abstentions. Per partition it reports item count, decided, abstained and label metrics for both sides, and the agreement rate where both decided. `basis_counts` counts which heuristic rule produced each outcome. The other questions are listed as unsupported with `no_existing_heuristic_counterpart`.

`generative` reuses the existing reclassification prompt, client and task validator for `theme` only. The prompt includes thread-title context and the existing domain themes; the cohort is restricted to typed-eligible, supported-language comments whose bodies are at most 300 characters, so the task does not truncate them. The full typed/heuristic assessment population is preserved. A separate paired cohort records exact thread/comment IDs, denominators, exclusions, abstentions and label availability, with typed, heuristic and generative metrics on those same IDs. Longer comments are unsupported, not losses. The other five dimensions have no existing generative task and remain unsupported. Default or novel themes map to `other_or_uncertain` abstention; malformed or mismatched response IDs cannot be silently dropped.

Fresh acquisition retains exact raw requests/responses, requested/returned model identity and task identity in a separate immutable, content-addressed capture, bound to the manifest, ordered annotation hashes, labels and question set. `--generative-capture PATH` validates the bytes' content identity, bindings and current request layout, then re-derives normalized outcomes from retained responses rather than trusting stored outcomes. It performs no model listing or inference. Its run record reports `available` / `replayed_frozen_capture`, with null `method` and `served_models`; nested `captured_availability` preserves the original acquisition observation without presenting it as a current runtime check. The capture path and complete-file SHA-256 are reported by the acquisition; retain its bytes for offline replay. A fresh run without that input is a new acquisition, not replay. Unavailable acquisition creates no fabricated capture or metrics; optional inference failures, including invalid numeric or decoded responses, are recorded as `inference_failed` while valid frozen typed/heuristic evidence is preserved. Invalid supplied captures instead refuse evaluation as `generative_capture_invalid`. Transient availability is run metadata, leaving frozen typed/heuristic metrics reproducible. All results remain shadow-only and uncalibrated; no policy is installed or activated.

Capture schema version `1` contains `bindings`, `runtime`, `availability`, `unsupported` and `batches`. Batches retain the task/messages and repair settings, raw exchanges (`url`, `request`, `status_code`, `response_text`), returned model and normalized rows. `availability` is the retained acquisition observation, not the current replay run's availability. `capture_sha256` hashes the canonical `decision_bytes` serialization of the capture with that field omitted: this envelope digest binds its contents and is retained in `comparators.generative.capture_sha256`. The SHA-256 of the complete serialized file is a separate publication identity used by the filename and CLI/run-record capture fields. Replay validates both identities and re-derives the rows; neither digest is evidence of current model availability.

Capture files are published under the layout above using the SHA-256 of their full bytes as `capture_file_sha256`. Publication is immutable/no-clobber and reuses only identical bytes; the capture directory must not be a symlink. The run record's `generative_comparator.capture_path` is relative to the output root and `generative_comparator.capture_sha256` is this complete-file hash. The CLI reports the capture's output path and the same file hash. These publication fields differ from the envelope digest in `comparators.generative.capture_sha256`.

For `--generative-capture PATH`, retain the content-addressed filename: `<capture_file_sha256>.json`. An unreadable capture or a filename stem that differs from the SHA-256 of its bytes is refused as `generative_capture_invalid` before publication. Copying or relocating unchanged bytes with their original filename does not change the capture identity.

The evaluation's `comparators.generative` records `comparison` (`performed` or `not_performed`), `capture_sha256`, `shape_support` and per-partition `partitions`. Each partition reports `items`, exact `ids`, the `excluded` count, `exclusions`, `typed`, `heuristic`, `generative` and pairwise `agreement`. Each method reports decided and abstained counts, explicit abstention IDs and label metrics; agreement rates use only pairs where both methods decided, with `both_decided` as their denominator. Without a frozen capture, `comparison` is `not_performed` with reason `no_frozen_generative_capture`; run metadata explains acquisition availability.

#### Option stability

`option_stability.status` is `vacuous`: each question set has one fixed option layout and no permuted-layout probe was run, so stability across option order is not measured. `layouts` lists every choice question's ordered options and `probes` is empty.

#### Evaluation run record

`artifact_kind: typed_decision_evaluation_run`, `schema_version: 1`, `run_id`, `evaluation_sha256`, the relative `evaluation_path`, `manifest_sha256`, `started_at_utc` and `completed_at_utc` (Unix epoch seconds), `wall_seconds`, `generative_comparator` and `observation_scope` (evaluator wall time only; no model-server memory is observed).

`generative_comparator` records the configured runtime and availability observation. Fresh acquisition lists models only when enabled and both the base URL and chat endpoint satisfy literal-loopback restrictions; requests use no environment proxies and follow no redirects. Disabled or invalid/nonliteral-endpoint guards report `not_observed`, not observed unavailability, because no request was made. Observed failures (`runtime_unreachable`, `listing_http_status`, `listing_invalid`) and `configured_model_not_served` remain explicitly unavailable. The default `gemma4:latest` is not guaranteed to be served. A successful listing is not evidence of model quality; comparison requires valid captured responses. Offline capture replay does not probe the runtime.

#### Refusal reasons

All refusals exit `1`. Every input, identity, binding, replay and integrity refusal happens before anything is published. Evaluation refusals use code `decision_evaluation_error`:

| Reason | Cause |
| --- | --- |
| `input_unreadable`, `invalid_json` | An input file cannot be read or is not strict JSON |
| `manifest_invalid`, `manifest_version_unsupported` | Manifest shape or version |
| `sampling_limit_exceeded` | Corpus exceeds the manifest sampling limits |
| `corpus_snapshot_mismatch`, `corpus_thread_mismatch` | Thread file bytes or thread id differ from the manifest |
| `duplicate_comment_id` | A corpus thread repeats a comment id |
| `labels_digest_mismatch`, `labels_invalid` | Label file bytes or content, including an unsupported label `schema_version` |
| `stale_binding` | Manifest bindings differ from the current pinned identities |
| `partition_leak` | A partition integrity check failed |
| `annotation_unreadable`, `annotation_digest_mismatch`, `annotation_not_canonical`, `annotation_invalid`, `annotation_version_unsupported`, `annotation_coverage_mismatch` | A saved decision fails strict re-validation, including malformed exchange records and `settings` or `context_policy` digests that do not recompute |
| `annotation_unbound`, `annotation_duplicate`, `annotation_missing` | Annotations do not map one-to-one onto manifest threads |
| `annotation_identity_mismatch`, `annotation_corpus_mismatch` | An annotation was produced under other identities or for other comments, or has `ok` items without checkpoint verification matching the pinned files (`field: checkpoint_verification`) |
| `replay_identity_mismatch`, `replay_exchange_digest_mismatch`, `replay_mismatch` | Offline replay does not reproduce the saved items; `replay_identity_mismatch` also covers a saved `questions` object that differs from the rebuilt question set |
| `heuristic_question_unavailable`, `heuristic_theme_outside_options` | The question set has no theme choice question for the keyword heuristic, or the heuristic produced a theme outside its options |
| `reconciliation_failed` | Internal count reconciliation failed |

Output location and publication refusals use `decision_input_error` (`output_inside_protected_storage`, `output_contains_protected_storage`, `output_aliases_input`, `output_root_not_directory`) and `decision_artifact_error` (`artifact_name_invalid`, `artifact_directory_symlink`, `artifact_conflict`, `artifact_write_failed`).
