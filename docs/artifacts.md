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

The database carries a ThreadSense application id and a schema version, currently `7`. A version `1` to `6` database is upgraded in place by the first command that writes to it: the upgrade only creates the missing tables and index, in one transaction, and keeps every existing profile, revision, run and sweep record. Read-only commands read an older database without modifying it; `watch inspect` reports an empty `digest` summary for a database older than version `3`, `issues inspect` reports no issues for a database older than version `4`, and `watch policy` reports the built-in default policy with no selections for a database older than version `7`. A database written by a newer schema version, a SQLite file that is not a ThreadSense monitoring database, and a file that is not SQLite are all refused with `monitoring_state_error` and left unmodified. There is no downgrade path. An existing completely empty SQLite file (no tables, no application id) is treated as uninitialized and receives the schema on the first write.

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

Issue history tables (schema version `4`), all append-only: triggers abort every `UPDATE` and `DELETE`, so rows are only inserted and corrections are new events:

- `watch_issue_policies`: the grouping policy (canonical JSON and its SHA-256) that issue ingestion ran under, with the candidate bounds
- `watch_issues`: one row per home-thread issue with its creation sequence within the watch, the anchor time (earliest report source time at creation), creating run and policy
- `watch_issue_entries`: one row per finding entry recorded as issue evidence, with its theme key and the key phrases of its finding
- `watch_issue_reports`: one row per cited source comment with its record sequence, first text hash, hashed author account (`null` for deleted or blank authors), community, permalink, source creation time, `lineage` (`firsthand` or `exact_copy`) and `copy_of_report_id`; this recorded lineage never changes after the row is written. Effective lineage is derived from it and the latest correction event in the ingestion ledger
- `watch_issue_evidence`: one row per report and entry citing it, with the text hash, revision, snapshot hash and the links found in the text
- `watch_issue_updates`: one row per unverified update claim (`reproduction`, `contradiction`, `workaround` or `resolution_claim`) with the lexical cue that produced it
- `watch_issue_memberships`: membership events (`assigned`, `linked`, `unlinked`) with basis (`thread_seed`, `exact_content_lineage` or `operator`), previous issue and reason; a thread's current issue is its latest event, and operator events carry no run, report or policy
- `watch_issue_suggestions`: bounded candidate pairs with status (`unconfirmed` or `refused`), refusal reason and signals
- `watch_issue_ingestions`: one row per run that recorded issue evidence with its summary

Digest issue views (schema version `5`):

- `watch_digest_issue_views`: one append-only row per digest created at schema version `5` holding the canonical JSON issue view recorded with that digest, so a resumed digest renders identical bytes; a digest created before version `5` has no row

Operator feedback tables (schema version `6`), all append-only: triggers abort every `UPDATE` and `DELETE`:

- `watch_feedback_keys`: the watch-scoped idempotency namespace; one row per key with its `fbk-` feedback id, kind and content hash
- `watch_preference_events`: preference events with their per-watch sequence, resolved target, thread/theme and preference scope
- `watch_correction_events`: correction assertions with their per-watch sequence and resolved target; never a ranking input

Ranking tables (schema version `7`), all append-only: triggers abort every `UPDATE` and `DELETE`:

- `watch_ranking_policies`: one row per ranking policy with its content-addressed `policy_id` (`pol-` followed by the first 32 hex characters of the SHA-256 of the canonical policy JSON), `name`, canonical `policy_json` and creation time
- `watch_ranking_selections`: one row per policy selection with its watch, per-watch `sequence`, `policy_id` and selection time; the latest selection is the watch's active policy, and a watch without rows uses the built-in default
- `watch_digest_rankings`: one row per digest created at schema version `7` with the ranking basis recorded in the same transaction as the digest: `profile_revision`, `policy_id` and `preference_sequence`; a digest created before version `7` has no row and keeps its selection order

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

The digest JSON artifact (`artifact_type: watch_digest`, `schema_version: 3`; `2` for a pending digest recorded with an issue view but before rankings existed, `1` for one recorded before issue views existed) has `watch_id`, `digest_id`, `sequence`, `created_at_utc`, `entries` and `backlog`. It is written with sorted keys, two-space indentation and a trailing newline, and rendered only from stored digest data, so a resumed digest is byte-identical. Each entry has `kind`, `entry_id`, `watch_id`, `source_thread_id`, `thread_url`, `subreddit`, `title`, `revision_id`, `revision_sequence`, `snapshot_path`, `snapshot_sha256`, `analysis_path`, `analysis_sha256` and `thread_text_changed`; a `thread_update` entry adds `novel_comments` and `counts` (every change kind with its count, including zeros). Each novel comment has `comment_id`, `kind` (`initial_observation`, `new_source_event`, `newly_observed_historical` or `edited`), `text_sha256`, `previous_text_sha256`, `source_created_utc`, `source_edited_at_utc` (present only on `edited` comments and always `null`; no source edit time is recorded), `first_observed_at_utc` or, for edits, `observed_at_utc`, `permalink` and a whitespace-collapsed `excerpt` of at most 280 characters. A `finding` entry adds `theme_key`, `theme_label`, `severity`, `finding_comment_count`, `novel_evidence_comment_ids` and `novel_evidence` (the novel comments it cites, in the same shape). `backlog` has `presentation_limit`, `presented_in_digest`, `unpresented_entries_after`, `pending_analyses`, `tracked_threads`, `refresh_pending`, `latest_sweep_outcome` and `search_completeness` (always `unknown`). Snapshot and analysis paths are relative to the monitoring directory.

A schema `2` digest adds a top-level `issues` object, rendered only from the stored view. Digests recorded before state schema `5` stay schema `1` with no `issues` key. `issues` has `semantic_grouping` (always `not_eligible`: suggestions are unconfirmed and never merge memberships or counts), `entry_refs` and `issues`. Each `entry_refs` item, in digest entry order and only for entries whose thread has issue history, has `entry_id`, `kind`, `source_thread_id`, `issue_id` (the thread's current issue at presentation), `home_issue_id`, `reports` (this entry's evidence: `report_id`, `comment_id`, `lineage`, `copy_of_report_id`, `text_sha256`, with effective lineage as of presentation) and `updates` (`update_id`, `kind`, `cue`, `report_id`, `comment_id`); `thread_update` entries have empty lists. `issues` holds each referenced issue once, in first-reference order, in the `issues inspect` list shape plus its `suggestions` (`unconfirmed` and `refused`, with `currently_grouped`).

New digest entry `updates` omit a corrected copy's cue when its text matches the copied first text; cues in edited-added text are retained. Raw update rows and previously recorded digest views are unchanged.

A stored issue view and its rendered digest are never rewritten: a digest recorded before a lineage correction keeps its original bytes, and only digests created afterwards capture the corrected effective lineage. The ingestion-ledger row of each ingestion (`watch_issue_ingestions.summary_json`) carries `lineage_corrections` and an append-only `lineage_events` list; earlier ledger rows are not edited.

A schema `3` digest adds a top-level `ranking` object, materialized only from the ranking basis recorded with the digest, so a resumed digest is byte-identical and `watch explain` can replay it. `entries` keeps selection order and identical entry payloads; `ranking` only orders their presentation. `ranking` has:

- `ranking_version`: `1`
- `basis`: `profile_revision`, `policy_id` and `preference_sequence` as recorded, plus the profile `objective` and the policy `name`
- `semantic`: `status` (always `not_eligible`) and `reason`
- `corrections`: always `not_a_ranking_input`
- `order`: one item per entry, best first, each with `rank` (1-based), `entry_id`, `kind`, `score` (the sum of the integer component points), `components` and `tie_break`

`components` lists, in this order, `severity` (with `severity`), `objective` (with `objective`, `signal_type`, `action_type` and `recommended_owner`), `novelty` (with `novelty_kind` when the entry has new comments), `evidence` (with `novel_comments` and `counted_comments`), `preference` (with `unbounded_points`, `bound` and `matched`, each match carrying `feedback_id`, `sequence`, `value`, `scope_kind`, `scope_key` and `points`) and `semantic` (with `status`). Every component has `component`, `points` and `reason`; `semantic` points are always `null`, and thread updates score `0` for severity and objective without the extra fields. `tie_break` holds the baseline order keys used only when scores are equal: `kind`, `severity`, `issue_markers`, `request_markers`, `comment_count`, `theme_key` and `entry_id`.

A schema `2` or schema `1` digest has no `ranking` key and presents its entries in selection order.

The Markdown artifact has a title with the watch id and sequence, a backlog section, an Issues section when the digest carries an issue view (each referenced issue with its counts, unverified update totals, unconfirmed suggestions and refused groupings), a Ranking section when the digest carries a ranking (policy name and id, objective and profile revision, the preference sequence applied and the `not_eligible` semantic component), and one section per entry, which adds the entry's issue, its updates as unverified cue claims and its exact copies. A ranked digest lists its entries in ranked order, and each entry section starts with a Rank line giving its rank, score and per-component points, followed by a Preference line when operator preferences applied. An unranked digest lists entries in selection order. Source text is untrusted: excerpts are block quotes with markdown control characters escaped.

The raw file is the connector's raw thread artifact and the acquisition id is derived from its SHA-256. A revision snapshot is a canonical thread artifact holding every comment known for the thread at that revision, including comments absent from the latest acquisition; its provenance points at the raw acquisition file that created it. An analysis artifact's provenance points at the immutable snapshot it analyzed, not at a mutable latest path. Files from an interrupted sweep that the database never referenced are kept, not deleted.

Timestamps are stored as Unix epoch seconds (UTC) floating-point values. The content hash excludes timestamps, so saving identical profile content again does not write a new revision.

Inspection payloads emitted by the CLI:

`watch create` (`artifact_type: watch_profile`): `status` (`created`, `revised` or `unchanged`), `watch_id`, `revision`, `content_sha256`, `state_path`.

`watch list` (`artifact_type: watch_list`): `status`, `state_path`, and `watches`, each with `watch_id`, `name`, `current_revision`, `content_sha256`, `created_at_utc`, `updated_at_utc`.

`watch inspect` (`artifact_type: watch_inspection`): `status`, `state_path`, `watch_id`, `current_revision`, `created_at_utc`, `updated_at_utc`, `profile`, `revisions` (each with `revision`, `content_sha256`, `created_at_utc`), `active_run`, `run_count`, `recent_runs` (newest 20, each with `run_id`, `profile_revision`, `status`, `claimed_at_utc`, `lease_expires_at_utc`, `finished_at_utc`, `recovered_by_run_id`), and `digest` (`entry_count`, `unpresented_entries`, `pending_digest_id`, `digest_count`, `latest_digest` with `digest_id`, `sequence`, absolute `json_path` and `markdown_path`, `entry_count` and `written_at_utc`, or `null`), and `sweep` with `tracked_threads`, `refresh_due_never_refreshed`, `pending_analyses`, `revision_count`, `comment_count` and `latest_sweep` (`run_id`, `outcome`, `finished_at_utc`, `summary`, or `null`).

`watch run` (`artifact_type: watch_run`): `status` (the sweep outcome), `state_path`, `sweep` and `digest`.

`watch digest` (`artifact_type: watch_digest_result`): `status` (`presented` or `no_entries`), `state_path`, `run_id`, `watch_id`, `digest_id`, `sequence`, `resumed`, `json_path`, `markdown_path`, `entry_ids` (selection order), `materialized_entries`, `backlog`, `issues` (the issue ingestion counts of the run) and `ranking` (the digest's ranking `basis` fields plus `ranked_entry_ids` in ranked order, or `null` when no digest was presented or the digest is unranked).

`issues inspect` (`artifact_type: issue_list` or `issue_inspection`) and `issues link` or `issues unlink` (`artifact_type: issue_membership_change`) are specified in [usage.md](usage.md).

`watch explain` (`artifact_type: watch_explanation`) and `watch policy` (`artifact_type: ranking_policy` or `ranking_policy_selection`) are specified in [usage.md](usage.md). Neither writes an artifact file; `watch explain` writes nothing at all.

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
