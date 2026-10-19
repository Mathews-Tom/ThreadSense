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

The database carries a ThreadSense application id and a schema version, currently `2`. A version `1` database is upgraded in place by the first command that writes to it: the upgrade only creates the version `2` tables, in one transaction, and keeps every existing profile, revision and run. Read-only commands read a version `1` database without modifying it. A database written by a newer schema version, a SQLite file that is not a ThreadSense monitoring database, and a file that is not SQLite are all refused with `monitoring_state_error` and left unmodified. There is no downgrade path. An existing completely empty SQLite file (no tables, no application id) is treated as uninitialized and receives the schema on the first write.

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

Sweep files live under the monitoring directory and are never overwritten. Paths in the database are relative to the monitoring directory:

```text
watches/<watch_id>/threads/<thread_id>/acquisitions/<acquisition_id>.raw.json
watches/<watch_id>/threads/<thread_id>/acquisitions/<acquisition_id>.normalized.json
watches/<watch_id>/threads/<thread_id>/revisions/<revision_id>/<snapshot_sha256>.json
watches/<watch_id>/threads/<thread_id>/analyses/<revision_id>/<run_id>.json
```

The raw file is the connector's raw thread artifact and the acquisition id is derived from its SHA-256. A revision snapshot is a canonical thread artifact holding every comment known for the thread at that revision, including comments absent from the latest acquisition; its provenance points at the raw acquisition file that created it. An analysis artifact's provenance points at the immutable snapshot it analyzed, not at a mutable latest path. Files from an interrupted sweep that the database never referenced are kept, not deleted.

Timestamps are stored as Unix epoch seconds (UTC) floating-point values. The content hash excludes timestamps, so saving identical profile content again does not write a new revision.

Inspection payloads emitted by the CLI:

`watch create` (`artifact_type: watch_profile`): `status` (`created`, `revised` or `unchanged`), `watch_id`, `revision`, `content_sha256`, `state_path`.

`watch list` (`artifact_type: watch_list`): `status`, `state_path`, and `watches`, each with `watch_id`, `name`, `current_revision`, `content_sha256`, `created_at_utc`, `updated_at_utc`.

`watch inspect` (`artifact_type: watch_inspection`): `status`, `state_path`, `watch_id`, `current_revision`, `created_at_utc`, `updated_at_utc`, `profile`, `revisions` (each with `revision`, `content_sha256`, `created_at_utc`), `active_run`, `run_count`, `recent_runs` (newest 20, each with `run_id`, `profile_revision`, `status`, `claimed_at_utc`, `lease_expires_at_utc`, `finished_at_utc`, `recovered_by_run_id`), and `sweep` with `tracked_threads`, `refresh_due_never_refreshed`, `pending_analyses`, `revision_count`, `comment_count` and `latest_sweep` (`run_id`, `outcome`, `finished_at_utc`, `summary`, or `null`).

See [usage.md](usage.md) for the profile format and command behavior.

## Research Result Payloads

`research reddit` also returns runtime metadata in the emitted result payload, including:

- selected thread list
- manifest path
- corpus analysis path
- corpus report path
- terminal summary when available

This payload is emitted directly to stdout according to the selected output mode.
