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

The database carries a ThreadSense application id and schema version `1`. A database written by a newer schema version, a SQLite file that is not a ThreadSense monitoring database, and a file that is not SQLite are all refused with `monitoring_state_error` and left unmodified. There is no downgrade or migration path. An existing completely empty SQLite file (no tables, no application id) is treated as uninitialized and receives the schema on the first write.

If a crash left `state.sqlite3-journal` next to the database, any watch command, including `watch list` and `watch inspect`, lets SQLite roll back the interrupted transaction, but only after verifying that a private recovered copy is a supported ThreadSense state. Otherwise the database and journal are left untouched.

Tables:

- `watches`: one row per watch with its current revision number and creation and update times
- `watch_revisions`: one row per saved profile revision with the canonical profile JSON and its SHA-256 content hash; earlier revisions are kept
- `watch_runs`: one row per run claim with the profile revision it used, status (`running`, `succeeded`, `failed`, `abandoned`), claim, lease-expiry and finish times, the SHA-256 of the owner token, and `recovered_by_run_id` when a later claim took over an expired run; a partial unique index allows at most one `running` row per watch

Timestamps are stored as Unix epoch seconds (UTC) floating-point values. The content hash excludes timestamps, so saving identical profile content again does not write a new revision.

Inspection payloads emitted by the CLI:

`watch create` (`artifact_type: watch_profile`): `status` (`created`, `revised` or `unchanged`), `watch_id`, `revision`, `content_sha256`, `state_path`.

`watch list` (`artifact_type: watch_list`): `status`, `state_path`, and `watches`, each with `watch_id`, `name`, `current_revision`, `content_sha256`, `created_at_utc`, `updated_at_utc`.

`watch inspect` (`artifact_type: watch_inspection`): `status`, `state_path`, `watch_id`, `current_revision`, `created_at_utc`, `updated_at_utc`, `profile`, `revisions` (each with `revision`, `content_sha256`, `created_at_utc`), `active_run`, `run_count`, and `recent_runs` (newest 20, each with `run_id`, `profile_revision`, `status`, `claimed_at_utc`, `lease_expires_at_utc`, `finished_at_utc`, `recovered_by_run_id`).

See [usage.md](usage.md) for the profile format and command behavior.

## Research Result Payloads

`research reddit` also returns runtime metadata in the emitted result payload, including:

- selected thread list
- manifest path
- corpus analysis path
- corpus report path
- terminal summary when available

This payload is emitted directly to stdout according to the selected output mode.
