# ThreadSense Usage

This document is the command reference for the current CLI.

## Global Option

All commands support:

```bash
--output-format json|human|quiet
```

Modes:

- `json`: machine-readable payloads
- `human`: Rich tables and summary panels
- `quiet`: status-only output

See [output-modes.md](output-modes.md) for details.

## Command Overview

Top-level commands:

- `preflight`
- `fetch`
- `normalize`
- `analyze`
- `infer`
- `report`
- `inspect`
- `replay`
- `diff`
- `corpus`
- `evaluate`
- `batch`
- `serve`
- `run`
- `research`
- `watch`

## Happy Paths

### Single-thread run

```bash
uv run threadsense run [<source>] <url> \
  [--format markdown|html|json] \
  [--with-summary] \
  [--summary-required] \
  [--expand-more] \
  [--flat]
```

Source can be `reddit`, `hn`, `github-discussions`, `gist`, or omitted for auto-detection.

Examples:

```bash
uv run threadsense run reddit \
  "https://www.reddit.com/r/ClaudeCode/comments/1ro0qbl/anyone_actually_built_a_second_brain_that_isnt/" \
  --format markdown \
  --with-summary \
  --summary-required
```

```bash
uv run threadsense run gist \
  "https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f" \
  --format markdown
```

### Topic research across subreddits

```bash
uv run threadsense research reddit \
  --query "second brain OR agentic PKM" \
  --subreddit ClaudeCode \
  --subreddit LocalLLaMA \
  --subreddit AI_Agents \
  [--time-window 30d] \
  [--limit 20] \
  [--per-subreddit-limit 8] \
  [--sort relevance] \
  [--with-summary]
```

Force human-mode summary output:

```bash
uv run threadsense --output-format human research reddit \
  --query "second brain OR agentic PKM" \
  --subreddit ClaudeCode \
  --subreddit LocalLLaMA \
  --subreddit AI_Agents \
  --limit 5 \
  --per-subreddit-limit 3 \
  --with-summary
```

## `preflight`

Validate config and optionally probe the local runtime.

```bash
uv run threadsense preflight [--config threadsense.toml] [--skip-runtime]
```

## `fetch`

### `fetch reddit`

```bash
uv run threadsense fetch reddit <reddit-url> \
  [--output path/to/raw.json] \
  [--expand-more] \
  [--flat]
```

Notes:

- `--expand-more` expands Reddit `morechildren`
- `--flat` flattens nested comments in the persisted raw artifact

### `fetch hn`

```bash
uv run threadsense fetch hn <hackernews-item-url> [--output path/to/raw.json]
```

### `fetch github-discussions`

```bash
uv run threadsense fetch github-discussions <discussion-url> [--output path/to/raw.json]
```

### `fetch github-gist`

```bash
uv run threadsense fetch github-gist <gist-url> [--output path/to/raw.json]
```

Aliases: `fetch gist`

Notes:

- Authentication is optional. Public gists work without a token (60 req/hr rate limit).
- Set `THREADSENSE_GITHUB_GIST_TOKEN` for authenticated access (5,000 req/hr).
- Comments are paginated at 100/page automatically.

## `normalize`

Normalize one raw artifact into the canonical thread schema.

### `normalize reddit`

```bash
uv run threadsense normalize reddit --input path/to/raw.json [--output path/to/normalized.json]
```

### `normalize hn`

```bash
uv run threadsense normalize hn --input path/to/raw.json [--output path/to/normalized.json]
```

### `normalize github-discussions`

```bash
uv run threadsense normalize github-discussions --input path/to/raw.json [--output path/to/normalized.json]
```

### `normalize github-gist`

```bash
uv run threadsense normalize github-gist --input path/to/raw.json [--output path/to/normalized.json]
```

Aliases: `normalize gist`

Notes:

- Gist comments are flat — all normalized to `depth=0` with no `parent_comment_id`.
- Multi-file gists are concatenated into a single body, each file prefixed with its filename.

## `analyze`

Run deterministic analysis for one canonical thread artifact.

```bash
uv run threadsense analyze normalized \
  --input path/to/normalized.json \
  [--output path/to/analysis.json]
```

Contract-related options are also supported:

- `--domain`
- `--objective`
- `--abstraction-level`
- `--auto-domain`

## `infer`

### `infer analysis`

```bash
uv run threadsense infer analysis \
  --input path/to/analysis.json \
  [--task analysis_summary] \
  [--required]
```

### `infer corpus`

```bash
uv run threadsense infer corpus \
  --input path/to/corpus-analysis.json \
  [--required]
```

Behavior:

- without `--required`, local inference may degrade to deterministic output where supported
- with `--required`, the command fails if the local runtime is unavailable or returns invalid output

## `report`

### `report analysis`

```bash
uv run threadsense report analysis \
  --input path/to/analysis.json \
  [--format markdown|html|json] \
  [--output path/to/report.md] \
  [--with-summary] \
  [--summary-required]
```

Single-thread reports support:

- Markdown
- HTML
- JSON report artifacts

Summary-capable reports include richer executive-summary fields such as priority, owner, action type, and expected outcome.

## `inspect`

Inspect persisted artifacts without rerunning the pipeline.

```bash
uv run threadsense inspect normalized --input path/to/normalized.json
uv run threadsense inspect analysis --input path/to/analysis.json
uv run threadsense inspect report --input path/to/report.json
```

## `replay`

Replay one analysis artifact through the analysis stack.

```bash
uv run threadsense replay --input path/to/analysis.json
```

## `diff`

Compare two analysis artifacts.

```bash
uv run threadsense diff --left path/to/a.json --right path/to/b.json
```

## `corpus`

### `corpus create`

```bash
uv run threadsense corpus create \
  --name corpus-name \
  --description "corpus description" \
  --domain developer_tools \
  --analysis-dir .threadsense/analysis/reddit
```

### `corpus analyze`

```bash
uv run threadsense corpus analyze --input path/to/manifest.json
```

### `corpus report`

```bash
uv run threadsense corpus report \
  --input path/to/manifest.json \
  [--with-summary] \
  [--summary-required]
```

### `corpus search`

```bash
uv run threadsense corpus search --input path/to/manifest.json --query "workflow"
```

## `research`

### `research reddit`

Discover and analyze topic discussions across selected subreddits.

```bash
uv run threadsense research reddit \
  --query "second brain OR agentic PKM" \
  --subreddit ClaudeCode \
  --subreddit LocalLLaMA \
  --subreddit AI_Agents \
  [--time-window 30d] \
  [--sort relevance|new|top|comments] \
  [--limit 20] \
  [--per-subreddit-limit 8] \
  [--format markdown] \
  [--expand-more] \
  [--flat] \
  [--with-summary] \
  [--summary-required]
```

Important behavior:

- default `--time-window` is `30d`
- time window is enforced exactly after retrieval
- Reddit still uses a coarse internal bucket such as `month`
- `--format` currently supports `markdown` only for this workflow
- the result includes selected thread metadata plus corpus artifact paths

Supported query syntax:

- `OR`
- `|`

Unsupported query syntax:

- quotes
- parentheses
- `title:`
- `selftext:`
- negation-style advanced Reddit syntax

See [research-reddit.md](research-reddit.md) for the full workflow and ranking behavior.

## `watch`

Manage saved research profiles ("watches") and their persistent monitoring state. A watch stores what to look for: queries, subreddits, analysis contract, refresh cadence and per-stage limits. The command group saves and reads profiles, runs bounded sweeps, presents digests of new evidence and renders scheduler definitions. It does not call a model, send remote notifications or install a scheduler.

```bash
uv run threadsense watch create --profile PATH [--config PATH]
uv run threadsense watch list [--config PATH]
uv run threadsense watch inspect --id ID [--config PATH]
uv run threadsense watch run --id ID [--lease-seconds N] [--config PATH]
uv run threadsense watch digest --id ID [--lease-seconds N] [--config PATH]
uv run threadsense watch schedule --id ID --format launchd|cron [--workspace DIR] [--config PATH]
```

Output follows [output-modes.md](output-modes.md): human when stdout is a TTY and JSON otherwise, with `--output-format` overriding. In `json` mode the payload is printed to stdout. In `human` mode a successful payload is printed to stdout as syntax-highlighted JSON, and an error is a red panel on stderr showing the error code, message and details. In `quiet` mode a success prints only its `status` (`created`, `revised`, `unchanged`, `ok`, `presented`, `no_entries`, `rendered`, or the sweep outcome for `watch run`) and an error prints `error: <message>`, both on stdout. Failures exit with code `1`; `watch run` also exits `1` for a `partial` or `failed` sweep and still prints its payload. In `json` mode errors print:

```json
{"status": "error", "error": {"type": "...", "code": "...", "message": "...", "details": {}}}
```

Invalid input you supply, such as a profile file, a watch ID or the configured monitoring directory, uses code `monitoring_input_error`. State database problems use `monitoring_state_error`, with a `reason` in `details` such as `unsupported_schema_version`, `foreign_database`, `not_a_database`, `state_locked`, `unknown_watch`, `profile_hash_mismatch` (a stored profile revision no longer matches its content hash), `profile_unreadable` (a stored profile revision is not valid JSON, is not a JSON object, or is not valid under the current profile schema; `details` carries `watch_id` and `revision`), `recovery_failed` (SQLite could not recover an interrupted transaction of a supported state) or `recovery_conflict` (the state files changed during recovery). An empty, uninitialized state file reads as no watches. Run claim refusals use `run_claim_error`.

### Profile file

Profiles are TOML documents with `profile_version = 1`.

```toml
profile_version = 1
id = "agent-memory"
name = "Agent memory tooling"
domain = "developer_tools"
objective = "friction_analysis"
abstraction_level = "operational"
queries = ["second brain", "agentic PKM"]
subreddits = ["ClaudeCode", "r/LocalLLaMA"]
refresh_interval_minutes = 1440
include_terms = ["obsidian"]
exclude_terms = ["hiring"]

[limits]
discovered_candidates = 100
refreshed_threads = 25
analyzed_revisions = 25
presented_findings = 10
```

| Field                          | Required | Constraint                                                                                            |
| ------------------------------ | -------- | ----------------------------------------------------------------------------------------------------- |
| `profile_version`              | yes      | integer `1`; any other value, including a newer version, is rejected                                  |
| `id`                           | yes      | 1-64 lowercase letters, digits or inner hyphens, so `../x` is rejected; a database key, not a path    |
| `name`                         | yes      | 1-200 characters after trimming                                                                       |
| `domain`                       | yes      | same values as `--domain` in the analysis contract                                                    |
| `objective`                    | yes      | same values as `--objective`                                                                          |
| `abstraction_level`            | yes      | same values as `--level`                                                                              |
| `queries`                      | yes      | 1-10 entries, each at most 256 characters, same query syntax as `research reddit`                     |
| `subreddits`                   | yes      | 1-25 valid subreddit names; a leading `r/` is removed                                                 |
| `refresh_interval_minutes`     | yes      | integer 15-10080                                                                                      |
| `include_terms`                | no       | 0-50 entries, 1-100 characters each                                                                   |
| `exclude_terms`                | no       | 0-50 entries, 1-100 characters each; must not overlap `include_terms` case-insensitively              |
| `[limits]`                     | yes      | table with exactly the four keys below                                                                |
| `limits.discovered_candidates` | yes      | integer 1-500                                                                                         |
| `limits.refreshed_threads`     | yes      | integer 1-200                                                                                         |
| `limits.analyzed_revisions`    | yes      | integer 1-200                                                                                         |
| `limits.presented_findings`    | yes      | integer 1-100                                                                                         |

Validation rules:

- unknown top-level fields and unknown `[limits]` keys are rejected
- numeric fields must be integers; booleans are rejected
- duplicate entries (case-insensitive) in `queries`, `subreddits`, `include_terms` or `exclude_terms` are rejected
- entries are trimmed before validation
- the id never becomes a path; filesystem containment comes from `monitoring_dirname` staying inside the storage root
- all validation runs before the state database is opened for writing, so an invalid profile never creates or modifies state
- a profile path that is missing, unreadable or a directory, or a file that is not valid UTF-8 or not valid TOML, is rejected as `monitoring_input_error` (with `details.path`) before any state is written

### `watch create`

Validate a profile and save it as a new watch or a new revision of an existing watch.

```bash
uv run threadsense watch create --profile profiles/agent-memory.toml
```

`queries`, `subreddits`, `include_terms` and `exclude_terms` are treated as sets and stored sorted. The content hash is the SHA-256 of the canonical JSON form of the validated profile and excludes timestamps. Key order, list order, comments and whitespace in the TOML file do not affect it.

`status` in the result:

- `created`: first save for this `id`; revision `1`
- `unchanged`: content hash matches the current revision; the database is not modified
- `revised`: content differs from the current revision; `revision` is incremented and earlier revisions are kept

```json
{
  "artifact_type": "watch_profile",
  "status": "created",
  "watch_id": "agent-memory",
  "revision": 1,
  "content_sha256": "...",
  "state_path": "<resolved storage root>/monitoring/state.sqlite3"
}
```

### `watch list`

List saved watches. A missing state database lists nothing and is not created.

```json
{
  "artifact_type": "watch_list",
  "status": "ok",
  "state_path": "<resolved storage root>/monitoring/state.sqlite3",
  "watches": [
    {
      "watch_id": "agent-memory",
      "name": "Agent memory tooling",
      "current_revision": 2,
      "content_sha256": "...",
      "created_at_utc": 1760000000.0,
      "updated_at_utc": 1760003600.0
    }
  ]
}
```

Timestamps are Unix epoch seconds (UTC) as floating-point numbers.

### `watch inspect`

Show the current profile, revision history and recent run claims for one watch. An unknown `--id` is an error (`monitoring_state_error`, reason `unknown_watch`).

```json
{
  "artifact_type": "watch_inspection",
  "status": "ok",
  "state_path": "<resolved storage root>/monitoring/state.sqlite3",
  "watch_id": "agent-memory",
  "current_revision": 2,
  "created_at_utc": 1760000000.0,
  "updated_at_utc": 1760003600.0,
  "profile": {"profile_version": 1, "id": "agent-memory", "...": "..."},
  "revisions": [
    {"revision": 1, "content_sha256": "...", "created_at_utc": 1760000000.0},
    {"revision": 2, "content_sha256": "...", "created_at_utc": 1760003600.0}
  ],
  "active_run": null,
  "run_count": 0,
  "recent_runs": [],
  "sweep": {
    "tracked_threads": 0,
    "refresh_due_never_refreshed": 0,
    "pending_analyses": 0,
    "revision_count": 0,
    "comment_count": 0,
    "latest_sweep": null
  }
}
```

`sweep` summarizes recorded sweep state: tracked threads, tracked threads never refreshed, revisions still waiting for analysis, revision and comment counts, and the latest finished sweep with its outcome and summary, or `null`.

`digest` summarizes digest state: `entry_count`, `unpresented_entries`, `pending_digest_id` (or `null`), `digest_count`, and `latest_digest` (`digest_id`, `sequence`, `json_path`, `markdown_path`, `entry_count`, `written_at_utc`, or `null`). A database that has not reached schema version `3` reports zero counts and `null` values.

`recent_runs` holds the newest 20 claims. Each entry has `run_id`, `profile_revision`, `status` (`running`, `succeeded`, `failed` or `abandoned`), `claimed_at_utc`, `lease_expires_at_utc`, `finished_at_utc` and `recovered_by_run_id`. `active_run` is the entry whose status is `running`, or `null`. `run_count` is the total number of claims for the watch.

### `watch run`

Run one bounded sweep of a watch, then present a digest of the evidence it produced.

```bash
uv run threadsense watch run --id agent-memory [--lease-seconds 3600]
```

The sweep and the digest each claim the watch's run slot in turn, so one invocation records one sweep run followed by one digest run. `--lease-seconds` (default `3600`, bounds 1-86400) applies to both claims. The digest runs after every completed sweep outcome, so evidence analyzed by an earlier sweep is still presented after a `partial` or `failed` sweep. An error that stops the sweep, such as a refused claim or a state error, is raised as the usual error payload and no digest runs.

```json
{
  "artifact_type": "watch_run",
  "status": "changes",
  "state_path": "<resolved storage root>/monitoring/state.sqlite3",
  "sweep": {"run_id": "...", "watch_id": "agent-memory", "outcome": "changes", "...": "..."},
  "digest": {"status": "presented", "digest_id": "...", "sequence": 1, "...": "..."}
}
```

`status` is the sweep outcome: `changes`, `no_changes`, `partial` or `failed`. The exit code is `0` for `changes` and `no_changes` and `1` for `partial` and `failed`. A partial or failed sweep is never reported as `no_changes`. `sweep` carries the sweep result and `digest` the digest result described under `watch digest`.

A run that overlaps another live run of the same watch is refused with `run_claim_error` (reason `claim_held`) and exit code `1`, writing nothing.

### `watch digest`

Present new evidence without running a sweep.

```bash
uv run threadsense watch digest --id agent-memory [--lease-seconds 3600]
```

Each analyzed revision is turned into digest entries, once, and entries are presented in a numbered digest. A digest holds at most the profile's `limits.presented_findings` entries, oldest first; the rest wait for the next digest. The command is resumable: if an earlier attempt wrote its artifacts but stopped before recording them, the next `watch digest` or `watch run` finishes the same digest, with the same `digest_id`, sequence and entries and byte-identical files. An entry counts as presented only after both the JSON and Markdown files are durably written and recorded, so an interruption never marks unseen evidence as presented and no entry appears in two digests.

```json
{
  "artifact_type": "watch_digest_result",
  "status": "presented",
  "state_path": "<resolved storage root>/monitoring/state.sqlite3",
  "run_id": "...",
  "watch_id": "agent-memory",
  "digest_id": "dig-...",
  "sequence": 1,
  "resumed": false,
  "json_path": "<monitoring directory>/watches/agent-memory/digests/000001-dig-....json",
  "markdown_path": "<monitoring directory>/watches/agent-memory/digests/000001-dig-....md",
  "entry_ids": ["ent-..."],
  "materialized_entries": 1,
  "backlog": {"presentation_limit": 10, "presented_in_digest": 1, "...": "..."}
}
```

`status` is `presented` when a digest was written or resumed, and `no_entries` when nothing is waiting; then `digest_id`, `sequence`, `json_path`, `markdown_path` and `backlog` are `null` and `entry_ids` is empty. `materialized_entries` counts entries created by this invocation. The command exits `0` in both cases.

`backlog` fields:

- `presentation_limit`: the profile's `limits.presented_findings`
- `presented_in_digest`: entries in this digest
- `unpresented_entries_after`: entries still waiting after this digest
- `pending_analyses`: revisions recorded but not yet analyzed
- `tracked_threads`: tracked threads
- `refresh_pending`: tracked threads the latest sweep did not refresh within its budget, or `null` before any sweep
- `latest_sweep_outcome`: outcome of the latest finished sweep, or `null`
- `search_completeness`: always `unknown`; Reddit search is never treated as exhaustive

### Digest entries

A digest has two entry kinds. A `thread_update` entry exists for every analyzed revision and lists the comments that are new to the watch since the thread's previously digested revision. A `finding` entry exists for each analysis finding whose evidence includes at least one of those new comments, with the novel evidence quoted and linked to the snapshot and analysis files it came from. Entry ids are derived from the watch, revision and theme, so reprocessing never creates a duplicate.

Each novel comment carries a novelty label:

- `initial_observation`: seen on the thread's first acquisition
- `new_source_event`: created on Reddit after the previous complete observation, so it is new in the source as well as to the watch
- `newly_observed_historical`: created before that observation but first seen now, evidence that existed earlier and was not visible
- `edited`: the text changed; the observation time is recorded and the source edit time is `null` because Reddit does not expose it

Not every change creates an entry. Score changes, reordering, and comments that are absent from a truncated fetch create no entries, and a comment that disappears and later reappears with the same text is not new. A `partial` or `failed` sweep is never reported as `no_changes`, and a failed source fetch creates no evidence for the missing thread.

Excerpts are untrusted source text. The Markdown digest escapes markdown control characters and renders excerpts as block quotes, so source text is never interpreted as markup.

### `watch schedule`

Render a scheduler definition for recurring `watch run` invocations.

```bash
uv run threadsense watch schedule --id agent-memory --format launchd
uv run threadsense watch schedule --id agent-memory --format cron --workspace /path/to/workspace
```

The command only renders. It never writes a file, never runs `launchctl` or `crontab`, and installs and enables nothing; `installed` and `enabled` in the payload are always `false`. The watch must exist. You decide whether to install the output and do so manually, following `install_instructions`.

The interval is the watch's `refresh_interval_minutes`. The scheduled command is the current Python interpreter running `-m threadsense --output-format json watch run --id ID --lease-seconds N`, with `--config` pointing at the config file this render loaded (the `--config` argument, `THREADSENSE_CONFIG`, or `threadsense.toml` in the current directory), because launchd and cron do not inherit your shell environment; other `THREADSENSE_*` environment overrides are not carried into the job. Every path is absolute: the interpreter (not resolved through symlinks, so a virtual environment interpreter stays that environment), the workspace (`--workspace`, default the current directory, which must exist), the storage root (`[storage] root_dir` resolved against the workspace) and the log file under `<monitoring directory>/watches/<watch_id>/schedule/`. The lease is the interval capped at 86400 seconds. Paths with spaces, quotes or `%` are escaped for the target format: the launchd output is a property list with XML escaping, and the cron line is shell-quoted with each `%` written as `\%`. A cron render is refused when a path or argument contains a line break (reason `cron_line_break`) or a backslash directly before `%` (reason `cron_backslash_percent`).

- `launchd` renders a property list labeled `com.threadsense.watch.<id>` with the arguments, working directory, `StartInterval`, a `THREADSENSE_STORAGE_ROOT` environment entry, log paths, and `RunAtLoad` false
- `cron` renders one crontab line that changes into the workspace, sets `THREADSENSE_STORAGE_ROOT` and appends output to the log. The interval must be a whole number of minutes dividing an hour, whole hours dividing a day, or one week (`10080`); any other interval is refused with `monitoring_input_error` (reason `cron_interval_unrepresentable`) and `launchd` is the alternative

Overlap is controlled by the run claim, regardless of scheduler: `watch run` refuses to start while another run of the watch holds a live claim (`run_claim_error`, reason `claim_held`). The claim is not renewed, so a run that outlasts its lease (the interval) can be taken over by the next scheduled start; the overtaken run is then refused at its next write (`run_claim_error`, reason `lease_expired`, or `stale_owner` once the new run has recovered it) and records nothing further.

```json
{
  "artifact_type": "watch_schedule",
  "status": "rendered",
  "format": "launchd",
  "watch_id": "agent-memory",
  "installed": false,
  "enabled": false,
  "executable": "/abs/path/to/python",
  "workspace": "/abs/path/to/workspace",
  "storage_root": "/abs/path/to/storage",
  "config_path": null,
  "interval_seconds": 86400,
  "lease_seconds": 86400,
  "argv": ["/abs/path/to/python", "-m", "threadsense", "..."],
  "log_path": "/abs/path/to/watch-run.log",
  "overlap_policy": "run_claim",
  "content": "<rendered text>",
  "install_instructions": ["..."]
}
```

### State location

State is a SQLite database at `<storage root>/<monitoring_dirname>/state.sqlite3`, by default `.threadsense/monitoring/state.sqlite3`. Set `monitoring_dirname` under `[storage]` in the config file or with `THREADSENSE_STORAGE_MONITORING_DIR`. The value must be a relative path that stays inside the storage root; absolute paths and `..` segments are rejected. See [artifacts.md](artifacts.md) for the schema and version behavior.

### Run claims

The `threadsense.monitoring` Python package provides the run-claim primitive that `watch run` and `watch digest` use, together with the `run_watch_sweep` and `present_watch_digest` library APIs. `watch inspect` reports claims, sweeps and digests recorded in the database.

- at most one run per watch is `running`, enforced transactionally and by a partial unique index
- a claim carries an owner token and a lease of 1-86400 seconds; only the SHA-256 of the token is stored
- a competing claim while the lease is live is refused with `run_claim_error`, reason `claim_held`
- after the lease expires, the next claim marks the old run `abandoned` with `recovered_by_run_id` pointing at the new run
- the previous owner can no longer finalize that run (reason `stale_owner`); an owner whose lease expired without a newer claim cannot finalize either (reason `lease_expired`)

## `evaluate`

Run golden-dataset evaluation.

```bash
uv run threadsense evaluate --dataset path/to/dataset.json --strategies keyword_heuristic hybrid
```

## `batch run`

Execute a reproducible batch workflow from a manifest.

```bash
uv run threadsense batch run --manifest tests/fixtures/batch/reddit_manifest.json
```

See [batch-api-runtime.md](batch-api-runtime.md).

## `serve`

Run the local HTTP API.

```bash
uv run threadsense serve [--host 127.0.0.1] [--port 8090]
```

Current routes:

- `GET /v1/healthz`
- `GET /v1/metrics`
- `POST /v1/fetch/reddit`
- `POST /v1/normalize/reddit`
- `POST /v1/analyze/normalized`
- `POST /v1/infer/analysis`
- `POST /v1/report/analysis`

## Artifacts

See [artifacts.md](artifacts.md) for the full artifact inventory.

Common paths:

- `.threadsense/raw/<source>/...`
- `.threadsense/normalized/<source>/...`
- `.threadsense/analysis/<source>/...`
- `.threadsense/reports/<source>/...`
- `.threadsense/corpora/<corpus-id>/...`
- `.threadsense/monitoring/state.sqlite3`
- `.threadsense/monitoring/watches/<watch_id>/digests/<sequence>-<digest_id>.{json,md}`

## Related Docs

- [README.md](../README.md)
- [research-reddit.md](research-reddit.md)
- [output-modes.md](output-modes.md)
- [artifacts.md](artifacts.md)
- [overview.md](overview.md)
- [system-design.md](system-design.md)
- [local-runtime-contract.md](local-runtime-contract.md)
- [batch-api-runtime.md](batch-api-runtime.md)
