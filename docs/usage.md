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
- `decisions` (`annotate`, `evaluate`, `policy check`)

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

Excerpts are untrusted source text. The Markdown digest escapes HTML and link, image, code, heading and table delimiters, then renders excerpts as block quotes. Display-only formatting such as strike-through, list markers and automatic URL linking can remain inside the quote.

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

## `decisions`

Shadow typed semantic annotations from a separate, operator-provisioned local Laya service. This path is independent of the generative runtime used by `infer`: it never runs analysis, never calls the generative runtime and does not depend on `THREADSENSE_RUNTIME_ENABLED`. Annotations are separate artifacts and never change baseline findings, reports or monitoring state.

### `decisions annotate`

```bash
threadsense decisions annotate --input PATH --endpoint URL \
  [--requirement optional|required] \
  [--domain D] [--objective O] \
  [--output-dir DIR] [--timeout-seconds F] [--config PATH]
```

| Flag | Meaning |
| --- | --- |
| `--input PATH` | Required. A canonical thread artifact: a normalized artifact or a monitoring revision snapshot |
| `--endpoint URL` | Required. The service base URL. Literal loopback `http` only, for example `http://127.0.0.1:PORT`; `localhost` and non-loopback hosts are refused |
| `--requirement` | `optional` (default) or `required` |
| `--domain D` | Domain vocabulary for the theme question; defaults to the configured analysis domain |
| `--objective O` | Analysis objective for the relevance question; defaults to the configured analysis objective |
| `--output-dir DIR` | Annotation artifact root; defaults to `<storage-root>/annotations` |
| `--timeout-seconds F` | Timeout for each HTTP call to the service; default `300` |
| `--config PATH` | Configuration file, as for other commands |

The default annotation directory name is `annotations` under the storage root; configure it with `annotations_dirname` in the `[storage]` table or the `THREADSENSE_STORAGE_ANNOTATIONS_DIR` environment variable.

Requirement modes:

- `optional` (default) is a shadow run. A refused comment, an unavailable service or an unhealthy runtime stays visible in the written artifact and in the printed coverage, and the command exits `0`.
- `required` exits `1` on any refused or unavailable comment or on an unhealthy runtime. Artifacts are still written so the gap can be inspected.

Output locations are refused when the output root lies inside the baseline or monitoring storage, or when it or any output path aliases the input artifact. See [artifacts.md](artifacts.md#typed-decision-annotations) for the layout and the full document, status and reason definitions.

Printed JSON payload (`artifact_type: decision_annotations`, `mode: shadow`):

| Field | Content |
| --- | --- |
| `status` | Aggregate coverage status: `complete`, `partial` or `unavailable` |
| `coverage` | Counts of total, ok, refused and unavailable comments |
| `requirement`, `satisfied` | The requested mode and whether it was met |
| `decision_path`, `decision_sha256` | The content-addressed decision document and its hash |
| `run_path`, `run_id` | The run record that holds run-varying values |
| `snapshot_sha256` | Hash of the input snapshot |
| `question_set_sha256`, `manifest_sha256` | Identity of the question set and of the pinned runtime manifest |

Exit codes: `0` when the run is satisfied (always for `optional` once the artifacts are published), `1` when `required` coverage is not complete, the runtime is unhealthy, or the command fails.

Examples:

```bash
# Annotate a normalized artifact in shadow mode
uv run threadsense decisions annotate \
  --input .threadsense/normalized/reddit/<thread-artifact>.json \
  --endpoint http://127.0.0.1:<port>

# Annotate a monitoring revision snapshot and require complete coverage
uv run threadsense decisions annotate \
  --input <revision-snapshot-path> \
  --endpoint http://127.0.0.1:<port> \
  --requirement required \
  --output-dir <annotation-root>
```

The service is not installed, downloaded or started by ThreadSense. Prepare or reuse it with the procedure in [laya-runtime.md](laya-runtime.md). Checkpoint identity is verified on every invocation.

Operator smoke check against the real service (fixtures are not a substitute, so a running service is required):

```bash
uv run python scripts/smoke_monitoring.py --scenario annotations \
  --work-dir "$THREADSENSE_SMOKE_DIR" \
  --endpoint "$THREADSENSE_DECISION_ENDPOINT"
```

`THREADSENSE_SMOKE_DIR` is an empty or absent working directory and `THREADSENSE_DECISION_ENDPOINT` is the loopback service URL.

### `decisions evaluate`

Offline assessment of saved decision documents against a frozen dataset manifest and its labels. It replays every saved item from the recorded exchange bodies, scores the typed decisions against the declared labels per partition, compares the `theme` question with the existing keyword heuristic, and publishes one content-addressed evaluation document plus a run record.

```bash
threadsense decisions evaluate --manifest PATH \
  --annotation PATH [--annotation PATH ...] \
  [--output-dir DIR] [--config PATH]
```

| Flag | Meaning |
| --- | --- |
| `--manifest PATH` | Required. A frozen dataset manifest (`typed_decision_evaluation_manifest`) that binds every thread file and the label file by sha256 |
| `--annotation PATH` | Required, repeatable. A saved decision document from `decisions annotate`; supply exactly one per manifest thread |
| `--output-dir DIR` | Artifact root; defaults to the annotation root, `<storage-root>/annotations` |
| `--config PATH` | Configuration file, as for other commands |

What it does and does not do:

- It never contacts the Laya service and never re-annotates. Saved decisions must have been produced under the exact identities the manifest binds (runtime manifest, checkpoint revision, question set, settings, context policy and language), and those bindings must still equal the current pinned identities.
- The only network access is the generative comparator availability observation: one `GET <base_url>/v1/models` to the configured generative runtime, made only when the runtime is enabled and its URL is literal loopback. No generation request is sent. The observation is recorded in the run record, not in the evaluation document.
- Results describe the listed inputs against their declared labels only. They are not population accuracy. The shipped labels (`tests/fixtures/decisions/evaluation/`) are engineering-authored boundary expectations; they are not independent adjudication and not ground truth.
- `answer_confidence` is uncalibrated and is not a probability of correctness.
- Unavailable labels and unavailable comparators are reported as unavailable with a reason, never as zero error.
- Option stability is reported as vacuous: each question has one fixed option layout and no permuted-layout probe exists yet.

Output locations follow the `decisions annotate` rules: the output root must not lie inside or contain baseline or monitoring storage, and no output path may alias the manifest, the label file, a thread file or an annotation. See [artifacts.md](artifacts.md#typed-decision-evaluation) for the input formats, the document, metric definitions and refusal reasons.

Printed JSON payload (`artifact_type: decision_evaluation`, `status: ready`, `mode: shadow`):

| Field | Content |
| --- | --- |
| `evaluation_path`, `evaluation_sha256` | The published evaluation document and the sha256 of its bytes |
| `run_path`, `run_id` | The run record of this invocation |
| `manifest_sha256` | The sha256 of the manifest bytes |
| `label_origin`, `independent_adjudication` | The declared label origin and whether it is independent adjudication |
| `partitions` | Per partition (`development`, `calibration`, `evaluation`): threads, comments, ok, refused, unavailable, excluded_language and eligible counts |
| `generative_comparator` | Availability `status` and `reason` observed for this run |

Exit codes: `0` when the evaluation and run record are published; `1` on any refusal (stale binding, missing, duplicate, unbound or mismatched annotation, replay mismatch, partition leak, invalid input, refused output location). Refusals print the error JSON with `details.reason`.

Re-running over the same inputs reproduces the same evaluation bytes, sha256 and path, and adds a new run record.

Example:

```bash
uv run threadsense decisions evaluate \
  --manifest <dataset-dir>/manifest.json \
  --annotation <annotation-root>/<snapshot_sha256>/decisions/<decision_sha256>.json \
  --annotation <annotation-root>/<snapshot_sha256>/decisions/<decision_sha256>.json
```

Operator smoke check for the evaluation and eligibility path against the real service:

```bash
uv run python scripts/smoke_monitoring.py --scenario evaluation \
  --work-dir "$THREADSENSE_SMOKE_DIR" \
  --endpoint "$THREADSENSE_DECISION_ENDPOINT"
```

A running local typed-decision service is required; the scenario never installs, downloads, starts or stops it. It stages the shipped evaluation fixtures into the work directory, annotates them with real `decisions annotate` calls, runs real `decisions evaluate` twice (replay reproduces the same evaluation bytes), exercises evaluation refusals, and runs `decisions policy check`. It finally verifies that staged inputs are unchanged, nothing was written outside the work directory and the source checkout is unchanged. Evidence is written to the work directory.

The first evaluation honestly observes the runtime configured in the source `threadsense.toml`; it is not forced disabled. When a configured local model can answer, comparison is performed and its frozen capture is recorded. The second evaluation uses that exact capture and the same runtime configuration, with no model listing or inference. If the first evaluation produces no capture, the second uses the existing runtime-disabled CLI path for deterministic typed/heuristic-only replay, explicitly recording why; it does not retry acquisition or hide the first observed availability. CLI/run status, reason, optional capture digest and normalized capture path must agree. Comparison is accepted as performed exactly when a capture exists. Older standalone evaluation implementations without capture fields remain supported for typed/heuristic-only replay.

The client character budget (2000 characters per comment) is necessary but not sufficient. The one comment over the budget must be refused with `comment_exceeds_client_char_budget`. The boundary comment, the staged comment whose body is exactly 2000 characters, may be refused by the pinned service with a non-empty set of server usage reasons (`server_dropped_state_tokens`, `server_reported_truncation`, `server_truncated_questions`, `server_reported_option_usage`); this was observed against the pinned checkpoint. Such a refusal is recorded in the evidence `boundary_comments` with its observed status and reasons, is never scored, and is audited as `rejected`; the evaluation's rejected audit must equal the refused comments and reasons exactly. No other within-budget refusal is accepted: every other comment under the budget must be `ok`, and the boundary comment may also be `ok`. This is not a quality claim.

What the policy steps establish:

- The real runtime result is `not_eligible`. The real evidence with no policy, and with a `runtime`-scope policy bound to the real identities, is `not_eligible` with `held_out_labels_not_independent` because the shipped labels are engineering-authored. Semantic decisions remain shadow-only.
- A constructed `test_only` fixture is `eligible` with `activation_permitted: false` and `activation.performed: false`. Its label file is a copy of the shipped labels with the same values and only the origin changed to `constructed_test_fixture` with a disclosure; nothing is relabeled. A test-only result cannot be consumed as a runtime policy.
- The test-only policy uses fixed, predeclared, engineering-only, test-only tolerances (`max_error_rate=1.0`, `min_decision_rate=0.0`, `min_labeled_decided=1`) declared before any model call. They exercise the checker mechanics only and are not quality thresholds.
- Each one-field invalidation of that fixture yields exactly its single reason code. Tolerance invalidations also use fixed predeclared values (`max_error_rate=0.0`, `min_decision_rate=1.0`, `min_labeled_decided=1000000`); one that is vacuous for the observed metrics (for example `max_error_rate=0.0` when the observed error rate is 0) is recorded `not_applicable`, not passed.
- The generative comparator availability is recorded as observed; an unavailable comparator is never zero accuracy. Wall time and server RSS are point observations, not peak memory and not quality.

It proves that the commands, bindings, refusals and checker behave as documented against the real runtime. It is not quality evidence: the shipped labels are engineering-authored, not independent adjudication, and the test-only tolerances are not quality thresholds.

### `decisions policy check`

Offline eligibility check of one saved evaluation document against an optional operator-authored activation policy. It decides whether a typed decision shape would be eligible for semantic activation by a consumer (`semantic_reranking` or `semantic_grouping`), records the decision in a content-addressed policy check document, and does nothing else: it never activates a consumer, never installs or stores a policy, never contacts any service and never changes mode. Semantic decisions stay in shadow mode.

```bash
threadsense decisions policy check --evaluation PATH \
  [--policy PATH] [--output-dir DIR] [--config PATH]
```

| Flag | Meaning |
| --- | --- |
| `--evaluation PATH` | Required. A saved evaluation document `<evaluation_sha256>.json` from `decisions evaluate`; the sha256 of its bytes must equal the file name stem |
| `--policy PATH` | Optional. An activation policy JSON (`typed_decision_activation_policy`). Omitting it is reported as `policy_not_declared` |
| `--output-dir DIR` | Artifact root; defaults to the annotation root, `<storage-root>/annotations` |
| `--config PATH` | Configuration file, as for other commands |

No policy is shipped with ThreadSense or installed by any command. A policy is an operator-authored input passed per invocation; nothing reads a policy implicitly. Every check runs against the current pinned runtime identity, and all reasons are accumulated rather than stopping at the first failure. A check whose inputs are absent is recorded `not_run`, never `passed`. See [artifacts.md](artifacts.md#activation-policy) for the policy format and [artifacts.md](artifacts.md#policy-check) for the checks, every reason code and the document.

Outcomes:

- `eligible`: zero reasons. Every binding matches the current runtime and the evidence, the held-out (`evaluation` partition) labels have the origin the scope accepts, evidence integrity passed, and the observed held-out metrics for the policy question satisfy the declared tolerances.
- `not_eligible`: one or more reasons. This is a supported outcome, not a failure. Under the real runtime it is the expected result: no independently adjudicated held-out labels and no domain tolerances exist, so the shipped evidence yields `held_out_labels_not_independent` (and `policy_not_declared` when no policy is passed).
- `scope`: the policy scope, `runtime` or `test_only`, or `null` when no valid policy was read. A `test_only` policy accepts only labels whose origin is a disclosed constructed test fixture, and its own `disclosure` must contain the whole word `constructed` and the whole word `test` or `tests` (case-insensitive; `contest` or `latest` do not count). All other checks apply unchanged. Test-only fixtures live only in tests and smoke work directories, are never installed, and never permit activation.
- `activation_permitted`: `true` only when the result is `eligible` and the scope is `runtime`. Even then nothing is activated by this command; it reports permission only.

Unavailable labels and unavailable comparators are never treated as zero error, and a non-finite tolerance or observed value is reported as `nonfinite_value`.

Output locations follow the `decisions annotate` rules: the output root must not lie inside or contain baseline or monitoring storage, no output path may alias the evaluation or policy file, and a symlinked `policy-checks` directory is refused.

Printed JSON payload (`artifact_type: decision_policy_check`, `mode: shadow`):

| Field | Content |
| --- | --- |
| `status` | The eligibility: `eligible` or `not_eligible` |
| `policy_check_path`, `policy_check_sha256` | The published policy check document and the sha256 of its bytes |
| `eligibility` | `eligible` or `not_eligible` |
| `scope` | `runtime`, `test_only` or `null` |
| `activation_permitted` | Whether a runtime-scope policy is satisfied; never an activation |
| `reasons` | The reason codes, in check order |

Exit codes: `0` whenever the check completed and its document was published, for both `eligible` and `not_eligible`; `1` when the command fails (invalid configuration, refused output location, publication error). Errors print the error JSON with `details.reason`.

Re-running with the same inputs under the same runtime identity reproduces the same document bytes, sha256 and path.

Example:

```bash
uv run threadsense decisions policy check \
  --evaluation <annotation-root>/evaluations/<manifest_sha256>/<evaluation_sha256>.json \
  --policy <operator-policy-dir>/policy.json
```

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
- `.threadsense/annotations/<snapshot_sha256>/{decisions,runs}/...`
- `.threadsense/annotations/evaluations/<manifest_sha256>/<evaluation_sha256>.json`
- `.threadsense/annotations/evaluations/<manifest_sha256>/runs/<run_id>.json`
- `.threadsense/annotations/policy-checks/<policy_check_sha256>.json`

## Related Docs

- [README.md](../README.md)
- [research-reddit.md](research-reddit.md)
- [output-modes.md](output-modes.md)
- [artifacts.md](artifacts.md)
- [overview.md](overview.md)
- [system-design.md](system-design.md)
- [local-runtime-contract.md](local-runtime-contract.md)
- [laya-runtime.md](laya-runtime.md)
- [batch-api-runtime.md](batch-api-runtime.md)
