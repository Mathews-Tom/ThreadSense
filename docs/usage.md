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
- `issues`
- `decisions`
- `feedback`

### Decision subcommands

- `decisions` (`annotate`, `evaluate`)

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
uv run threadsense watch explain --id ID --entry ENTRY_ID [--current] [--config PATH]
uv run threadsense watch policy --id ID [--set PATH | --select POLICY_ID] [--config PATH]
```

Output follows [output-modes.md](output-modes.md): human when stdout is a TTY and JSON otherwise, with `--output-format` overriding. In `json` mode the payload is printed to stdout. In `human` mode a successful payload is printed to stdout as syntax-highlighted JSON, and an error is a red panel on stderr showing the error code, message and details. In `quiet` mode a success prints only its `status` (`created`, `revised`, `unchanged`, `ok`, `presented`, `no_entries`, `rendered`, `selected`, the explanation status for `watch explain`, or the sweep outcome for `watch run`) and an error prints `error: <message>`, both on stdout. Failures exit with code `1`; `watch run` also exits `1` for a `partial` or `failed` sweep and still prints its payload. In `json` mode errors print:

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
  "backlog": {"presentation_limit": 10, "presented_in_digest": 1, "...": "..."},
  "issues": {"run_id": "...", "policy_sha256": "...", "entries": 1, "...": "..."},
  "ranking": {
    "profile_revision": 1, "policy_id": "pol-...", "preference_sequence": 0,
    "objective": "friction_analysis", "policy_name": "objective-default",
    "ranked_entry_ids": ["ent-..."]
  }
}
```

`status` is `presented` when a digest was written or resumed, and `no_entries` when nothing is waiting; then `digest_id`, `sequence`, `json_path`, `markdown_path` and `backlog` are `null` and `entry_ids` is empty. `materialized_entries` counts entries created by this invocation. The command exits `0` in both cases. `issues` is the issue-evidence ingestion result described under [`issues`](#issues) (counts of entries, new issues, lineage assignments, reports, exact copies, evidence, updates, comparisons, suggestions, `suggestions_truncated` and refusals), present whether or not a digest was presented.

`ranking` summarizes the basis the digest was ranked from and its entries in ranked order; `entry_ids` keeps selection order. It is `null` when no digest was presented and for a resumed digest recorded before rankings existed, which keeps its original unranked order. See [`watch explain`](#watch-explain) and [`watch policy`](#watch-policy).

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

### `watch explain`

Explain one digest entry's rank. Read-only: `explain` never writes state.

```bash
uv run threadsense watch explain --id agent-memory --entry ent-... [--current]
```

Every digest records the basis its order was ranked from: the profile revision, the ranking policy id and the latest preference sequence of the watch. `explain` replays that basis against the stored entry payloads, the stored issue references and the hash-verified analysis artifacts, and for a written digest it refuses unless the replayed ranking equals the `ranking` recorded in the digest JSON artifact. The historical order and its reasons are therefore proven rather than restated, and later preferences, policy selections or profile revisions never change them.

```json
{
  "artifact_type": "watch_explanation",
  "status": "ranked",
  "state_path": "<resolved storage root>/monitoring/state.sqlite3",
  "watch_id": "agent-memory",
  "entry_id": "ent-...",
  "digest": {
    "digest_id": "dig-...", "sequence": 3, "status": "written",
    "json_path": "<monitoring directory>/watches/agent-memory/digests/000003-dig-....json",
    "entry_count": 4
  },
  "replay": "matches_written_digest",
  "basis": {
    "profile_revision": 1, "policy_id": "pol-...", "preference_sequence": 2,
    "objective": "friction_analysis", "policy_name": "objective-default"
  },
  "semantic": {"status": "not_eligible", "reason": "no eligible semantic ranking policy exists; ..."},
  "ranking": {
    "rank": 1, "entry_id": "ent-...", "kind": "finding", "score": 66,
    "components": [
      {"component": "severity", "points": 30, "severity": "high", "reason": "finding severity high"},
      {"component": "objective", "points": 20, "objective": "friction_analysis", "signal_type": "issue", "action_type": "...", "recommended_owner": "...", "reason": "objective friction_analysis weights issue signals"},
      {"component": "novelty", "points": 10, "novelty_kind": "new_source_event", "reason": "most novel evidence is new_source_event"},
      {"component": "evidence", "points": 6, "novel_comments": 3, "counted_comments": 3, "reason": "3 of 3 novel comments counted (cap 5)"},
      {"component": "preference", "points": 0, "unbounded_points": 0, "bound": 60, "matched": [], "reason": "no operator preference applies"},
      {"component": "semantic", "points": null, "status": "not_eligible", "reason": "..."}
    ],
    "tie_break": {
      "kind": "finding", "severity": "high", "issue_markers": 2, "request_markers": 0,
      "comment_count": 7, "theme_key": "...", "entry_id": "ent-..."
    }
  },
  "order": [{"rank": 1, "entry_id": "ent-...", "score": 66}, {"rank": 2, "entry_id": "ent-...", "score": 40}]
}
```

`status` describes the entry, not the command:

- `ranked`: the entry is in a digest recorded with a ranking basis. `replay` is `matches_written_digest` for a written digest and `pending_digest` for a digest recorded but not yet written, which a later `watch digest` or `watch run` writes in exactly this order. `ranking` is the entry's full ranking item and `order` lists every entry of that digest by rank.
- `unranked`: the entry is in a digest recorded before rankings existed. That digest presents selection order; `ranking` is `null` and `position` is the entry's 1-based position.
- `unpresented`: the entry is waiting for a digest; `digest` and `ranking` are `null`.

Components, all integers from the policy except `semantic`:

- `severity`: the policy's points for the finding's severity; `0` for thread updates
- `objective`: the profile objective's points for the finding's existing action signal type (`issue`, `request`, `mixed` or `discussion`, classified from the analysis's issue markers, request markers and comment count); `0` for thread updates
- `novelty`: the points of the most novel kind among the entry's new comments; `0` without new comments
- `evidence`: `points_per_novel_comment` times the number of new comments, counting at most `max_counted_comments`
- `preference`: for each scope the entry belongs to (the entry, its finding stream, its thread and its issue), the points of the latest preference in that scope up to the recorded preference sequence; the sum is clamped to `-bound..bound`. `matched` lists each applied preference with its `feedback_id`, `sequence`, `value`, `scope_kind`, `scope_key` and `points`, and `unbounded_points` keeps the sum before clamping
- `semantic`: always `not_eligible` with `points` `null`; it contributes nothing

`score` is the sum of the integer points. Higher scores come first; ties fall back to `tie_break`: findings before thread updates, then higher severity, more issue markers, more request markers, more comments, theme key and finally entry id. Corrections are not a ranking input.

`--current` adds a `current` object: the same entries ranked under the watch's current basis (latest profile revision, active policy and latest preference sequence), with `"recorded": false`, its `basis` and the entry's ranking item. It is derived on demand and never recorded. For an unpresented entry, `current` scores that entry on its own: it has `"rank_scope": "entry_only"` and no `rank`, because its position among the next digest's entries is not known until that digest selects them.

Refusals exit `1` and write nothing; the `reason` is in `details`:

- `invalid_entry_id` (`monitoring_input_error`): `--entry` is not an `ent-` id
- `unknown_watch`: no such watch
- `unknown_entry`: no such digest entry
- `cross_watch_target`: the entry belongs to a different watch
- `ranking_replay_mismatch`: the replayed ranking differs from the one in the written digest
- `artifact_hash_mismatch` or `artifact_missing`: the written digest JSON, or an analysis artifact the ranking reads, no longer matches its recorded hash or is gone
- `unknown_policy` or `ranking_policy_unreadable`: the recorded policy is missing, fails stored validation, or no longer matches its recorded id and canonical bytes
- `digest_state_unreadable` or `ranking_input_unreadable`: a stored digest, entry payload or ranking input cannot be read
- `ranking_basis_missing`: a written digest artifact contains a ranking but its recorded ranking basis is missing

### `watch policy`

Show or select the ranking policy a watch's new digests use.

```bash
uv run threadsense watch policy --id agent-memory
uv run threadsense watch policy --id agent-memory --set ranking-policy.toml
uv run threadsense watch policy --id agent-memory --select pol-...
```

Without `--set` or `--select` the command is read-only and prints the active policy and the watch's selection history:

```json
{
  "artifact_type": "ranking_policy",
  "status": "ok",
  "state_path": "<resolved storage root>/monitoring/state.sqlite3",
  "watch_id": "agent-memory",
  "active_policy_id": "pol-...",
  "active_policy": {"policy_version": 1, "name": "objective-default", "severity": {"high": 30, "...": "..."}, "...": "..."},
  "default_policy_id": "pol-...",
  "selections": [{"sequence": 1, "policy_id": "pol-...", "name": "friction-first", "selected_at_utc": 1767225600.0}]
}
```

A watch with no selections uses the built-in default policy. Policies are immutable and content-addressed: the `pol-` id is derived from the SHA-256 of the policy's canonical JSON, so the same document always has the same id. `--set PATH` validates a TOML policy file, records it if its id is new and selects it. `--select POLICY_ID` switches back to a policy already recorded in the state database, or to the built-in default by its `default_policy_id`. Selecting appends a selection event; earlier selections stay in the history. A selection affects only digests recorded afterwards: digests already recorded, including a pending digest that is resumed later, keep the basis recorded with them, and written digests are never rewritten.

```json
{
  "artifact_type": "ranking_policy_selection",
  "status": "selected",
  "state_path": "<resolved storage root>/monitoring/state.sqlite3",
  "watch_id": "agent-memory",
  "policy_id": "pol-...",
  "previous_policy_id": "pol-...",
  "active_policy": {"policy_version": 1, "name": "friction-first", "...": "..."}
}
```

`status` is `selected` when the active policy changed and `unchanged` when the policy was already active; no selection event is appended then.

A policy file has exactly the tables and keys below; every key is required, values are TOML integers, and unknown keys are refused. This document is the built-in default, so `--set` with it selects the default policy's id:

```toml
policy_version = 1
name = "objective-default"

[severity]
high = 30
medium = 20
low = 10

[objective.friction_analysis]
issue = 20
mixed = 15
request = 5
discussion = 0

[objective.feature_demand]
issue = 5
mixed = 15
request = 20
discussion = 0

[objective.sentiment_mapping]
issue = 5
mixed = 5
request = 5
discussion = 10

[objective.competitive_intelligence]
issue = 10
mixed = 15
request = 10
discussion = 5

[objective.general_survey]
issue = 0
mixed = 0
request = 0
discussion = 0

[novelty]
new_source_event = 10
edited = 5
newly_observed_historical = 2
initial_observation = 0

[evidence]
points_per_novel_comment = 2
max_counted_comments = 5

[preference]
useful = 40
irrelevant = -40
already_known = -25
wrong_audience = -30
bound = 60
```

| Field | Allowed values |
| --- | --- |
| `policy_version` | `1` |
| `name` | lowercase slug of 1-64 characters: letters, digits and hyphens, starting and ending with a letter or digit |
| `severity.high`, `.medium`, `.low` | `0` to `100` |
| `objective.<objective>.issue`, `.mixed`, `.request`, `.discussion` | `-100` to `100`, for each of the five profile objectives |
| `novelty.new_source_event`, `.edited`, `.newly_observed_historical`, `.initial_observation` | `0` to `100` |
| `evidence.points_per_novel_comment` | `0` to `20` |
| `evidence.max_counted_comments` | `0` to `50` |
| `preference.useful` | `0` to `100` |
| `preference.irrelevant`, `.already_known`, `.wrong_audience` | `-100` to `0` |
| `preference.bound` | `0` to `100` |

Refusals exit `1` and write nothing; the `reason` is in `details`:

- `policy_file_missing` (`monitoring_input_error`): the `--set` file does not exist
- `policy_file_unreadable` (`monitoring_input_error`): the `--set` path cannot be read as a file (for example a directory, unreadable permissions or invalid UTF-8)
- `invalid_ranking_policy` (`monitoring_input_error`): the file is not valid TOML, or a field is missing, unknown, not an integer or out of range; `details.field` names the field that failed validation
- `unknown_policy`: `--select` names a malformed id or a policy not recorded in this state database
- `unknown_watch`: no such watch
- `ranking_policy_unreadable`: a stored policy is malformed, fails validation, or no longer matches its recorded id and canonical bytes. This is a `monitoring_state_error`; invalid operator-supplied policy documents remain `monitoring_input_error`.

### State location

State is a SQLite database at `<storage root>/<monitoring_dirname>/state.sqlite3`, by default `.threadsense/monitoring/state.sqlite3`. Set `monitoring_dirname` under `[storage]` in the config file or with `THREADSENSE_STORAGE_MONITORING_DIR`. The value must be a relative path that stays inside the storage root; absolute paths and `..` segments are rejected. See [artifacts.md](artifacts.md) for the schema and version behavior.

### Run claims

The `threadsense.monitoring` Python package provides the run-claim primitive that `watch run` and `watch digest` use, together with the `run_watch_sweep` and `present_watch_digest` library APIs. `watch inspect` reports claims, sweeps and digests recorded in the database.

- at most one run per watch is `running`, enforced transactionally and by a partial unique index
- a claim carries an owner token and a lease of 1-86400 seconds; only the SHA-256 of the token is stored
- a competing claim while the lease is live is refused with `run_claim_error`, reason `claim_held`
- after the lease expires, the next claim marks the old run `abandoned` with `recovered_by_run_id` pointing at the new run
- the previous owner can no longer finalize that run (reason `stale_owner`); an owner whose lease expired without a newer claim cannot finalize either (reason `lease_expired`)

## `issues`

Read and correct cross-thread issue history. An issue groups the watch threads that report the same problem. Every watch thread with finding evidence has a home issue identified by that thread; the history is append-only, so inspection never mutates state and `link` and `unlink` only add events.

```bash
uv run threadsense issues inspect --watch ID [--issue ISSUE_ID] [--config PATH]
uv run threadsense issues link --watch ID --issue ISSUE_ID --thread THREAD_ID --reason TEXT [--config PATH]
uv run threadsense issues unlink --watch ID --issue ISSUE_ID --thread THREAD_ID --reason TEXT [--config PATH]
```

Output modes and the error payload are as for `watch`. A refusal is a `monitoring_state_error` (or `monitoring_input_error` for a malformed request) with the refusal `reason` in `details`, exit code `1`, and nothing is written.

### How evidence is recorded

`watch run` and `watch digest` record issue evidence for every finding entry they materialize, after materialization and before the digest is created, using the `[issues]` settings below. A report is one source comment cited as finding evidence. Its recorded lineage is fixed when first recorded:

- `exact_copy` when the whitespace-collapsed text has at least 40 characters and is identical to an earlier firsthand report from another comment, where the origin was posted no later than the copy
- `firsthand` otherwise

A new thread joins another issue automatically only when every report first recorded for it is an exact copy of a report in another thread; the thread then joins the issue of the earliest such origin (basis `exact_content_lineage`). Any firsthand report makes the thread its own issue (basis `thread_seed`). A partial copy never groups; it is a candidate signal (`exact_copies`) only. After assignment each new thread is compared with a bounded set of other threads whose first evidence falls inside the candidate window, and shared themes or key phrases become `unconfirmed` suggestions. A shared link without shared themes or phrases is recorded as `refused` with reason `shared_link_only`. Suggestions never change memberships or counts.

Recorded lineage is an ingestion-time fact and is never edited. A later report can be ingested before its earlier original, so a repost may be recorded `firsthand`. When a later ingestion discovers that earlier original, it appends a correction event to its ingestion-ledger row (`watch_issue_ingestions.summary_json`), and the public ingestion summary reports the number as the integer `lineage_corrections` (`0` when none). Each event in `lineage_events` carries `watch_id`, `run_id`, `policy_sha256`, `report_id`, `origin_report_id`, `previous_origin_report_id`, `text_sha256`, `discovered_report_id` and `discovery_entry_id`. The latest event for a report sets its effective lineage (`exact_copy` of `origin_report_id`); counts and update cues use effective lineage. A correction never regroups threads: grouping stays an operator action, and suggestions stay `unconfirmed` until an operator runs `link`. Raw reports, evidence, earlier ledger rows and digests already recorded are never rewritten.

Update cues are lexical: a firsthand report records an update of kind `reproduction`, `contradiction`, `workaround` or `resolution_claim` when its text contains one of the fixed cue phrases (word-boundary, case-insensitive). An exact copy records cues only for text its origin never had. Updates are unverified claims; a `resolution_claim` never resolves an issue.

### `issues inspect`

Without `--issue`, list the issues of a watch in creation order:

```json
{
  "artifact_type": "issue_list",
  "status": "ok",
  "state_path": "<resolved storage root>/monitoring/state.sqlite3",
  "watch_id": "agent-memory",
  "issues": [
    {
      "issue_id": "iss-...",
      "home_thread_id": "...",
      "state": "active",
      "member_thread_ids": ["..."],
      "counts": {"observed_threads": 1, "...": "..."},
      "update_counts": {"reproduction": 0, "contradiction": 0, "workaround": 0, "resolution_claim": 0},
      "unconfirmed_suggestions": 1
    }
  ]
}
```

A watch without issue history, or a state database older than schema version `4`, lists `[]`. `state` is `active` while the home thread is a member of its own issue and `inactive` once it has been linked elsewhere. `unconfirmed_suggestions` counts unconfirmed suggestions that touch a member and are not currently grouped.

With `--issue`, inspect one issue:

```json
{
  "artifact_type": "issue_inspection",
  "status": "ok",
  "state_path": "<resolved storage root>/monitoring/state.sqlite3",
  "issue_status": "open",
  "issue_id": "iss-...",
  "watch_id": "agent-memory",
  "home_thread_id": "...",
  "state": "active",
  "resolution": {"resolved": false, "resolution_claims": 0, "basis": "resolution claims are unverified updates; a claim never resolves an issue"},
  "members": [{"source_thread_id": "...", "thread_url": "...", "subreddit": "...", "title": "...", "basis": "thread_seed", "joined_event_id": 1, "joined_at_utc": 0.0}],
  "counts": {
    "observed_threads": 1, "observed_communities": 1, "observed_accounts": 2,
    "reports_without_account": 0, "reports": 2, "firsthand_reports": 2, "exact_copies": 0,
    "independence": {"status": "not_verified", "verified_independent_reports": null, "basis": "..."}
  },
  "update_counts": {"reproduction": 0, "contradiction": 0, "workaround": 0, "resolution_claim": 0},
  "reports": [{"report_id": "rpt-...", "lineage": "firsthand", "copy_of_report_id": null, "evidence": [{"entry_id": "ent-...", "text_sha256": "...", "links": []}], "...": "..."}],
  "updates": [{"update_id": "upd-...", "kind": "workaround", "cue": "workaround", "report_id": "rpt-...", "...": "..."}],
  "membership_history": [{"event_id": 1, "action": "assigned", "basis": "thread_seed", "...": "..."}],
  "suggestions": [{"suggestion_id": "sug-...", "status": "unconfirmed", "refusal_reason": null, "signals": {"themes": [], "key_phrases": [], "links": [], "source_time_gap_seconds": 0.0}, "currently_grouped": false, "...": "..."}],
  "grouping_policy": {"sha256": "...", "...": "..."}
}
```

Each report's `lineage` and `copy_of_report_id` are the effective values: the recorded lineage overridden by the latest correction event. `recorded_lineage` and `recorded_copy_of_report_id` keep the ingestion-time facts, and `lineage_correction` is the latest correction event or `null`. `firsthand_reports` and `exact_copies` follow effective lineage, while `observed_*` counts do not change. An update cue of a corrected copy whose text equals the copied text is excluded from `update_counts`, and each inspected update gains `copied_from_report_id`. Digests recorded earlier keep their original lineage; a new digest captures effective lineage at presentation and omits copied-first-text cues from its entry updates while retaining cues in edited-added text.

`issue_status` is always `open`; ThreadSense has no resolved state. `observed_*` counts are observations over member threads (accounts are SHA-256 hashes of source and username; `reports_without_account` counts deleted or blank authors). Distinct accounts or communities are not proof of independent sources, so `independence.status` is always `not_verified`, and `exact_copies` are listed but never corroborate a firsthand report. An unknown issue, an issue of another watch or a state database older than schema version `4` is refused with reason `unknown_issue`.

### `issues link` and `issues unlink`

`link` moves a thread into another active issue; `unlink` returns a linked thread to its own home issue. `--reason` is required, is stored stripped, and must be 1-500 characters without control characters.

```json
{
  "artifact_type": "issue_membership_change",
  "status": "linked",
  "state_path": "<resolved storage root>/monitoring/state.sqlite3",
  "event": {
    "event_id": 7, "watch_id": "agent-memory", "source_thread_id": "...", "issue_id": "iss-...",
    "previous_issue_id": "iss-...", "action": "linked", "basis": "operator", "reason": "...",
    "report_id": null, "policy_sha256": null, "run_id": null, "created_at_utc": 0.0
  },
  "membership": {"source_thread_id": "...", "issue_id": "iss-...", "home_issue_id": "iss-..."}
}
```

`unlink` returns the same shape with `status: "unlinked"` and `action: "unlinked"`. Reverse a link with `unlink`, or a lineage assignment with `unlink` too; the earlier events stay in `membership_history`.

Refusal reasons:

- `invalid_issue_request`: a malformed watch id, issue id, thread id or reason (`details.field` names it)
- `unknown_watch`: no such watch
- `unknown_issue`: no such issue in this watch
- `unknown_issue_thread`: the thread has no issue history in this watch
- `already_member` (link): the thread is already in the target issue
- `inactive_issue` (link): the target issue's home thread has been linked elsewhere
- `issue_has_other_members` (link): the thread's own issue still has other member threads (`details.member_thread_ids`)
- `not_member` (unlink): the thread is not in the named issue
- `not_linked` (unlink): the thread is already in its own home issue

### `[issues]` configuration

Bounds on the candidate comparison made for each newly tracked thread:

| Key | Environment variable | Default | Range |
| --- | --- | --- | --- |
| `candidate_window_days` | `THREADSENSE_ISSUES_CANDIDATE_WINDOW_DAYS` | `30` | 1-365 |
| `max_candidate_comparisons` | `THREADSENSE_ISSUES_MAX_CANDIDATE_COMPARISONS` | `25` | 1-500 |
| `max_suggestions_per_thread` | `THREADSENSE_ISSUES_MAX_SUGGESTIONS_PER_THREAD` | `3` | 1-50 |

Candidates must have first evidence within `candidate_window_days` of the new thread's first evidence, are compared nearest first up to `max_candidate_comparisons`, and at most `max_suggestions_per_thread` unconfirmed suggestions are kept per thread (candidates beyond that are counted in `suggestions_truncated`; shared-link refusals are all kept). Out-of-range values are rejected when the configuration loads. The effective settings are stored as a policy row and cited by `grouping_policy` on every issue.

### Digest issue section

A digest created by a state database at schema version `5` or `6` is a `schema_version: 2` JSON artifact with a top-level `issues` section (at schema version `7` it is `schema_version: 3` and also carries a `ranking` section; see [`watch explain`](#watch-explain)), and its Markdown artifact gains an Issues section and per-entry issue lines. A digest still pending from an earlier schema version keeps `schema_version: 1` and no issue section. See [artifacts.md](artifacts.md) for the recorded shape.

## `feedback`

Record and read operator feedback about a watch's digest entries, thread revisions and issues. Feedback has two kinds kept in separate append-only tables. A preference is a subjective presentation signal: digests recorded after it rank its entries accordingly (see [`watch policy`](#watch-policy)), and it never selects, hides or drops an entry. A correction is an operator assertion recorded beside the unchanged original: it never edits evidence, findings, counts, issue membership or written digests and reports, and it is not a truth label, training data or a ranking input.

```bash
uv run threadsense feedback record --watch ID --target TARGET_ID --kind preference|correction --value VALUE --key KEY [--target-kind entry|finding|observation|issue] [--note TEXT] [--config PATH]
uv run threadsense feedback list --watch ID [--target TARGET_ID] [--config PATH]
```

Output modes and the error payload are as for `watch`. A refusal is a `monitoring_state_error` (or `monitoring_input_error` for a malformed request) with the refusal `reason` in `details`, exit code `1`, and nothing is written.

### Targets

The target id prefix names the target; `--target-kind` defaults to the narrowest kind the id names.

| Target kind | Id prefix | Target | A preference applies to |
| --- | --- | --- | --- |
| `entry` | `ent-` | one digest entry | that entry |
| `finding` | `ent-` (a finding entry) | the thread's finding stream for the entry's theme | every revision of that thread's findings for the theme |
| `observation` | `rev-` | one thread revision | the whole thread |
| `issue` | `iss-` | one issue | that issue |

`--target-kind finding` widens a preference from one entry to the thread's finding stream for that theme and requires an entry of kind `finding`. Every target is resolved against the watch's own state before anything is written.

Retry identity includes the resolved target kind as well as its id. Omitting `--target-kind` and explicitly supplying the default kind are equivalent retries. Recording the same `ent-` id once as `entry` and once as `finding` is different feedback and requires a different key.

### `feedback record`

| Flag | Required | Meaning |
| --- | --- | --- |
| `--watch` | yes | Watch id |
| `--target` | yes | Target id (`ent-`, `rev-` or `iss-`) |
| `--target-kind` | no | `entry`, `finding`, `observation` or `issue`; defaults to the kind the id names |
| `--kind` | yes | `preference` or `correction` |
| `--value` | yes | Preference: `useful`, `irrelevant`, `already_known`, `wrong_audience`. Correction: `misclassified`, `unsupported`, `duplicate`, `missing_evidence` |
| `--key` | yes | Watch-scoped idempotency key: 1-128 characters from letters, digits, `.`, `_`, `:` and `-`, starting with a letter or digit |
| `--note` | no | Operator note, stored stripped, 1-500 characters without control characters |

Examples:

```bash
uv run threadsense feedback record --watch agent-memory --target ent-... --target-kind finding --kind preference --value already_known --key triage-2026-10-20-1
uv run threadsense feedback record --watch agent-memory --target iss-... --kind correction --value duplicate --key triage-2026-10-20-2 --note "Same report as the home thread"
```

Preferences and corrections share one idempotency namespace per watch. Repeating a key with identical kind, value, target, resolved target kind (`--target-kind`, or the default its id prefix names when omitted) and note returns the original event with `status: "unchanged"`; reusing a key for different feedback is refused. The feedback id is `fbk-` followed by the first 32 hex characters of the SHA-256 of the watch id and key.

```json
{
  "artifact_type": "feedback_event",
  "status": "recorded",
  "state_path": "<resolved storage root>/monitoring/state.sqlite3",
  "feedback": {
    "kind": "preference", "watch_id": "agent-memory", "feedback_id": "fbk-...", "sequence": 1,
    "value": "already_known", "target_kind": "finding", "target_id": "ent-...",
    "source_thread_id": "...", "theme_key": "...", "scope_kind": "finding", "scope_key": "<thread id>/<theme key>",
    "note": null, "recorded_at_utc": 0.0, "idempotency_key": "triage-2026-10-20-1", "content_sha256": "..."
  }
}
```

A correction event has the same shape without `scope_kind` and `scope_key`. `sequence` counts each kind separately per watch.

Refusal reasons:

- `invalid_feedback_request`: a malformed watch id, key, kind, target id, target kind or note (`details.field` names it)
- `value_kind_mismatch`: the value does not belong to the kind (`details.allowed_values` lists those that do)
- `target_kind_mismatch`: the target id does not name the target kind, or `finding` names an entry that is not a finding
- `unknown_watch`: no such watch
- `unknown_target`: no such target
- `cross_watch_target`: the target belongs to a different watch
- `idempotency_conflict`: the key already recorded different feedback for this watch (`details.feedback_id` names it)

### `feedback list`

Read-only: `list` never writes state. Without `--target`, list all feedback for the watch; with `--target`, only events recorded against that target id, which must exist in the watch. A state database older than schema version `6` lists empty histories.

```json
{
  "artifact_type": "feedback_history",
  "status": "ok",
  "state_path": "<resolved storage root>/monitoring/state.sqlite3",
  "watch_id": "agent-memory",
  "preferences": [{"kind": "preference", "feedback_id": "fbk-...", "sequence": 1, "value": "already_known", "...": "..."}],
  "effective_preferences": [
    {
      "scope_kind": "finding", "scope_key": "<thread id>/<theme key>", "value": "already_known",
      "feedback_id": "fbk-...", "sequence": 1, "target_kind": "finding", "target_id": "ent-...",
      "superseded_feedback_ids": []
    }
  ],
  "corrections": [{"kind": "correction", "feedback_id": "fbk-...", "sequence": 1, "value": "duplicate", "...": "..."}],
  "correction_view": [
    {
      "target_kind": "issue", "target_id": "iss-...",
      "original": {"home_thread_id": "..."},
      "effect": "assertion_only",
      "assertions": [{"feedback_id": "fbk-...", "sequence": 1, "value": "duplicate", "note": "Same report as the home thread"}]
    }
  ]
}
```

`effective_preferences` holds the latest preference per scope; earlier preferences in that scope stay in `preferences` and are listed in `superseded_feedback_ids`. A target filter limits raw history to that target, but effective preferences still use the full watch history for its scopes, so a newer event against another revision can supersede the target's earlier preference. `correction_view` groups corrections by target beside the unchanged original facts: for an entry its kind, thread, revision, theme, severity and payload SHA-256; for an observation its thread, revision sequence and content hashes; for an issue its home thread. `effect` is always `assertion_only`.

## `decisions`

Shadow typed semantic annotation and frozen assessment. `decisions annotate` uses a separate, operator-provisioned local Laya service, independent of the generative runtime used by `infer` and `THREADSENSE_RUNTIME_ENABLED`. `decisions evaluate` replays saved typed artifacts and can compare themes using the configured local generative runtime or a frozen capture. Neither command changes baseline findings, reports or monitoring state, or installs or activates a policy.

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

Assessment of saved decision documents against a frozen dataset manifest and its labels. It replays recorded typed exchanges offline, scores typed decisions per partition, compares themes with the existing keyword heuristic, and publishes a content-addressed evaluation plus a run record. An available local generative model can supply a separate paired theme comparison; a saved capture makes that comparison replayable offline.

```bash
threadsense decisions evaluate --manifest PATH \
  --annotation PATH [--annotation PATH ...] \
  [--generative-capture PATH] \
  [--output-dir DIR] [--config PATH]
```

| Flag | Meaning |
| --- | --- |
| `--manifest PATH` | Required. A frozen dataset manifest (`typed_decision_evaluation_manifest`) that binds every thread file and the label file by sha256 |
| `--annotation PATH` | Required, repeatable. A saved decision document from `decisions annotate`; supply exactly one per manifest thread |
| `--generative-capture PATH` | Replay an immutable generative capture offline; no model listing or inference. Omit for fresh acquisition when the configured local runtime is available |
| `--output-dir DIR` | Artifact root; defaults to the annotation root, `<storage-root>/annotations` |
| `--config PATH` | Configuration file, as for other commands |

What it does and does not do:

- It never contacts the Laya service and never re-annotates. Saved decisions must have been produced under the exact identities the manifest binds (runtime manifest, checkpoint revision, question set, settings, context policy and language), and those bindings must still equal the current pinned identities.
- Without `--generative-capture`, an enabled literal-loopback runtime is queried for its model listing and, when the configured model is served, for theme reclassification. Exact raw exchanges and identities are captured immutably. Disabled runtime and invalid-endpoint guards are `not_observed` (no request); configured-model absence or an observed request failure is explicitly unavailable. Optional generation failures, including invalid response numbers or decoding failures, record `inference_failed` without discarding valid typed/heuristic evidence or publishing a capture. No missing comparison is scored as a loss. The repository-default model, `gemma4:latest`, must actually be served; configuration alone is not availability.
- With `--generative-capture PATH`, the evaluator validates the capture's file hash, envelope digest, frozen input bindings and request layout, then re-derives outcomes from retained responses without any model listing or inference. Invalid replay is refused rather than treated as an unavailable fresh comparison. The run record reports `available` / `replayed_frozen_capture`, with null `method` and `served_models`: no current runtime observation was made. Its nested `captured_availability` retains the original acquisition observation, not a claim that the model is currently served.
- The paired theme cohort contains only typed-eligible, supported-language comments of at most 300 characters, without body truncation. Typed, heuristic and generative metrics use the same thread/comment IDs and denominators. Longer comments and the other five dimensions remain explicitly unsupported. A default or novel generative theme maps to `other_or_uncertain` abstention.
- Results describe the listed inputs against their declared labels only. They are not population accuracy. The shipped labels (`tests/fixtures/decisions/evaluation/`) are engineering-authored boundary expectations; they are not independent adjudication and not ground truth.
- `answer_confidence` is uncalibrated and is not a probability of correctness.
- Unavailable labels and unavailable comparators are reported as unavailable with a reason, never as zero error.
- Option stability is reported as vacuous: each question has one fixed option layout and no permuted-layout probe exists yet.
- Count reconciliation establishes structural invariants; corpus/annotation alignment is checked separately against the bound inputs. `labels.status` describes label presence, not decision coverage: error rate remains `null` without labeled decisions.
- The exactly-2000-character `fixture:lc-3` fits the typed client's budget, but the pinned checkpoint may report context truncation and be refused. Its labels then remain `not_covered`, not errors; authored rationales do not promise acceptance.

Output locations follow the `decisions annotate` rules: the output root must not lie inside or contain baseline or monitoring storage, and no output path may alias the manifest, the label file, a thread file or an annotation. See [artifacts.md](artifacts.md#typed-decision-evaluation) for the input formats, the document, metric definitions and refusal reasons.

Printed JSON payload (`artifact_type: decision_evaluation`, `status: ready`, `mode: shadow`):

| Field | Content |
| --- | --- |
| `evaluation_path`, `evaluation_sha256` | The published evaluation document and the sha256 of its bytes |
| `run_path`, `run_id` | The run record of this invocation |
| `manifest_sha256` | The sha256 of the manifest bytes |
| `label_origin`, `independent_adjudication` | The declared label origin and whether it is independent adjudication |
| `partitions` | Per partition (`development`, `calibration`, `evaluation`): threads, comments, ok, refused, unavailable, excluded_language and eligible counts |
| `generative_comparator` | This run's availability `status` and `reason`, plus `capture_path` and `capture_sha256` (complete-file SHA-256), or null capture fields when none was acquired |

Exit codes: `0` when the evaluation and run record are published; `1` on any refusal (stale binding, missing, duplicate, unbound or mismatched annotation, replay mismatch, partition leak, invalid input, refused output location). Refusals print the error JSON with `details.reason`.

Replaying the same frozen inputs and supplied generative capture reproduces the same evaluation bytes, SHA-256 and path and adds a new run record. Fresh acquisition without a capture may produce a different model response and is not advertised as deterministic replay. When no comparison is available, transient availability stays in the run record rather than changing the frozen typed/heuristic assessment.

For offline generative replay, repeat the command with `--generative-capture <capture-path>` using the capture path reported by the fresh acquisition. Keep the original immutable capture bytes and content-addressed `<capture_file_sha256>.json` filename. Changing the corpus, annotations, labels or question identity is not replay of that comparison; the runtime configuration must also match the capture's bound request identity.

Example:

```bash
uv run threadsense decisions evaluate \
  --manifest <dataset-dir>/manifest.json \
  --annotation <annotation-root>/<snapshot_sha256>/decisions/<decision_sha256>.json \
  --annotation <annotation-root>/<snapshot_sha256>/decisions/<decision_sha256>.json
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
- `.threadsense/annotations/evaluations/<manifest_sha256>/generative-captures/<capture_file_sha256>.json`
- `.threadsense/annotations/<snapshot_sha256>/{decisions,runs}/...`
- `.threadsense/annotations/evaluations/<manifest_sha256>/<evaluation_sha256>.json`
- `.threadsense/annotations/evaluations/<manifest_sha256>/runs/<run_id>.json`

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
