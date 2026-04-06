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

## Happy Paths

### Single-thread run

```bash
uv run threadsense run reddit <reddit-url> \
  [--format markdown|html|json] \
  [--with-summary] \
  [--summary-required] \
  [--expand-more] \
  [--flat]
```

Example:

```bash
uv run threadsense run reddit \
  "https://www.reddit.com/r/ClaudeCode/comments/1ro0qbl/anyone_actually_built_a_second_brain_that_isnt/" \
  --format markdown \
  --with-summary \
  --summary-required
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

## Related Docs

- [README.md](../README.md)
- [research-reddit.md](research-reddit.md)
- [output-modes.md](output-modes.md)
- [artifacts.md](artifacts.md)
- [overview.md](overview.md)
- [system-design.md](system-design.md)
- [local-runtime-contract.md](local-runtime-contract.md)
- [batch-api-runtime.md](batch-api-runtime.md)
