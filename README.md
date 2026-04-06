# ThreadSense

ThreadSense is an evidence-first discussion intelligence system for turning public discussion threads into structured, inspectable product and research intelligence.

It combines:

- source connectors for Reddit, Hacker News, and GitHub Discussions
- canonical normalization into a stable thread model
- deterministic analysis with evidence-linked findings
- optional local-model synthesis through an OpenAI-compatible runtime
- report generation for single threads and cross-thread corpora
- operator-facing CLI output in JSON, human, and quiet modes

## What You Can Do

### Analyze one thread end to end

```bash
uv run threadsense run reddit \
  "https://www.reddit.com/r/ClaudeCode/comments/1ro0qbl/anyone_actually_built_a_second_brain_that_isnt/" \
  --format markdown \
  --with-summary \
  --summary-required
```

### Research a topic across selected subreddits

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

This workflow:

1. searches the selected subreddits
2. ranks and selects matching threads deterministically
3. fetches and analyzes each selected thread
4. builds a corpus manifest and corpus analysis
5. writes a corpus markdown report
6. prints a compact terminal summary in human mode

## Core Features

- Reddit thread ingestion through the public JSON API
- Reddit topic research across selected subreddits
- canonical thread artifacts with top-level thread body support
- deterministic issue/request/theme extraction
- optional local-runtime summaries for thread reports and corpus reports
- Markdown, HTML, and JSON report outputs for single-thread runs
- Markdown corpus reports for topic research and corpus reporting
- terminal summaries for single-thread `run` and `research reddit` in human mode

## Supported Sources

Implemented connector support:

- Reddit
- Hacker News
- GitHub Discussions

Current research discovery support:

- Reddit only

## Output Modes

ThreadSense supports three output modes:

- `json`: machine-readable payloads
- `human`: Rich terminal panels and summaries
- `quiet`: status-only output

Force human mode explicitly when you want the terminal summary panels:

```bash
uv run threadsense --output-format human research reddit ...
```

See [docs/output-modes.md](docs/output-modes.md) for details.

## Quickstart

### 1. Install dependencies

```bash
uv sync
```

### 2. Validate your local setup

```bash
uv run threadsense preflight
```

### 3. Run a single-thread workflow

```bash
uv run threadsense run reddit \
  "https://www.reddit.com/r/ClaudeCode/comments/1ro0qbl/anyone_actually_built_a_second_brain_that_isnt/"
```

### 4. Run topic research

```bash
uv run threadsense research reddit \
  --query "second brain OR agentic PKM" \
  --subreddit ClaudeCode \
  --subreddit LocalLLaMA \
  --subreddit AI_Agents
```

## Research Query Notes

`research reddit` supports a deliberately narrow query style so local deterministic matching stays aligned with retrieval:

- supported: `OR`, `|`
- unsupported: quotes, parentheses, `title:`, `selftext:`, negation-style advanced Reddit syntax

Example supported query:

```text
second brain OR agentic PKM
```

This is executed as a union of clause searches, then deterministically filtered and ranked locally.

See [docs/research-reddit.md](docs/research-reddit.md) for the full workflow.

## Main CLI Workflows

- `preflight`
- `fetch`
- `normalize`
- `analyze`
- `infer`
- `report`
- `inspect`
- `corpus`
- `batch run`
- `research reddit`
- `serve`
- `run`

The detailed command reference lives in [docs/usage.md](docs/usage.md).

## Artifacts

ThreadSense persists separate artifacts for each stage so runs are inspectable and rerunnable.

Typical layout under `.threadsense`:

- `raw/<source>/...`
- `normalized/<source>/...`
- `analysis/<source>/...`
- `reports/<source>/...`
- `corpora/<corpus-id>/manifest.json`
- `corpora/<corpus-id>/analysis.json`
- `corpora/<corpus-id>/report.md`
- `batches/<run-name>.json`

See [docs/artifacts.md](docs/artifacts.md) for the full layout and artifact responsibilities.

## Runtime

ThreadSense can run without a local model, but summaries require a local OpenAI-compatible runtime.

Default endpoint:

- base URL: `http://127.0.0.1:8080`
- chat path: `/v1/chat/completions`

See [docs/local-runtime-contract.md](docs/local-runtime-contract.md).

## Documentation Map

- [docs/usage.md](docs/usage.md): command reference
- [docs/research-reddit.md](docs/research-reddit.md): subreddit topic research workflow
- [docs/output-modes.md](docs/output-modes.md): JSON, human, and quiet output modes
- [docs/artifacts.md](docs/artifacts.md): persisted artifact types and storage layout
- [docs/overview.md](docs/overview.md): product and workflow overview
- [docs/system-design.md](docs/system-design.md): implemented architecture and boundaries
- [docs/local-runtime-contract.md](docs/local-runtime-contract.md): local inference contract
- [docs/batch-api-runtime.md](docs/batch-api-runtime.md): batch, API, and runtime notes
- [docs/pitch.md](docs/pitch.md): product positioning

## Validation

```bash
uv run ruff check
uv run ruff format --check .
uv run mypy --strict src tests
uv run pytest
./scripts/validate.sh
```

## Current Limits

- Reddit topic research is implemented; broader cross-source research discovery is not
- Reddit research query grammar is intentionally limited
- corpus reports are currently Markdown only
- the local API remains a trusted local surface, not a hardened public service

## Direction

The current system is beyond the original single-thread MVP. The highest-value next areas are:

- richer corpus presentation and operator workflows
- more discovery workflows beyond Reddit subreddit search
- stronger evaluation and replay benchmarking
- better source-distribution and research-quality reporting
