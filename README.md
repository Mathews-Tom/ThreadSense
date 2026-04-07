# ThreadSense

**Discussion intelligence, not scraping.** ThreadSense is a reproducible pipeline that turns public discussion threads into structured, evidence-backed product and research intelligence.

## Why ThreadSense

Scrapers give you raw payloads. AI summarizers give you prose. Neither gives you a defensible basis for decisions.

ThreadSense keeps the evidence chain intact at every stage:

1. **Acquire** — source connectors fetch threads from Reddit, Hacker News, GitHub Discussions, and GitHub Gists
2. **Normalize** — source-specific payloads are mapped into a canonical thread model with provenance metadata
3. **Analyze** — deterministic extraction identifies issues, feature requests, themes, and sentiment — each linked to the comment that produced it
4. **Synthesize** — optional local-model inference adds summaries on top of the deterministic evidence layer
5. **Report** — structured outputs in Markdown, HTML, or JSON, with full traceability from finding back to source comment

Every stage produces a persisted, inspectable artifact. Rerun any stage independently. Diff results across runs. Audit exactly where a finding came from.

## What This Enables

### Single-thread analysis

Feed a discussion URL. Get structured findings — issues, requests, themes, severity — with every claim linked to the comment that produced it.

```bash
uv run threadsense run reddit \
  "https://www.reddit.com/r/ClaudeCode/comments/1ro0qbl/anyone_actually_built_a_second_brain_that_isnt/" \
  --format markdown \
  --with-summary \
  --summary-required
```

Works with GitHub Gists too — flat-comment discussions with code context:

```bash
uv run threadsense run gist \
  "https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f" \
  --format markdown
```

### Cross-thread research

Search a topic across multiple subreddits. ThreadSense deterministically selects, ranks, and analyzes matching threads, then synthesizes a corpus-level report.

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

### Domain-aware analysis

The analysis layer uses a contract system with domain-specific vocabularies (developer tools, product feedback, hiring, research, financial markets, gaming). Each domain defines its own theme keywords, issue markers, and severity calibration — so analysis adapts to context rather than applying one-size-fits-all heuristics.

## Architecture

```text
fetch → normalize → analyze → [optional inference] → report
         ↓              ↓              ↓                ↓
     raw artifact   canonical     analysis         report artifact
                    artifact      artifact
```

- **Deterministic core** — parsing, normalization, scoring, and selection are reproducible across runs
- **Inference on top** — LLM synthesis is optional and layered over deterministic evidence, never a substitute for it
- **Stable artifacts** — each stage persists a separate JSON artifact with `schema_version` and SHA256 provenance
- **Fail fast** — invalid URLs, malformed payloads, and schema inconsistencies surface immediately

## Sources and Discovery

| Capability      | Reddit | Hacker News | GitHub Discussions | GitHub Gists |
| --------------- | :----: | :---------: | :----------------: | :----------: |
| Thread analysis |  yes   |     yes     |        yes         |     yes      |
| Topic research  |  yes   |      —      |         —          |      —       |

## Output Modes

| Mode    | Purpose                                          |
| ------- | ------------------------------------------------ |
| `json`  | Machine-readable payloads for downstream tooling |
| `human` | Rich terminal panels and summaries for operators |
| `quiet` | Status-only output for scripts and CI            |

```bash
uv run threadsense --output-format human research reddit ...
```

See [docs/output-modes.md](docs/output-modes.md) for details.

## Who This Is For

- **Product teams** validating pain points and feature demand from community discussions
- **Founders** doing market and competitor research across technical communities
- **DevRel teams** tracking developer workflow friction and tooling sentiment
- **Researchers** studying technical communities with reproducible methodology

## Quickstart

```bash
# 1. Install
uv sync

# 2. Validate local setup
uv run threadsense preflight

# 3. Analyze a single thread
uv run threadsense run reddit \
  "https://www.reddit.com/r/ClaudeCode/comments/1ro0qbl/anyone_actually_built_a_second_brain_that_isnt/"

# 4. Analyze a GitHub Gist
uv run threadsense run gist \
  "https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f"

# 5. Research a topic across subreddits
uv run threadsense research reddit \
  --query "second brain OR agentic PKM" \
  --subreddit ClaudeCode \
  --subreddit LocalLLaMA \
  --subreddit AI_Agents
```

## CLI Commands

| Command           | Purpose                                             |
| ----------------- | --------------------------------------------------- |
| `run`             | End-to-end single-thread pipeline                   |
| `research reddit` | Cross-subreddit topic research and corpus synthesis |
| `fetch`           | Acquire raw thread data                             |
| `normalize`       | Map raw data to canonical model                     |
| `analyze`         | Deterministic evidence extraction                   |
| `infer`           | LLM-assisted synthesis                              |
| `report`          | Generate output reports                             |
| `corpus`          | Build and analyze cross-thread corpora              |
| `inspect`         | Examine persisted artifacts                         |
| `batch run`       | Process multiple threads                            |
| `preflight`       | Validate local environment                          |
| `serve`           | Local API server                                    |

Full command reference: [docs/usage.md](docs/usage.md)

## Artifact Storage

Every pipeline run produces inspectable artifacts under `.threadsense/`:

```text
.threadsense/
├── raw/<source>/          # Source payloads as fetched
├── normalized/<source>/   # Canonical thread model
├── analysis/<source>/     # Evidence-linked findings
├── reports/<source>/      # Rendered reports
├── corpora/<corpus-id>/   # Manifest, analysis, and report
└── batches/               # Batch run metadata
```

Details: [docs/artifacts.md](docs/artifacts.md)

## Local Runtime

ThreadSense runs without a local model for deterministic analysis. Summaries and synthesis require a local OpenAI-compatible endpoint (default: `http://127.0.0.1:8080/v1/chat/completions`).

Details: [docs/local-runtime-contract.md](docs/local-runtime-contract.md)

## Documentation

| Document                                                    | Content                             |
| ----------------------------------------------------------- | ----------------------------------- |
| [usage.md](docs/usage.md)                                   | Command reference                   |
| [research-reddit.md](docs/research-reddit.md)               | Reddit topic research workflow      |
| [output-modes.md](docs/output-modes.md)                     | JSON, human, and quiet output modes |
| [artifacts.md](docs/artifacts.md)                           | Artifact types and storage layout   |
| [overview.md](docs/overview.md)                             | Product and workflow overview       |
| [system-design.md](docs/system-design.md)                   | Architecture and system boundaries  |
| [local-runtime-contract.md](docs/local-runtime-contract.md) | Local inference contract            |
| [pitch.md](docs/pitch.md)                                   | Product positioning                 |

## Validation

```bash
uv run ruff check
uv run ruff format --check .
uv run mypy --strict src tests
uv run pytest
```

## Current Limits

- Topic research is implemented for Reddit; other source discovery workflows are planned
- Reddit research queries support `OR`/`|` clause unions only (intentionally narrow for deterministic alignment)
- Corpus reports are Markdown only
- GitHub Gist comments are flat (no threading); analysis uses temporal/semantic clustering
- GitHub Gist connector does not retry on transient API errors (502/503)
- The local API is a trusted local surface, not a hardened public service

## Direction

- Richer corpus presentation and operator workflows
- Discovery workflows beyond Reddit
- Evaluation and replay benchmarking
- Source-distribution and research-quality reporting
