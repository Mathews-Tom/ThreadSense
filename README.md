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

### Saved watches

Save a research profile as a versioned watch. Profiles are TOML files validated before anything is written; saving identical content again changes nothing, and a real edit records a new revision.

```toml
profile_version = 1
id = "agent-memory"
name = "Agent memory tooling"
domain = "developer_tools"
objective = "friction_analysis"
abstraction_level = "operational"
queries = ["second brain", "agentic PKM"]
subreddits = ["ClaudeCode", "LocalLLaMA"]
refresh_interval_minutes = 1440

[limits]
discovered_candidates = 100
refreshed_threads = 25
analyzed_revisions = 25
presented_findings = 10
```

```bash
uv run threadsense watch create --profile PATH
uv run threadsense watch list
uv run threadsense watch inspect --id agent-memory
uv run threadsense watch run --id agent-memory
uv run threadsense watch digest --id agent-memory
uv run threadsense watch schedule --id agent-memory --format launchd
```

`watch run` performs one bounded sweep of the watch and then presents a digest of the new evidence; `watch digest` presents only. A digest is a JSON and Markdown artifact pair under the monitoring directory listing new evidence-linked thread updates and findings, at most `presented_findings` per digest, with the remaining backlog reported. `watch schedule` renders a launchd property list or a cron line and nothing else: it installs and enables nothing, and you install the output yourself if you want recurring runs.

Field bounds and output shapes: [docs/usage.md](docs/usage.md).

### Cross-thread issue history

Each watch thread with finding evidence gets a stable issue identified by its home thread. A thread joins another issue automatically only when every report first recorded for it is an exact copy of an earlier report in another thread, and an operator can link or unlink a thread explicitly. Every change is an appended event, so history is reversible and never rewritten. Counts report observed threads, accounts and communities and never verify independence; exact copies never count as corroboration. Each report keeps the lineage recorded at ingestion, and when a later ingestion discovers an earlier original of a report recorded as firsthand, an append-only correction event in the ingestion ledger (`lineage_corrections`, `lineage_events`) makes the report's effective lineage an exact copy. Firsthand and exact-copy counts follow effective lineage while observed counts stay unchanged; a correction never regroups threads, and suggestions stay unconfirmed until an operator links. Digests and issue views already recorded keep their original bytes; new digests capture effective lineage. Candidate issues are bounded, unconfirmed suggestions that never merge memberships or counts (semantic grouping is `not_eligible`), and update cues such as reproduction, contradiction, workaround and resolution claims are unverified lexical signals that never resolve an issue. `watch run` and `watch digest` record the evidence and add an `issues` section to digests.

```bash
uv run threadsense issues inspect --watch agent-memory [--issue ISSUE_ID]
uv run threadsense issues link --watch agent-memory --issue ISSUE_ID --thread THREAD_ID --reason TEXT
uv run threadsense issues unlink --watch agent-memory --issue ISSUE_ID --thread THREAD_ID --reason TEXT
```

Bounds, output shapes and refusal reasons: [docs/usage.md](docs/usage.md).

### Operator feedback

`feedback record` appends one operator event about a digest entry, a thread revision or an issue of a watch, and `feedback list` shows a watch's feedback history without writing state. Feedback comes in two separate kinds stored in separate append-only tables. A preference (`useful`, `irrelevant`, `already_known`, `wrong_audience`) is a subjective presentation signal, and the latest preference for a scope supersedes earlier ones without deleting them. A correction (`misclassified`, `unsupported`, `duplicate`, `missing_evidence`) is an auditable operator assertion recorded beside the unchanged original; it is not a truth label. Every event carries a watch-scoped idempotency key: repeating a key with identical feedback returns the original event, and reusing it for different feedback is refused. Targets are resolved before anything is written, so an unknown target or a target owned by another watch is refused. New digests present their entries in a ranked order with a reason per ranking component, and a preference changes only that presentation order: it never selects, hides or drops an entry. Corrections are not a ranking input, and they never change evidence, findings, counts, issue membership or written digests and reports.

```bash
uv run threadsense feedback record --watch agent-memory --target ENTRY_ID --kind preference --value useful --key KEY
uv run threadsense feedback record --watch agent-memory --target ENTRY_ID --kind correction --value unsupported --key KEY --note TEXT
uv run threadsense feedback list --watch agent-memory [--target TARGET_ID]
```

Target kinds, output shapes and refusal reasons: [docs/usage.md](docs/usage.md).

### Ranking and explanations

A digest selects the same entries it always has (the oldest unpresented ones) and then presents them in a ranked order. Each entry's score is the sum of bounded integer components from the watch's active ranking policy: finding severity, the profile objective's weight on the finding's existing action signal, the most novel kind of new evidence, a capped count of novel comments, and the operator's latest preferences per scope clamped to a bound. Ties fall back to the existing deterministic order and finally the entry id. No semantic component is scored: it is disclosed as `not_eligible` and contributes nothing. Ranking never hides, drops or rewrites an entry.

Ranking policies are immutable TOML documents identified by a content hash (`pol-` ids). Every watch starts on a built-in default policy. `watch policy` shows the active policy and its selection history, `--set` records a policy file and selects it, and `--select` switches back to any recorded policy or the default. Selecting appends an event and affects only digests recorded afterwards; written digests are never rewritten.

Each digest records the exact basis it was ranked from: the profile revision, the policy id and the latest preference sequence. `watch explain` replays that basis for one entry and refuses unless the replayed ranking equals the one in the written digest, so the historical order and its reasons are proven rather than restated. `--current` additionally shows how the same entries would rank under the current basis, derived on demand and never recorded.

```bash
uv run threadsense watch policy --id agent-memory
uv run threadsense watch policy --id agent-memory --set PATH
uv run threadsense watch policy --id agent-memory --select POLICY_ID
uv run threadsense watch explain --id agent-memory --entry ENTRY_ID [--current]
```

Policy format, allowed ranges, output shapes and refusal reasons: [docs/usage.md](docs/usage.md).

### Typed decision annotations (shadow)

`decisions annotate` asks a separate, operator-provisioned local Laya service six fixed typed questions about every comment of a canonical thread artifact and writes the answers as shadow artifacts. They are uncalibrated and never change baseline findings, reports or monitoring. The service is pinned, loopback-only and never installed or started by ThreadSense; comment text goes only to that local endpoint.

```bash
uv run threadsense decisions annotate \
  --input PATH \
  --endpoint http://127.0.0.1:<port>
```

The default `optional` requirement keeps unavailability visible and exits `0`; `--requirement required` exits `1` on any refused or unavailable comment. Preparing or reusing the service: [docs/laya-runtime.md](docs/laya-runtime.md). Artifact definitions: [docs/artifacts.md](docs/artifacts.md#typed-decision-annotations). Flags and exit codes: [docs/usage.md](docs/usage.md#decisions-annotate).

### Domain-aware analysis

The analysis layer uses a contract system with domain-specific vocabularies (developer tools, product feedback, hiring, research, financial markets, gaming). Each domain defines its own theme keywords, issue markers, and severity calibration — so analysis adapts to context rather than applying one-size-fits-all heuristics.

### Frozen decision assessment

`decisions evaluate` assesses saved annotations against a frozen dataset manifest and its labels. Typed replay is offline; the command never re-annotates or activates a policy. It compares typed and keyword themes; a fresh comparison with an available configured local generative model can also classify the same supported theme cohort: typed-eligible, supported-language comments of at most 300 characters. The other five typed dimensions have no existing generative counterpart. These boundary-case labels do not establish population quality.

A fresh comparison acquires an immutable raw request/response capture and reports its path and SHA-256. Supply `--generative-capture PATH` to replay that frozen comparison offline, with no model listing or inference. Another fresh acquisition is not a deterministic replay. An absent configured model is reported as unavailable, not as a loss; disabled or invalid-endpoint guards are `not_observed` because no request was made. See [evaluation usage](docs/usage.md#decisions-evaluate) for the command and [capture semantics](docs/artifacts.md#comparators) for its proof boundaries.

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
| `watch`           | Save, sweep, digest and schedule research profiles  |
| `watch explain`   | Replay and explain a digest entry's rank            |
| `watch policy`    | Show or select a watch's ranking policy             |
| `issues`          | Inspect and link cross-thread issue history         |
| `feedback record` | Record an operator preference or correction         |
| `feedback list`   | List a watch's preferences and corrections          |
| `decisions annotate` | Shadow typed annotations from a local Laya service |
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
├── batches/               # Batch run metadata
├── monitoring/            # Watch profiles and state (state.sqlite3)
└── annotations/           # Shadow typed decision annotations
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
| [laya-runtime.md](docs/laya-runtime.md)                     | Pinned local typed-decision service |
| [pitch.md](docs/pitch.md)                                   | Product positioning                 |

## Validation

```bash
uv sync --locked
bash scripts/validate.sh
```

`uv sync --locked` fails when `uv.lock` is stale instead of rewriting it. `scripts/validate.sh` runs, in order, `ruff check`, `ruff format --check`, `mypy --strict src tests scripts`, the complete `pytest` suite and a scan for absolute local paths in Markdown. GitHub Actions runs the same two commands on every pull request and on pushes to `main`, on Python 3.11 and 3.13, with `UV_LOCKED=1` so no step can modify the lock file.

The full `pytest` suite is deterministic recorded/loopback integration coverage, with no skips. It does not establish that a real source or local model is available, nor does it claim a successful live source fetch or model inference.

For a separate, non-gating operator smoke check, use the actual services:

```bash
uv run threadsense preflight --config PATH
uv run threadsense fetch reddit URL --output PATH --config PATH
uv run threadsense normalize reddit --input PATH --output PATH --config PATH
uv run threadsense analyze normalized --input PATH --output PATH --config PATH
uv run threadsense infer analysis --input PATH --task analysis_summary --required --config PATH
uv run threadsense report analysis --input PATH --with-summary --summary-required --config PATH
```

Replace `URL` with a real Reddit thread URL and each `PATH` with the relevant configuration, output, or input artifact path. The inference input is an analysis artifact; the report input is also an analysis artifact. The configuration must name an actually installed local model and point to its local endpoint; do not upload source content to a cloud service. Report external source or local-model failures separately from the deterministic suite results. A passing suite must never be interpreted as a live-service pass.

The incremental watch flow has its own smoke scenario. It drives the actual CLI in fresh child processes against synthesized recorded Reddit JSON served in-process, so it is not a live acquisition:

```bash
uv run python scripts/smoke_monitoring.py --scenario incremental --work-dir DIR
```

Replace `DIR` with a working directory for the scenario. The scenario prints `scenario=incremental status=passed evidence=...` only after every assertion passes and writes `incremental-evidence.json` into that directory.

The issue-history flow has its own scenario and integration test. The scenario drives the actual CLI against synthesized recorded Reddit JSON served in-process, with no network and no model, so it is not a live acquisition:

```bash
uv run pytest tests/integration/test_issue_history.py
uv run python scripts/smoke_monitoring.py --scenario issues --work-dir "${THREADSENSE_SMOKE_DIR:?}"
```

Set `THREADSENSE_SMOKE_DIR` to a working directory for the scenario. The scenario prints `scenario=issues status=passed evidence=...` only after every assertion passes and writes `issues-evidence.json` into that directory.

## Current Limits

- Topic research is implemented for Reddit; other source discovery workflows are planned
- Reddit research queries support `OR`/`|` clause unions only (intentionally narrow for deterministic alignment)
- Corpus reports are Markdown only
- GitHub Gist comments are flat (no threading); analysis uses temporal/semantic clustering
- GitHub Gist connector does not retry on transient API errors (502/503)
- Watches sweep and digest on demand through `watch run` and `watch digest`; nothing runs on a timer unless you install the rendered `watch schedule` output yourself, and digests are local files with no remote notification or delivery guarantee
- Typed decision annotations are shadow-only and uncalibrated, use an English checkpoint and need a separately provisioned loopback Laya service; fixtures are not independent labels
- The local API is a trusted local surface, not a hardened public service

## Direction

- Richer corpus presentation and operator workflows
- Discovery workflows beyond Reddit
- Evaluation and replay benchmarking
- Source-distribution and research-quality reporting
