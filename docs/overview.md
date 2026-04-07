# ThreadSense Overview

ThreadSense is an evidence-first discussion intelligence system for turning public discussion threads into structured, inspectable intelligence.

## What Exists Today

The implemented system supports:

- source connectors for Reddit, Hacker News, GitHub Discussions, and GitHub Gists
- canonical normalization into a shared thread model
- deterministic analysis with evidence-linked findings
- optional local-model synthesis for thread and corpus summaries
- single-thread reporting in Markdown, HTML, and JSON
- corpus manifest creation, corpus analysis, and corpus reporting
- Reddit topic research across selected subreddits
- operator-facing CLI output in JSON, human, and quiet modes

This is no longer only a single-thread Reddit MVP. The current codebase supports both single-thread analysis and cross-thread Reddit research through the corpus pipeline.

## Core Workflows

### 1. Single-thread analysis

Input:

- one source URL or source-qualified target

Flow:

- fetch
- normalize
- analyze
- optionally synthesize summary
- generate report

Output:

- raw artifact
- normalized artifact
- analysis artifact
- report artifact / rendered report

### 2. Corpus analysis

Input:

- a set of analysis artifacts

Flow:

- build corpus manifest
- aggregate cross-thread findings
- detect temporal trends
- optionally synthesize corpus summary
- generate corpus report

Output:

- manifest
- corpus analysis artifact
- corpus markdown report

### 3. Reddit topic research

Input:

- topic query
- explicit subreddit list

Flow:

- search selected subreddits
- split `OR` queries into clause searches
- apply exact time-window filtering after retrieval
- rank and select thread candidates deterministically
- fetch and analyze selected threads
- build corpus manifest and corpus report

Output:

- selected thread list
- corpus manifest path
- corpus analysis path
- corpus report path
- terminal summary in human mode

## Source Model Strategy

ThreadSense separates source-specific acquisition from canonical normalization.

Current source support:

- Reddit — threaded comments via JSON API with "more" expansion
- Hacker News — recursive tree-based item fetching via Firebase API
- GitHub Discussions — GraphQL with cursor-based pagination for comments and replies
- GitHub Gists — REST API with paginated flat comments; authentication optional for public gists

Each connector preserves enough source metadata for traceability, while normalization maps the result into a canonical `Thread` object with:

- source metadata
- thread title
- top-level thread body when available
- author metadata
- normalized comments
- provenance metadata

This lets downstream analysis work against one stable model instead of branching on source-specific payloads.

## Analysis Strategy

ThreadSense uses a deterministic core with optional inference on top.

### Deterministic layer

- phrase extraction
- duplicate handling
- issue/request markers
- theme grouping
- severity heuristics
- deterministic action-signal classification

### Optional inference layer

- analysis summaries
- corpus synthesis
- schema repair / validation
- required vs degraded execution behavior

This keeps retrieval, normalization, and primary evidence extraction reproducible while using the runtime only where synthesis adds value.

## Output Strategy

ThreadSense produces both machine-readable and operator-facing outputs.

### Machine-readable

- JSON payloads from CLI commands
- persisted raw, normalized, analysis, report, and corpus artifacts

### Operator-facing

- Markdown reports
- HTML thread reports
- Rich terminal summary panels in human mode

Human mode currently includes:

- single-thread run summary panel
- Reddit research summary panel

## Design Principles

### Evidence First

Every claim should trace back to thread or comment evidence.

### Deterministic Core, Inference on Top

Parsing, normalization, scoring, and selection should stay deterministic.

### Fail Fast at Boundaries

Invalid URLs, malformed payloads, unsupported query syntax, and schema inconsistencies should fail explicitly.

### Stable Artifacts

Each stage persists a separate artifact so operators can inspect and rerun without redoing the entire pipeline.

### Operator-Friendly Output

JSON is for machines. Human mode is for operators. Both should expose the same underlying result with different presentation.

## Current Limits

- Reddit topic research is implemented; equivalent discovery workflows for other sources are not
- Reddit research query grammar is intentionally limited to simple clause unions
- corpus reports are currently Markdown only
- the local API does not yet expose the full Reddit research workflow
- legacy raw and canonical artifacts must be regenerated when required schema fields are missing

## What ThreadSense Is Now

ThreadSense is best described as:

```text
multi-source discussion intelligence + corpus synthesis + subreddit topic research
```

It is not just a scraper, and it is not just a summarizer. It is a reproducible pipeline from source thread to inspectable intelligence.
