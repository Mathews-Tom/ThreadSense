# ThreadSense Pitch

## One-Line Positioning

ThreadSense turns noisy public discussion threads into evidence-backed product and research intelligence.

## The Problem

Teams do not lack raw discussion data. They lack a disciplined way to turn that data into decisions.

Typical workflows still look like this:

- manually search Reddit or community platforms
- open too many threads
- copy a few quotes into notes
- rely on generic summaries with weak evidence chains
- argue about whether the sample is representative

That workflow is slow, non-reproducible, and hard to defend.

## The Product

ThreadSense is a local-first discussion intelligence pipeline.

It already supports:

- source connectors for Reddit, Hacker News, and GitHub Discussions
- canonical normalization of thread data
- deterministic signal extraction
- optional local-model synthesis
- single-thread reports
- corpus analysis and reporting
- Reddit topic research across selected subreddits

The product is not “a scraper.” The product is a repeatable path from public thread to defensible insight.

## Why It Matters

Generic scrapers give you raw payloads.

Generic AI summaries give you prose.

ThreadSense is more useful because it keeps the evidence chain intact while compressing thread volume into structured outputs teams can actually act on.

That means:

- explicit source provenance
- stable schemas
- deterministic analysis layer
- optional synthesis on top of evidence
- persisted artifacts at every stage

## Example Outcome

Without ThreadSense:

- manually search a topic like `second brain` across Reddit
- read multiple threads separately
- try to merge takeaways by hand

With ThreadSense:

- run `research reddit` across selected subreddits
- deterministically select the best matching threads
- analyze each thread
- generate a corpus-level report
- review a concise terminal summary and a persisted markdown report

## Who It Is For

Primary users:

- product teams validating pain points and demand
- founders doing market and competitor research
- developer relations teams tracking workflow friction
- researchers studying technical communities

## Why This Can Win

The moat is not access to comments.

The moat is the quality of the pipeline:

- reliable acquisition
- canonical normalization
- evidence-backed analysis
- deterministic selection and ranking
- reproducible reports

Anyone can summarize a thread. Fewer systems can do it repeatably, across runs, with preserved evidence and explicit output contracts.

## Current Product Shape

ThreadSense now sits at the point where it can credibly be described as:

```text
single-thread analysis + corpus synthesis + subreddit topic research
```

That is a stronger and more useful position than a source-specific ingestion MVP.
