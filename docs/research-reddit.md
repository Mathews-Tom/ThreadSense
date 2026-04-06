# Reddit Topic Research

`research reddit` discovers relevant Reddit threads inside explicit subreddits, analyzes the selected thread set, and generates a corpus-level report.

## Example

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

## Workflow

The workflow is:

1. search each selected subreddit
2. split `OR` queries into separate Reddit searches
3. apply exact post-filtering for the requested time window
4. rank and select matches deterministically
5. fetch each selected thread
6. normalize and analyze each selected thread
7. create a corpus manifest from the resulting analysis artifacts
8. analyze the corpus
9. write a corpus markdown report
10. optionally generate a corpus synthesis summary

## Query Syntax

Supported:

- `OR`
- `|`

Examples:

```text
second brain OR agentic PKM
workflow | memory layer
```

Unsupported:

- quotes
- parentheses
- `title:`
- `selftext:`
- negation-style advanced Reddit syntax

These are rejected explicitly so ThreadSense does not silently disagree with Reddit search semantics.

## Time Window Behavior

Default:

- `30d`

Examples:

- `7d`
- `30d`
- `90d`
- `365d`
- `all`

ThreadSense uses two layers:

1. a coarse Reddit search bucket such as `week`, `month`, or `year`
2. exact filtering on `created_utc` after retrieval

This means a request like `30d` is not only mapped to Reddit `month`; it is also filtered precisely afterward.

## Ranking And Selection

Selection is deterministic.

Signal strength comes from:

- title phrase hits
- title term hits
- selftext phrase hits
- selftext term hits
- score
- comment count
- recency

Controls:

- `--limit`: final selected thread count
- `--per-subreddit-limit`: per-community cap before global selection

`selected_threads` in the result payload shows the chosen set and the local match source for each thread.

## Result Payload

`research reddit` returns:

- `query`
- `subreddits`
- `time_window`
- `reddit_time_bucket`
- `discovered_thread_count`
- `selected_thread_count`
- `fetched_thread_count`
- `failed_thread_count`
- `selected_threads`
- `manifest_path`
- `corpus_analysis_path`
- `corpus_report_path`
- `corpus_id`
- `summary_provider`
- `degraded_summary`
- `terminal_summary` when available

## Human-Mode Output

Force human mode to see the Rich research summary panel:

```bash
uv run threadsense --output-format human research reddit ...
```

The panel includes:

- query and subreddit overview
- discovered / selected / fetched counts
- corpus report path
- summary headline
- key patterns
- recommended actions
- confidence note
- top matched threads

## Common Failure Cases

### No matching threads

You may see:

```text
reddit research did not find any matching threads
```

Try:

- increasing the time window, for example `--time-window 365d`
- simplifying the query to one clause
- increasing `--per-subreddit-limit`

### Inconsistent domain across selected analyses

Research corpus creation requires a stable domain unless the user provided one explicitly.

If selected threads drift across different analysis domains, provide a contract/domain override.

### Degraded summary

If `--with-summary` is used without `--summary-required`, corpus synthesis can degrade when the local runtime is unavailable or invalid.

## Practical Tips

- start with `--limit 5 --per-subreddit-limit 3` for a faster run
- use `--with-summary` when you want a decision-oriented corpus synthesis
- use `--output-format human` when you want the terminal summary panel
