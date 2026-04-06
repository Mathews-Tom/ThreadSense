# Output Modes

ThreadSense supports three output modes across the CLI.

## Modes

### `json`

Machine-readable JSON payloads.

Use when:

- piping output to tools
- parsing results programmatically
- storing run metadata in automation

Example:

```bash
uv run threadsense --output-format json run reddit <url>
```

### `human`

Rich terminal output with tables and summaries.

Use when:

- working interactively in a terminal
- you want the run or research summary panel
- you want artifact paths and key results without reading JSON

Example:

```bash
uv run threadsense --output-format human research reddit \
  --query "second brain OR agentic PKM" \
  --subreddit ClaudeCode \
  --subreddit LocalLLaMA \
  --subreddit AI_Agents \
  --with-summary
```

### `quiet`

Status-only output.

Use when:

- you only need `ready` or `error`
- integrating with simple shell scripts

Example:

```bash
uv run threadsense --output-format quiet run reddit <url>
```

## Default Behavior

If `--output-format` is not provided:

- ThreadSense uses `human` when stdout is a TTY
- ThreadSense uses `json` when stdout is not a TTY

This matters because wrappers such as `uv run`, editors, shells, or redirected output may cause stdout to look non-interactive.

If you expect a Rich panel and instead see JSON, force the mode explicitly:

```bash
uv run threadsense --output-format human ...
```

## Human-Mode Summary Panels

### Single-thread `run`

`run` can render:

- stage table
- summary headline and paragraph
- priority / owner / action
- next steps
- top findings

### `research reddit`

`research reddit` can render:

- query / subreddit / time-window overview
- discovered / selected / fetched counts
- corpus report path
- summary headline
- key patterns
- recommended actions
- confidence note
- top matched threads

## Recommendation

Use:

- `json` for automation
- `human` for operator workflows
- `quiet` for status checks
