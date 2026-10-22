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

Rich terminal output with command-specific panels or formatted JSON payloads.

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

- you only need the command's top-level status or an error message
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

### Watches, issues, feedback, and typed decisions

These command families use the shared payload renderer, not bespoke monitoring panels. `json` emits the full JSON payload; `human` displays it as Rich-formatted JSON. For example, `watch inspect` reports `ok` in quiet mode. Quiet mode prints the actual top-level `status`, not a universal `ready`: issue membership changes report `linked` or `unlinked`, a watch run reports its sweep outcome, and schedule rendering reports `rendered`. Errors print `error: <message>`.

Use JSON when inspecting nested digest or alert refusals, coverage, backlog, or recovery information: a quiet top-level status does not reproduce those details. Schedule JSON includes the rendered definition in `content` and explicitly reports `installed: false` and `enabled: false`; changing output mode does not install or enable a schedule. Alerts remain local artifacts with no notifications. Typed semantic decisions remain in shadow mode, with supported qualification `not_eligible`; output formatting does not activate a consumer.

See the [command and operator reference](usage.md) for command-specific fields and operational boundaries. Source: [shared output dispatch and quiet rendering](../src/threadsense/cli_display.py) and [command payloads](../src/threadsense/cli.py).

## Recommendation

Use:

- `json` for automation
- `human` for operator workflows
- `quiet` for status checks
