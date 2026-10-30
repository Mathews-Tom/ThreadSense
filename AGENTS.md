# AGENTS.md

## Code Review Rules

- **What failure means here:** In `src/threadsense/cli.py`, a documented flag, JSON output shape or required-versus-optional exit status stops matching the CLI contract.
- In `src/threadsense/connectors/`, `pipeline/` or `reporting/`, acquisition or normalization loses source/comment provenance, or a report presents a finding without its supporting evidence.
- In `src/threadsense/monitoring/`, retries lose or duplicate durable evidence, digest/alert baselines drift, or bounded continuation cannot resume without rewriting historical artifacts.
- In `src/threadsense/decisions/`, shadow annotations change deterministic findings, offline replay contacts a runtime, or local inference escapes its pinned loopback-only contract.
- **Do not report:** Style, formatting or naming preferences; diagnostics already enforced by Ruff lint/format, `mypy --strict`, or the Markdown local-path check in `scripts/validate.sh`; speculative hardening beyond the documented trusted-local runtime/API boundary.
- Review regressions introduced by this PR. Omit unchanged-code issues from fix requests; if essential context, label them `Pre-existing`. Do not repeat findings already raised on an earlier commit of the same PR; report only a new regression or materially different evidence.
- **Paths to skip:** `tests/fixtures/**` and `tests/golden/**` are recorded-input/golden data, not routine code-review targets. Consult changed fixtures only to establish a concrete contract or regression mismatch. `uv.lock` stays in scope for dependency changes: flag packages added or version-changed without a matching `pyproject.toml` change; do not review it line by line.
- **Evidence and format:** Every finding names `file:line`, the concrete failing input or execution scenario, its observable consequence, and the bug class so the author can fix every instance; a suspicious name alone is not evidence.
- Tag each finding `Important`, `Nit` or `Pre-existing`. Start the summary with the number of `Important` findings; when it is zero, say plainly that there is nothing to fix. Do not manufacture Nits to fill a review.
- **Recurring rule:** In `scripts/smoke_annotations.py` and `scripts/smoke_journey.py`, operational subprocess failures and missing-result failures must reach the existing `SmokeFailure` boundary, preserve the cause/partial stage evidence, write failed evidence and exit nonzero rather than escaping without a receipt.
