# Engineering evaluation cases

This directory is a frozen dataset for offline assessment of saved typed decision artifacts (`threadsense decisions evaluate`). `manifest.json` binds every thread file and `labels.json` by sha256, and pins the runtime, checkpoint, question set, settings and context policy identities the saved decisions must share.

## Provenance and disclosure

The thread texts and labels were authored by an AI coding agent as engineering boundary checks. Every expected answer was written from the case intent before any model run. The labels are engineering expectations: they are not independent human adjudication, not representative of any population and not ground truth. Metrics computed against them describe these cases only.

The threads use the synthetic source name `fixture`, invented authors and `example.invalid` permalinks. The eighth thread is the existing analysis fixture `../../analysis/canonical_feedback_thread.json`, referenced in place rather than copied. Its labels were written from the comment text and do not reuse the older golden dataset.

## Threads

| File | Partition | Probes |
| --- | --- | --- |
| `threads/negation.json` | development | Negated problems and requests ("not confusing at all") next to a real problem phrased with "not". |
| `threads/quoted_claims.json` | development | Quoted claims the author disputes or relays, and secondhand reports. |
| `threads/mixed_intents.json` | calibration | Problem and request in one comment, a complaint phrased as a question, promotional framing. |
| `threads/option_relabeling.json` | calibration | Comments that name another theme's keyword or an option label literally while meaning something else. |
| `threads/repetition.json` | evaluation | Exact, casing and whitespace copies of one comment, plus a rephrased repeat of the browser crash in `reddit:c5`. |
| `threads/long_context.json` | evaluation | Title over 300 and body over 600 characters, a parent over 400 characters with a reply, and comments of exactly 2000 and 2001 characters. |
| `threads/unsupported_language.json` | evaluation | Spanish, German and Japanese comments declared through `comment_languages`, plus one English control. |
| `../../analysis/canonical_feedback_thread.json` | evaluation | Baseline thread from the analysis fixtures. |

## Expected boundary outcomes

- `fixture:lc-3` is exactly 2000 characters and fits the client comment budget. This does not promise service success: the pinned checkpoint may report context truncation, causing refusal and leaving its labels `not_covered`, not errors.
- `fixture:lc-4` is exactly 2001 characters. The client must refuse it with `comment_exceeds_client_char_budget`, so its label stays uncovered and is never scored as an error.
- `fixture:ul-1`, `fixture:ul-2` and `fixture:ul-3` are declared non-English. They must be excluded as unsupported language, not scored.

The authored label rationales describe intended semantics, not a promise that the checkpoint will accept the input. `labels.status` records label presence; even when labels exist, the error rate remains `null` without labeled decisions. Count reconciliation checks structural invariants; corpus/annotation alignment is a separate check against the bound input corpus.

Generative theme comparison uses only typed-eligible, supported-language comments of at most 300 characters, with the same IDs and denominators for all paired methods. Longer comments and the other five dimensions are unsupported, not comparison losses. A fresh local-model acquisition retains an immutable raw capture; `--generative-capture PATH` replays that capture offline without model requests. Neither path activates a policy or turns these authored cases into population quality evidence.

## Repeated-issue groups

- `browser-comment-expansion-crash` spans `fixture:rep-5` and `reddit:c5`, two evaluation-partition threads.
- `extension-large-file-crash` groups `fixture:rep-1` to `fixture:rep-4` inside one thread.

Editing any bound file changes its sha256. Update `manifest.json` after any edit, or loading fails with `corpus_snapshot_mismatch` or `labels_digest_mismatch`.
