# Pinned local Laya runtime for typed decisions

This document is the operator procedure to prepare or reuse the separately provisioned Laya service that `threadsense decisions annotate` talks to. ThreadSense never installs, downloads or starts this service. It is distinct from the generative runtime in [local-runtime-contract.md](local-runtime-contract.md): that runtime serves chat completions for `infer`, this one serves typed batch answers for `decisions annotate`, and neither depends on the other.

Command reference: [usage.md](usage.md#decisions-annotate). Artifact definitions: [artifacts.md](artifacts.md#typed-decision-annotations).

## Pinned identity

The identity below is declared by the packaged manifest `src/threadsense/decisions/laya_runtime_manifest.json`.

| Component | Pin |
| --- | --- |
| Code | `https://github.com/NandhaKishorM/laya` at commit `ffc142012af63d015fa19f05f29f9c5ff329a2cc`, package `laya` 0.4.12, Apache-2.0 |
| Checkpoint | Hugging Face `convaiinnovations/laya` at revision `7b928d828b7b0e022f929d9bd2e44165aa270148`, Apache-2.0 |

Checkpoint files (six, about 846 MB in total):

| Path | Bytes | SHA-256 |
| --- | ---: | --- |
| `config.json` | 160 | `7add7c041b53f7091e65908553af79ab3f077946e5f2312d510ae13b17c7c134` |
| `encoder/config.json` | 2083 | `bf3ab80598fdccf414855a2ce80f22859e4492d06ca8a62ddd1cfb63972f8979` |
| `rl_agent_config.json` | 745 | `ae287b56bbcf5f8c4f4541ae9dfd00c914c4c48b940b8398c3058af37ba92bbd` |
| `tokenizer/tokenizer.json` | 3583228 | `6c8aaa9a542084f2457eab775d4eeb51f92a70c0fd9de28d5edb0ddec3c08d30` |
| `tokenizer/tokenizer_config.json` | 308 | `50044de60daaa73df97d262e15a40d4faf0160e7d742df64b377877a1320dd12` |
| `model.safetensors` | 842609210 | `891102d372688fc2a094dac56a384bc537b87c63f21f9f3dac0be2b7cbc8d86c` |

How identity is established:

- The code identity is declared by the manifest and attested by this procedure. The service does not expose it over HTTP, so ThreadSense cannot observe it.
- The checkpoint identity is verified by ThreadSense on every invocation. It hashes the six files in the directory the service reports in `routing.repo`, once per invocation, and compares them with the manifest.
- The `/health` revision stays `null` for a local directory and is never filled in.

## Budget and isolation

- At most 12 GiB of additional disk, including the environment and the weights.
- At least 20 GiB free before installing.
- A private runtime root, `<runtime-root>`, holding an isolated Python 3.11 virtual environment, the source checkout, the checkpoint, a private Hugging Face cache and a private `TMPDIR`.
- No global installs and no credentials. Strip Hugging Face tokens from the service environment.
- Offline at serve time.

## Prepare

Every step is bounded and verified before use.

1. Check licenses and free space. Confirm the code and checkpoint licenses in the table above and confirm at least 20 GiB free on the volume that holds `<runtime-root>`.
2. Create `<runtime-root>` and an isolated Python 3.11 virtual environment inside it.
3. Fetch the exact source commit into `<runtime-root>/<source>` and verify it:

   ```bash
   git init <source>
   git -C <source> fetch --depth 1 https://github.com/NandhaKishorM/laya ffc142012af63d015fa19f05f29f9c5ff329a2cc
   git -C <source> checkout FETCH_HEAD
   git -C <source> rev-parse HEAD
   ```

   The last command must print the pinned commit.
4. Install the runtime dependencies into the private environment from a hash-locked requirements file, binary wheels only:

   ```bash
   <runtime-root>/<venv>/bin/python -m pip install --require-hashes --only-binary :all: -r <hashed-requirements-file>
   ```

   Key pins: torch 2.14.1, transformers 5.20.2, tokenizers 0.23.3, safetensors 0.8.0, fastapi 0.149.0, uvicorn 0.54.0, numpy 2.4.6, huggingface-hub 2.2.0. The wheels are platform-specific, so regenerate the hashes for your platform.
5. Download only the six checkpoint files above, at revision `7b928d828b7b0e022f929d9bd2e44165aa270148`, into `<checkpoint-dir>`, using the private Hugging Face cache. Do not download anything else from the repository.
6. Verify each file's size and SHA-256 against the table before serving.
7. Re-check the disk budget: the root, including environment and weights, must stay within 12 GiB.

## Reuse

An already prepared `<runtime-root>` is reused as is. Re-verify the six size and SHA-256 values against the table; nothing is re-downloaded.

## Serve

Run the service with `PYTHONPATH=<source>` and this environment:

| Variable | Value |
| --- | --- |
| `LAYA_HOST` | `127.0.0.1` (loopback only, never `0.0.0.0`) |
| `LAYA_PORT` | the port you pass in `--endpoint` |
| `LAYA_DEVICE` | `cpu` |
| `LAYA_THREADS` | `2` |
| `LAYA_PRELOAD` | `1` |
| `LAYA_MODELS` | `english` |
| `LAYA_DEFAULT_MODEL` | `english` |
| `LAYA_EXTRA_MODELS` | `{"english": "<checkpoint-dir>"}` |
| `LAYA_SHA256_DIGESTS` | the six digests from the table |
| `LAYA_AUTO_TASK` | `0` |
| `LAYA_MAX_LOADED` | `1` |
| `LAYA_MAX_CONCURRENT` | `1` |
| `LAYA_MAX_TOKEN_BUDGET` | `512` |
| `LAYA_JEV_STRICT` | `0` |
| `HF_HUB_OFFLINE` | `1` |
| `TRANSFORMERS_OFFLINE` | `1` |
| `HF_HUB_DISABLE_TELEMETRY` | `1` |
| `DO_NOT_TRACK` | `1` |
| `HF_HOME` | a directory inside `<runtime-root>` |
| `TMPDIR` | a directory inside `<runtime-root>` |

No `LAYA_API_KEY` is needed on loopback. Start it with `<runtime-root>/<venv>/bin/python -m laya.serve`.

Check health:

```bash
curl --noproxy '*' http://127.0.0.1:<port>/health
```

The response must show the `english` model loaded on `cpu`.

## What ThreadSense sends

ThreadSense sends the batch requests documented in [artifacts.md](artifacts.md#context-and-request-shape): model `english`, language `en`, `max_len` 512 and `head_max_len` 256, with no `sort_by_length`, `batch_size` or `min_confidence`. Discussion content goes only to this loopback service, and nothing is uploaded elsewhere.

## Rollback

Stop the service you own and stop invoking `decisions annotate`, or run it in the default `optional` mode. Baseline analysis, reports and monitoring are unaffected because annotations are separate shadow artifacts. The core ThreadSense install never requires torch or transformers.

## Limits

- Annotations are shadow only and uncalibrated.
- The checkpoint is English, and ThreadSense declares the language rather than detecting it.
- No claim of annotation quality is made here.
- Fixtures written by agents are not independent labels.
