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
- No global installs and no credentials in the service environment. Inherit only `PATH`, `LANG` and `LC_ALL`, then add the explicit settings below; never forward agent or provider tokens.
- Offline at serve time.

## Prepare

The commands below are operator instructions, not actions performed by ThreadSense. The exercised dependency tuple is **macOS arm64 / CPython 3.11**, on Darwin 25.6.0; this is not evidence that every macOS release runs the pinned wheels. Other platforms require a separate wheel and runtime assessment. Use an existing `uv` executable and Python 3.11; do not install into the core ThreadSense environment.

1. Confirm both Apache-2.0 licenses above. From the ThreadSense repository root, choose a new private runtime directory and an unused loopback port:

   ```bash
   export THREADSENSE_ROOT="$PWD"
   export RUNTIME_ROOT="$HOME/.local/share/threadsense-laya"
   export LAYA_PORT=8765
   export UV_CACHE_DIR="$RUNTIME_ROOT/uv-cache"
   export UV_PYTHON_DOWNLOADS=never
   export HF_HOME="$RUNTIME_ROOT/hf-cache"
   export TMPDIR="$RUNTIME_ROOT/tmp"
   unset HF_TOKEN HF_HUB_TOKEN HUGGING_FACE_HUB_TOKEN
   umask 077
   mkdir -p "$RUNTIME_ROOT" "$UV_CACHE_DIR" "$HF_HOME" "$TMPDIR"
   df -k "$RUNTIME_ROOT"
   ```

   Stop unless at least 20 GiB is free on that volume. `THREADSENSE_ROOT` is the ThreadSense source tree; `RUNTIME_ROOT/source` below is the separate pinned **Laya** source tree. All environment, cache, checkpoint and temporary paths belong to `RUNTIME_ROOT`, not either source tree. Include any Python provisioning in the disk projection; these instructions assume Python 3.11 already exists.
2. Create the private interpreter and fetch the exact source, without installing the Laya package:

   ```bash
   uv venv --python 3.11 "$RUNTIME_ROOT/env"
   git init "$RUNTIME_ROOT/source"
   git -C "$RUNTIME_ROOT/source" fetch --depth 1 https://github.com/NandhaKishorM/laya ffc142012af63d015fa19f05f29f9c5ff329a2cc
   git -C "$RUNTIME_ROOT/source" checkout --detach FETCH_HEAD
   git -C "$RUNTIME_ROOT/source" rev-parse HEAD
   ```

   The last command must print the pinned commit. Preserve that unmodified tree: serving imports it with `PYTHONPATH`, not an independently installed `laya` distribution.
3. Derive the requirements input directly from these exact public package pins (the 43-package exercised environment, not an upstream minimum-dependency list):

   ```bash
   cat > "$RUNTIME_ROOT/requirements.in" <<'EOF'
   annotated-doc==0.0.5
   annotated-types==0.8.0
   anyio==4.15.1
   click==8.5.0
   fastapi==0.149.0
   filelock==4.2.4
   fsspec==2026.9.0
   h11==0.16.0
   hf-xet==1.7.0
   httpcore2==2.14.0
   httpx2==2.14.0
   huggingface-hub==2.2.0
   idna==3.20
   jinja2==3.1.6
   markdown-it-py==4.2.0
   markupsafe==3.0.4
   mdurl==0.1.2
   mpmath==1.3.0
   networkx==3.6.1
   numpy==2.4.6
   opentelemetry-api==1.45.1
   packaging==26.3
   pydantic==2.14.1
   pydantic-core==2.50.1
   pygments==2.21.0
   python-multipart==0.0.32
   pyyaml==6.0.3
   regex==2026.9.29
   rich==15.0.0
   safetensors==0.8.0
   setuptools==84.0.0
   shellingham==1.5.4
   starlette==1.9.0
   sympy==1.14.0
   tokenizers==0.23.3
   torch==2.14.1
   tqdm==4.70.1
   transformers==5.20.2
   truststore==0.10.5
   typer==0.27.3
   typing-extensions==4.16.0
   typing-inspection==0.4.4
   uvicorn==0.54.0
   EOF
   uv pip compile "$RUNTIME_ROOT/requirements.in" \
     --python "$RUNTIME_ROOT/env/bin/python" --only-binary :all: \
     --default-index https://pypi.org/simple --generate-hashes \
     --output-file "$RUNTIME_ROOT/requirements-arm64-py311-hashed.txt"
   ```

   Run on the stated native tuple; do not reinterpret this as cross-platform validation. Resolution failure, unavailable wheels, changed pins or extra resolved packages require stopping and reassessing, not relaxing pins or allowing source builds. The generated file is the named hash-locked installation input; no internal requirements artifact is needed. Its hashes originate from public package-index metadata.
4. **Before installation or weight download**, project peak additional disk from the exact selected wheels. Public primary metadata is available at `https://pypi.org/pypi/<package>/<version>/json`: for each compatible wheel record its filename, SHA-256 and `size`; verify its digest is allowed by the generated lock. Obtain expanded sizes from that wheel's ZIP central directory (HTTP byte ranges can inspect it without staging the entire archive). Sum the ZIP members' uncompressed sizes, not the compressed download sizes. Never use source-archive sizes or a wheel for another interpreter/architecture as the estimate.

   Let `B` be current runtime-root allocated bytes, `C` the sum of compressed wheel bytes, `E` the sum of expanded wheel bytes, and `W = 846195734` the six checkpoint bytes. A conservative projection is `B + 2*C + 2*E + 2*W + 1 GiB`: allow download/cache copies, extraction/install overlap, checkpoint cache plus local files, and filesystem/metadata overhead. Include any other planned copies explicitly. Compare allocated disk, not merely logical file lengths; if block rounding, sparse files or cache placement make this bound unreliable, stop for a larger justified projection. Require both a projection at most 12 GiB and at least 20 GiB currently free. Unknown or incomplete wheel inventories, unavailable ZIP sizes, or an unbounded cache/temp location are **NO-GO**, never zero-byte estimates. Re-check during preparation and stop before exceeding the cap. Dependency resolution itself uses the private bounded cache; do not treat it as permission to stage unbounded archives.
5. Only after that GO decision, install binary wheels with `uv`:

   ```bash
   uv pip install --python "$RUNTIME_ROOT/env/bin/python" \
     --default-index https://pypi.org/simple --require-hashes --only-binary :all: \
     -r "$RUNTIME_ROOT/requirements-arm64-py311-hashed.txt"
   ```

6. Download only the six checkpoint files above at the pinned revision. This uses the public manifest in the ThreadSense source tree, never a private configuration file:

   ```bash
   "$RUNTIME_ROOT/env/bin/python" - <<'PY'
   import hashlib
   import json
   import os
   from pathlib import Path
   from huggingface_hub import hf_hub_download

   root = Path(os.environ["RUNTIME_ROOT"])
   manifest = json.loads((Path(os.environ["THREADSENSE_ROOT"]) /
       "src/threadsense/decisions/laya_runtime_manifest.json").read_text())
   checkpoint = manifest["checkpoint"]
   for entry in checkpoint["files"]:
       path = Path(hf_hub_download(
           repo_id=checkpoint["repository"], revision=checkpoint["revision"],
           filename=entry["path"], local_dir=root / "checkpoint", token=False))
       digest = hashlib.sha256()
       with path.open("rb") as stream:
           for block in iter(lambda: stream.read(1024 * 1024), b""):
               digest.update(block)
       if path.stat().st_size != entry["bytes"] or digest.hexdigest() != entry["sha256"]:
           raise SystemExit(f"checkpoint identity mismatch: {entry['path']}")
   PY
   ```

7. Re-check free space and total allocated runtime-root disk, including hidden caches and temporary files. It must remain within 12 GiB; the projection is not a substitute for this check.

## Reuse

An already prepared `RUNTIME_ROOT` is reused as is: do not recreate its environment or re-download files. Restore the `THREADSENSE_ROOT`, `RUNTIME_ROOT`, cache, temporary-directory and port exports from preparation in the new shell. Confirm the unmodified Laya source still has the pinned commit, re-check the disk cap, and re-verify the six size and SHA-256 values against the table (the launch below performs this file check). Historical resolver projections are not runtime outcomes; reuse requires the prepared exact environment, not just a matching candidate requirements input.

## Serve

Use the following settings from the pinned source's `laya.serve` environment interface:

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
| `LAYA_SHA256_DIGESTS` | JSON `{"english": {"config.json": "<sha256>", "encoder/config.json": "<sha256>", ...}}`, containing all six table entries |
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

Digest keys are paths **relative to the checkpoint directory**, not to the Laya source tree or the runtime root. `LAYA_EXTRA_MODELS` instead maps `english` to the absolute local checkpoint directory. No `LAYA_API_KEY` is needed on loopback. The following launch derives both JSON values from the packaged public manifest, verifies the local files again, constructs a small service environment from `PATH` and locale variables plus the explicit pinned settings, and executes the private interpreter. It does not activate or depend on the core environment:

```bash
"$RUNTIME_ROOT/env/bin/python" - <<'PY'
import hashlib
import json
import os
from pathlib import Path

root = Path(os.environ["RUNTIME_ROOT"]).resolve()
manifest = json.loads((Path(os.environ["THREADSENSE_ROOT"]) /
    "src/threadsense/decisions/laya_runtime_manifest.json").read_text())
digests = {}
for entry in manifest["checkpoint"]["files"]:
    path = root / "checkpoint" / entry["path"]
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    if path.stat().st_size != entry["bytes"] or digest.hexdigest() != entry["sha256"]:
        raise SystemExit(f"checkpoint identity mismatch: {entry['path']}")
    digests[entry["path"]] = entry["sha256"]
port = os.environ["LAYA_PORT"]
environment = {key: os.environ[key] for key in ("PATH", "LANG", "LC_ALL")
    if key in os.environ}
environment.update({
    "PYTHONPATH": str(root / "source"),
    "PYTHONDONTWRITEBYTECODE": "1",
    "LAYA_HOST": "127.0.0.1", "LAYA_PORT": port,
    "LAYA_DEVICE": "cpu", "LAYA_THREADS": "2", "LAYA_PRELOAD": "1",
    "LAYA_MODELS": "english", "LAYA_DEFAULT_MODEL": "english",
    "LAYA_EXTRA_MODELS": json.dumps({"english": str(root / "checkpoint")}),
    "LAYA_SHA256_DIGESTS": json.dumps({"english": digests}),
    "LAYA_AUTO_TASK": "0", "LAYA_MAX_LOADED": "1",
    "LAYA_MAX_CONCURRENT": "1", "LAYA_MAX_TOKEN_BUDGET": "512",
    "LAYA_JEV_STRICT": "0", "HF_HUB_OFFLINE": "1",
    "TRANSFORMERS_OFFLINE": "1", "HF_HUB_DISABLE_TELEMETRY": "1",
    "DO_NOT_TRACK": "1", "HF_HOME": str(root / "hf-cache"),
    "TMPDIR": str(root / "tmp"), "USE_TF": "0",
    "TOKENIZERS_PARALLELISM": "false",
})
python = str(root / "env/bin/python")
os.chdir(root)
os.execve(python, [python, "-m", "laya.serve"], environment)
PY
```

Keep that process in its own terminal. The literal service entry point is `-m laya.serve`; `PYTHONPATH` selects the exact pinned Git tree. Offline flags prevent Hub fetches at serve time, and binding to `127.0.0.1` prevents remote service exposure.

Check health:

```bash
curl --noproxy '*' "http://127.0.0.1:${LAYA_PORT}/health"
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
