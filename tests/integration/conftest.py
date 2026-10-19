from __future__ import annotations

import json
import re
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import Any

import pytest


@pytest.fixture
def loopback_runtime(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    """Recorded engineering prose over real HTTP, not a local-model acceptance test."""
    exchanges: list[tuple[dict[str, Any], dict[str, Any]]] = []
    errors: list[Exception] = []

    class CompletionHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            try:
                assert self.path == "/v1/chat/completions"
                request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                assert request["model"] == "threadsense-recorded-summary"
                assert request["stream"] is False
                assert request["response_format"] == {"type": "json_object"}
                messages = request["messages"]
                assert messages[-1]["role"] == "user"
                prompt = messages[-1]["content"]
                system = messages[0]["content"]
                summary: dict[str, Any]
                if system.startswith("You summarize evidence-backed thread analysis."):
                    evidence = json.loads(prompt.split("Input:\n", 1)[1])
                    findings = evidence["findings"]
                    finding = next(item for item in findings if item["evidence_comment_ids"])
                    summary = {
                        "headline": "Investigate the reported engineering friction",
                        "summary": (
                            "The cited discussion identifies a concrete engineering concern. "
                            "Reproduce the reported behavior before selecting a fix."
                        ),
                        "priority": "medium",
                        "confidence": 0.8,
                        "why_now": "The analysis contains directly attributable user evidence.",
                        "cited_theme_keys": [finding["theme_key"]],
                        "cited_comment_ids": [finding["evidence_comment_ids"][0]],
                        "next_steps": [
                            "Reproduce the cited behavior and capture a diagnostic trace.",
                        ],
                        "recommended_owner": "engineering",
                        "action_type": "investigate",
                        "expected_outcome": "A reproducible case guiding an evidence-backed fix.",
                    }
                    exchanges.append((evidence, summary))
                elif system.startswith("You analyze discussion threads and propose thematic"):
                    vocabulary = json.loads(
                        prompt.split("Existing vocabulary themes:\n", 1)[1].split("\n\n", 1)[0]
                    )
                    assert isinstance(vocabulary, dict)
                    comments = prompt.split("Top comments by score:\n", 1)[1].split("\n\n", 1)[0]
                    assert comments
                    # The recorded normal thread only says "top level comment"/"nested reply";
                    # there is no topical evidence warranting invented vocabulary.
                    bodies = re.findall(r"\[score=-?\d+\] (.*)", comments)
                    assert bodies
                    assert set(bodies) <= {"Top level comment", "Second top level comment"}
                    summary = {"existing_themes": {}, "new_themes": {}}
                elif system.startswith("You classify discussion comments into thematic"):
                    themes = prompt.split("Existing themes:\n", 1)[1].split("\n\n", 1)[0]
                    theme_lines = themes.splitlines()
                    assert all(re.fullmatch(r"  [a-z0-9_]+: .*", line) for line in theme_lines)
                    comments = prompt.split("Unclassified comments:\n", 1)[1].split("\n\n", 1)[0]
                    entries = re.findall(r"\[id=([^\]]+)\] ([^\n]*)", comments)
                    assert entries
                    assert all(
                        body in {"Top level comment", "Second top level comment", "Nested reply"}
                        for _, body in entries
                    )
                    # These recorded comments genuinely contain no thematic content.
                    summary = {
                        "classifications": [
                            {"comment_id": cid, "theme": "general_feedback", "confidence": 0.95}
                            for cid, _ in entries
                        ]
                    }
                else:
                    raise AssertionError(f"Unexpected inference protocol: {system}")
                body = json.dumps(
                    {
                        "id": "chatcmpl-recorded-summary",
                        "object": "chat.completion",
                        "created": 1710000100,
                        "model": request["model"],
                        "choices": [
                            {
                                "index": 0,
                                "message": {"role": "assistant", "content": json.dumps(summary)},
                                "finish_reason": "stop",
                            }
                        ],
                    }
                ).encode("utf-8")
                self.send_response(200)
            except Exception as error:
                errors.append(error)
                body = json.dumps({"error": str(error)}).encode("utf-8")
                self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), CompletionHandler)
    server_thread = Thread(target=server.serve_forever, daemon=True)
    monkeypatch.setenv("THREADSENSE_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("THREADSENSE_RUNTIME_BASE_URL", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setenv("THREADSENSE_RUNTIME_CHAT_PATH", "/v1/chat/completions")
    monkeypatch.setenv("THREADSENSE_RUNTIME_MODEL", "threadsense-recorded-summary")
    monkeypatch.setenv("THREADSENSE_RUNTIME_JSON_MODE", "true")
    monkeypatch.setenv("THREADSENSE_RUNTIME_TIMEOUT_SECONDS", "5")
    monkeypatch.setenv("THREADSENSE_RUNTIME_REPAIR_RETRIES", "0")
    monkeypatch.setenv("THREADSENSE_STORAGE_ROOT", str(tmp_path / "loopback-store"))
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    server_thread.start()
    try:
        yield
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)
        assert not server_thread.is_alive()
    assert not errors, errors
    assert exchanges, "The workflow must call the HTTP runtime, not a fallback."
    for evidence, summary in exchanges:
        findings = evidence["findings"]
        assert set(summary["cited_theme_keys"]) <= {item["theme_key"] for item in findings}
        assert set(summary["cited_comment_ids"]) <= {
            comment_id for item in findings for comment_id in item["evidence_comment_ids"]
        }
        assert summary["cited_theme_keys"]
        assert summary["cited_comment_ids"]
        cited_findings = [
            item for item in findings if item["theme_key"] in summary["cited_theme_keys"]
        ]
        assert set(summary["cited_comment_ids"]) <= {
            comment_id for item in cited_findings for comment_id in item["evidence_comment_ids"]
        }
