from __future__ import annotations

from pathlib import Path

import pytest

from threadsense import cli


def test_build_parser_parses_run_reddit_summary_flags() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(
        [
            "run",
            "reddit",
            "https://example.com/thread",
            "--format",
            "json",
            "--with-summary",
            "--summary-required",
        ]
    )

    assert args.command == "run"
    assert args.source == "reddit"
    assert args.url == "https://example.com/thread"
    assert args.format == "json"
    assert args.with_summary is True
    assert args.summary_required is True


def test_main_dispatches_fetch_reddit(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_reddit_fetch(
        config_path: Path | None,
        url: str,
        output_path: Path | None,
        expand_more: bool,
        flat: bool,
    ) -> int:
        captured.update(
            {
                "config_path": config_path,
                "url": url,
                "output_path": output_path,
                "expand_more": expand_more,
                "flat": flat,
            }
        )
        return 7

    monkeypatch.setattr(cli, "run_reddit_fetch", fake_run_reddit_fetch)

    result = cli.main(
        [
            "fetch",
            "reddit",
            "https://example.com/thread",
            "--config",
            "threadsense.toml",
            "--output",
            "artifact.json",
            "--expand-more",
            "--flat",
        ]
    )

    assert result == 7
    assert captured == {
        "config_path": Path("threadsense.toml"),
        "url": "https://example.com/thread",
        "output_path": Path("artifact.json"),
        "expand_more": True,
        "flat": True,
    }


def test_main_dispatches_batch_run(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_batch(
        config_path: Path | None,
        manifest_path: Path,
        output_path: Path | None,
    ) -> int:
        captured.update(
            {
                "config_path": config_path,
                "manifest_path": manifest_path,
                "output_path": output_path,
            }
        )
        return 11

    monkeypatch.setattr(cli, "run_batch", fake_run_batch)

    result = cli.main(
        [
            "batch",
            "run",
            "--config",
            "threadsense.toml",
            "--manifest",
            "manifest.json",
            "--output",
            "batch.json",
        ]
    )

    assert result == 11
    assert captured == {
        "config_path": Path("threadsense.toml"),
        "manifest_path": Path("manifest.json"),
        "output_path": Path("batch.json"),
    }
