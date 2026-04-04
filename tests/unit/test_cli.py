from __future__ import annotations

from pathlib import Path

import pytest

from threadsense import cli


def test_build_parser_parses_run_summary_and_contract_flags() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(
        [
            "run",
            "https://example.com/thread",
            "--format",
            "json",
            "--with-summary",
            "--summary-required",
            "--no-cache",
            "--auto-domain",
            "--domain",
            "financial_markets",
            "--objective",
            "competitive_intelligence",
            "--level",
            "strategic",
        ]
    )

    assert args.command == "run"
    assert args.target == ["https://example.com/thread"]
    assert args.format == "json"
    assert args.with_summary is True
    assert args.summary_required is True
    assert args.no_cache is True
    assert args.auto_domain is True
    assert args.domain == "financial_markets"
    assert args.objective == "competitive_intelligence"
    assert args.abstraction_level == "strategic"


def test_main_dispatches_fetch_reddit(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_reddit_fetch(
        config_path: Path | None,
        url: str,
        output_path: Path | None,
        expand_more: bool,
        flat: bool,
        no_cache: bool,
    ) -> int:
        captured.update(
            {
                "config_path": config_path,
                "url": url,
                "output_path": output_path,
                "expand_more": expand_more,
                "flat": flat,
                "no_cache": no_cache,
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
            "--no-cache",
        ]
    )

    assert result == 7
    assert captured == {
        "config_path": Path("threadsense.toml"),
        "url": "https://example.com/thread",
        "output_path": Path("artifact.json"),
        "expand_more": True,
        "flat": True,
        "no_cache": True,
    }


def test_build_parser_accepts_output_format_flag() -> None:
    parser = cli.build_parser()
    args = parser.parse_args(["--output-format", "quiet", "preflight"])
    assert args.output_mode == "quiet"
    assert args.command == "preflight"


def test_build_parser_output_format_default_is_none() -> None:
    parser = cli.build_parser()
    args = parser.parse_args(["preflight"])
    assert args.output_mode is None


def test_output_format_does_not_collide_with_subcommand_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--output-format sets output_mode; subcommand --output sets the artifact path."""
    captured: dict[str, object] = {}

    def fake_run_reddit_fetch(
        config_path: Path | None,
        url: str,
        output_path: Path | None,
        expand_more: bool,
        flat: bool,
        no_cache: bool,
    ) -> int:
        captured["output_path"] = output_path
        return 0

    monkeypatch.setattr(cli, "run_reddit_fetch", fake_run_reddit_fetch)

    from threadsense import cli_display

    monkeypatch.setattr(cli_display, "_output_mode", None)

    result = cli.main(
        [
            "--output-format",
            "json",
            "fetch",
            "reddit",
            "https://example.com/thread",
            "--output",
            "artifact.json",
        ]
    )
    assert result == 0
    assert captured["output_path"] == Path("artifact.json")
    assert cli_display.resolve_output_mode() == cli_display.OutputMode.JSON


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


def test_build_parser_parses_replay_command() -> None:
    parser = cli.build_parser()
    args = parser.parse_args(["replay", "--analysis-artifact", "analysis.json"])

    assert args.command == "replay"
    assert args.analysis_artifact == Path("analysis.json")


def test_build_parser_parses_corpus_create_command() -> None:
    parser = cli.build_parser()
    args = parser.parse_args(
        [
            "corpus",
            "create",
            "--name",
            "Demo Corpus",
            "--domain",
            "developer_tools",
            "--analysis-dir",
            "analysis",
        ]
    )

    assert args.command == "corpus"
    assert args.corpus_command == "create"
    assert args.analysis_dir == Path("analysis")


def test_build_parser_parses_evaluate_command() -> None:
    parser = cli.build_parser()
    args = parser.parse_args(
        [
            "evaluate",
            "--golden",
            "tests/golden/manifest.json",
            "--strategy",
            "keyword_heuristic",
            "keyword_heuristic",
        ]
    )

    assert args.command == "evaluate"
    assert args.golden == Path("tests/golden/manifest.json")
    assert args.strategy == ["keyword_heuristic", "keyword_heuristic"]


def test_build_parser_accepts_html_report_and_diff_command() -> None:
    parser = cli.build_parser()

    report_args = parser.parse_args(
        ["report", "analysis", "--input", "analysis.json", "--format", "html"]
    )
    diff_args = parser.parse_args(
        [
            "diff",
            "--analysis-path",
            "analysis.json",
            "--left-version",
            "1",
            "--right-version",
            "2",
        ]
    )

    assert report_args.format == "html"
    assert diff_args.command == "diff"
    assert diff_args.analysis_path == Path("analysis.json")
    assert diff_args.left_version == 1
    assert diff_args.right_version == 2


def test_build_parser_parses_corpus_search_command() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(["corpus", "search", "documentation"])

    assert args.command == "corpus"
    assert args.corpus_command == "search"
    assert args.query == "documentation"
