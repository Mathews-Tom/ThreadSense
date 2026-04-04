from __future__ import annotations

from pathlib import Path

from threadsense.config import load_config
from threadsense.contracts import contract_from_config
from threadsense.models.canonical import load_canonical_thread
from threadsense.pipeline.alignment import check_domain_alignment
from threadsense.pipeline.analyze import analyze_thread


def test_alignment_warning_flags_misaligned_domain() -> None:
    thread = load_canonical_thread(Path("tests/fixtures/analysis/canonical_feedback_thread.json"))
    config = load_config(
        env={
            "THREADSENSE_ANALYSIS_DOMAIN": "financial_markets",
            "THREADSENSE_ANALYSIS_OBJECTIVE": "general_survey",
            "THREADSENSE_ANALYSIS_LEVEL": "operational",
        }
    )
    contract = contract_from_config(config.analysis)
    analysis = analyze_thread(
        thread,
        Path("tests/fixtures/analysis/canonical_feedback_thread.json"),
        config.analysis,
        contract,
    )

    alignment = check_domain_alignment(thread, analysis, contract)

    assert alignment.domain == "financial_markets"
    assert alignment.warning is not None
    assert alignment.suggested_domain == "developer_tools"
