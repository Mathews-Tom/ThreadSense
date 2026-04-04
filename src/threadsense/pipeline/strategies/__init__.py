from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from threadsense.models.analysis import (
    AnalysisFinding,
    DuplicateGroup,
    RepresentativeQuote,
)
from threadsense.models.canonical import Thread


@dataclass(frozen=True)
class AnalysisResult:
    distinct_comment_count: int
    duplicate_group_count: int
    top_phrases: list[str]
    findings: list[AnalysisFinding]
    duplicate_groups: list[DuplicateGroup]
    top_quotes: list[RepresentativeQuote]


class AnalysisStrategy(Protocol):
    def analyze(self, thread: Thread) -> AnalysisResult: ...
