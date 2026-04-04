from __future__ import annotations

from time import time
from typing import TYPE_CHECKING

from threadsense.contracts.analysis_contract import (
    AbstractionLevel,
    AnalysisContract,
    DomainType,
    ObjectiveType,
)

if TYPE_CHECKING:
    from threadsense.config import AnalysisConfig


def default_contract(created_at_utc: float | None = None) -> AnalysisContract:
    return AnalysisContract(
        domain=DomainType.DEVELOPER_TOOLS,
        objective=ObjectiveType.GENERAL_SURVEY,
        abstraction_level=AbstractionLevel.OPERATIONAL,
        created_at_utc=created_at_utc or time(),
    )


def contract_from_config(
    config: AnalysisConfig,
    created_at_utc: float | None = None,
) -> AnalysisContract:
    return AnalysisContract(
        domain=config.domain,
        objective=config.objective,
        abstraction_level=config.abstraction_level,
        created_at_utc=created_at_utc or time(),
    )
