from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from time import time
from typing import Any

from threadsense.errors import AnalysisBoundaryError

ANALYSIS_CONTRACT_SCHEMA_VERSION = "1.0"


class DomainType(StrEnum):
    DEVELOPER_TOOLS = "developer_tools"
    PRODUCT_FEEDBACK = "product_feedback"
    HIRING_CAREERS = "hiring_careers"
    RESEARCH_ACADEMIC = "research_academic"
    FINANCIAL_MARKETS = "financial_markets"
    GAMING = "gaming"
    CUSTOM = "custom"


class ObjectiveType(StrEnum):
    FRICTION_ANALYSIS = "friction_analysis"
    FEATURE_DEMAND = "feature_demand"
    SENTIMENT_MAPPING = "sentiment_mapping"
    COMPETITIVE_INTELLIGENCE = "competitive_intelligence"
    GENERAL_SURVEY = "general_survey"


class AbstractionLevel(StrEnum):
    OPERATIONAL = "operational"
    ARCHITECTURAL = "architectural"
    STRATEGIC = "strategic"


@dataclass(frozen=True)
class AnalysisContract:
    domain: DomainType
    objective: ObjectiveType
    abstraction_level: AbstractionLevel
    schema_version: str = ANALYSIS_CONTRACT_SCHEMA_VERSION
    created_at_utc: float = 0.0

    def __post_init__(self) -> None:
        if not self.schema_version:
            raise AnalysisBoundaryError("analysis contract schema_version must not be empty")
        if self.created_at_utc <= 0:
            object.__setattr__(self, "created_at_utc", time())

    def to_dict(self) -> dict[str, str | float]:
        return {
            "domain": self.domain.value,
            "objective": self.objective.value,
            "abstraction_level": self.abstraction_level.value,
            "schema_version": self.schema_version,
            "created_at_utc": self.created_at_utc,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> AnalysisContract:
        try:
            created_at = payload.get("created_at_utc", time())
            if isinstance(created_at, int):
                created_at = float(created_at)
            if not isinstance(created_at, float):
                raise TypeError("created_at_utc")
            return cls(
                domain=DomainType(str(payload["domain"])),
                objective=ObjectiveType(str(payload["objective"])),
                abstraction_level=AbstractionLevel(str(payload["abstraction_level"])),
                schema_version=str(payload.get("schema_version", ANALYSIS_CONTRACT_SCHEMA_VERSION)),
                created_at_utc=created_at,
            )
        except KeyError as error:
            raise AnalysisBoundaryError(
                "analysis contract field is missing",
                details={"field": str(error)},
            ) from error
        except (TypeError, ValueError) as error:
            raise AnalysisBoundaryError(
                "analysis contract field is invalid",
                details={"payload": dict(payload)},
            ) from error
