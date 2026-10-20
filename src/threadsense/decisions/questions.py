"""Typed question set for the six comment-level semantic dimensions.

The dimensions overlap by design and none of them is a source-truth or factual-accuracy
score. Questions stay short because each one is rendered into a bounded head.
"""

from __future__ import annotations

from threadsense.contracts.analysis_contract import ObjectiveType
from threadsense.decisions import contracts
from threadsense.decisions.contracts import (
    ChoiceQuestion,
    NoulQuestion,
    ObjectiveIdentity,
    QuestionSet,
    ScoreQuestion,
    VocabularyIdentity,
)
from threadsense.domains.vocabulary import DomainVocabulary
from threadsense.errors import DecisionInputError

ATTRIBUTION = (
    "Judge only the text in `comment`; `thread_title`, `thread_body` and `parent_comment`, "
    "when present, are context written by others."
)

# Built over every member explicitly so a new ObjectiveType cannot go unmapped silently.
OBJECTIVE_TEXTS: dict[ObjectiveType, str] = {
    ObjectiveType.FRICTION_ANALYSIS: "find friction, problems and pain points that users report",
    ObjectiveType.FEATURE_DEMAND: "find features and changes that users ask for",
    ObjectiveType.SENTIMENT_MAPPING: "map how users feel about the product or topic",
    ObjectiveType.COMPETITIVE_INTELLIGENCE: (
        "find comparisons with alternative products or competitors"
    ),
    ObjectiveType.GENERAL_SURVEY: "survey the main topics that users discuss",
}

_THEME_KEYWORD_SAMPLE = 5


def _instructions(sentence: str) -> str:
    return f"{sentence} {ATTRIBUTION}"


def _objective_text(objective: ObjectiveType) -> str:
    try:
        return OBJECTIVE_TEXTS[objective]
    except KeyError:
        raise DecisionInputError(
            f"objective {objective!r} has no question text",
            {"reason": "objective_unsupported", "objective": str(objective)},
        ) from None


def vocabulary_identity(vocabulary: DomainVocabulary) -> VocabularyIdentity:
    themes = tuple(vocabulary.theme_rules)
    if not themes:
        raise DecisionInputError(
            "vocabulary defines no themes",
            {"reason": "theme_vocabulary_invalid", "domain": vocabulary.domain},
        )
    if contracts.OTHER_OR_UNCERTAIN in themes:
        raise DecisionInputError(
            f"vocabulary theme collides with reserved option {contracts.OTHER_OR_UNCERTAIN}",
            {
                "reason": "theme_vocabulary_invalid",
                "domain": vocabulary.domain,
                "theme": contracts.OTHER_OR_UNCERTAIN,
            },
        )
    # Ordered lists, never sorted: theme order is part of the question identity.
    digest = contracts.canonical_sha256(
        [
            vocabulary.domain,
            vocabulary.version,
            [[theme, list(keywords)] for theme, keywords in vocabulary.theme_rules.items()],
        ]
    )
    return VocabularyIdentity(
        domain=vocabulary.domain,
        version=vocabulary.version,
        themes=themes,
        sha256=digest,
    )


def _theme_options(vocabulary: DomainVocabulary) -> tuple[tuple[str, str], ...]:
    # The vocabulary default theme is deliberately not an option; uncertainty is explicit.
    options = tuple(
        (theme, "topics such as " + ", ".join(keywords[:_THEME_KEYWORD_SAMPLE]))
        for theme, keywords in vocabulary.theme_rules.items()
    )
    return (
        *options,
        (contracts.OTHER_OR_UNCERTAIN, "no listed theme fits or the text is insufficient"),
    )


def build_question_set(objective: ObjectiveType, vocabulary: DomainVocabulary) -> QuestionSet:
    objective_text = _objective_text(objective)
    identity = vocabulary_identity(vocabulary)
    other = contracts.OTHER_OR_UNCERTAIN
    questions = (
        ScoreQuestion(
            question_id="objective_relevance",
            dimension="objective_relevance",
            instructions=_instructions(
                f"How relevant is `comment` to this research objective: {objective_text}?"
            ),
            levels=(
                "not relevant to the objective",
                "partly relevant to the objective",
                "directly relevant to the objective",
            ),
        ),
        ChoiceQuestion(
            question_id="evidence_type",
            dimension="evidence_type",
            instructions=_instructions("What kind of evidence does `comment` offer?"),
            options=(
                (
                    "firsthand_experience",
                    "the author reports something they directly experienced or did",
                ),
                ("secondhand_report", "the author relays what someone else reported"),
                (
                    "opinion_or_preference",
                    "the author states a view or preference without a concrete report",
                ),
                ("question", "the author asks for information"),
                (other, "none of the other options fits or the text is insufficient"),
            ),
        ),
        ChoiceQuestion(
            question_id="request_intent",
            dimension="request_intent",
            instructions=_instructions(
                "Does `comment` report a problem, ask for a change, or both?"
            ),
            options=(
                ("problem_report", "describes something wrong or not working"),
                ("change_request", "asks for new or changed behavior"),
                ("problem_and_request", "reports a problem and asks for a change"),
                ("neither", "neither reports a problem nor asks for a change"),
                (other, "the text is insufficient to decide"),
            ),
        ),
        ScoreQuestion(
            question_id="actionability",
            dimension="actionability",
            instructions=_instructions(
                "How actionable is `comment` for a team that maintains the product?"
            ),
            levels=(
                "nothing a team could act on",
                "a vague signal that needs follow-up",
                "a concrete issue or request a team could act on",
            ),
        ),
        NoulQuestion(
            question_id="promotional_framing",
            dimension="promotional_framing",
            instructions=_instructions(
                "Does `comment` promote a product, service, link or the author's own work?"
            ),
        ),
        ChoiceQuestion(
            question_id="theme",
            dimension="theme",
            instructions=_instructions("Which listed theme best fits the topic of `comment`?"),
            options=_theme_options(vocabulary),
        ),
    )
    return QuestionSet(
        schema_id=contracts.QUESTION_SCHEMA_ID,
        schema_version=contracts.QUESTION_SCHEMA_VERSION,
        objective=ObjectiveIdentity(objective.value, objective_text),
        vocabulary=identity,
        questions=questions,
    )
