# Golden Datasets

These datasets pin expected deterministic behavior for annotated fixtures.

- `thread_fixture` points to a canonical thread fixture committed in the repository.
- `expected_findings` capture required themes, minimum severity, and required evidence ids.
- `expected_duplicate_groups` capture duplicate clusters that must remain detectable.
- `expected_absent_themes` documents themes that must not appear for the fixture domain.

These goldens cover deterministic theme, severity, evidence and duplicate expectations. They are not labels for the new typed semantic dimensions and do not establish independent adjudication or representative accuracy. See the separate [decision evaluation fixture guide](../fixtures/decisions/evaluation/README.md) for the engineering-authored semantic checks.
