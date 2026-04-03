from __future__ import annotations

from threadsense.errors import ConfigurationError


def test_error_serialization_includes_code_message_and_details() -> None:
    error = ConfigurationError("invalid config", details={"key": "THREADSENSE_RUNTIME_MODEL"})

    assert str(error) == "configuration_error: invalid config"
    assert error.to_dict() == {
        "type": "ConfigurationError",
        "code": "configuration_error",
        "message": "invalid config",
        "details": {"key": "THREADSENSE_RUNTIME_MODEL"},
    }
