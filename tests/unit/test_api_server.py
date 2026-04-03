from __future__ import annotations

import json
from pathlib import Path
from urllib import error, request

import pytest

from threadsense.api_server import start_api_server
from threadsense.config import load_config
from threadsense.logging_config import configure_logging


def test_api_server_rejects_invalid_json_body(tmp_path: Path) -> None:
    config = load_config(
        env={
            "THREADSENSE_API_PORT": "0",
            "THREADSENSE_STORAGE_ROOT": str(tmp_path),
        }
    )
    logger = configure_logging()
    handle = start_api_server(
        config=config,
        logger=logger,
        connector_factory=lambda app_config: pytest.fail("connector should not be used"),
        port=0,
    )
    try:
        api_request = request.Request(
            f"{handle.base_url}/v1/fetch/reddit",
            data=b"not-json",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(error.HTTPError) as raised:
            request.urlopen(api_request)
        payload = json.loads(raised.value.read().decode("utf-8"))

        assert raised.value.code == 400
        assert payload["error"]["code"] == "api_input_error"
    finally:
        handle.server.shutdown()
        handle.server.server_close()
        handle.thread.join(timeout=2)
