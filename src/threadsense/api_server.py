from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from threadsense.config import AppConfig
from threadsense.contracts import AbstractionLevel, DomainType, ObjectiveType, contract_from_config
from threadsense.errors import ApiInputError, ThreadSenseError
from threadsense.inference import InferenceTask
from threadsense.observability import DEFAULT_METRICS, MetricsRegistry, TraceContext, observe_stage
from threadsense.workflows import (
    RedditConnectorFactory,
    analyze_normalized_thread,
    build_source_registry,
    fetch_reddit_thread,
    fetch_source_thread,
    infer_analysis,
    normalize_source_thread,
    report_analysis,
    run_source_pipeline,
)
from threadsense.workflows import normalize_reddit_thread as normalize_reddit_workflow


@dataclass(frozen=True)
class ApiServerHandle:
    server: ThreadingHTTPServer
    thread: threading.Thread

    @property
    def base_url(self) -> str:
        host, port = self.server.server_address[:2]
        resolved_host = host.decode("utf-8") if isinstance(host, bytes) else host
        return f"http://{resolved_host}:{port}"


@dataclass(frozen=True)
class ServerDependencies:
    config: AppConfig
    logger: logging.Logger
    connector_factory: RedditConnectorFactory
    registry: MetricsRegistry


def start_api_server(
    *,
    config: AppConfig,
    logger: logging.Logger,
    connector_factory: RedditConnectorFactory,
    registry: MetricsRegistry = DEFAULT_METRICS,
    host: str | None = None,
    port: int | None = None,
) -> ApiServerHandle:
    dependencies = ServerDependencies(
        config=config,
        logger=logger,
        connector_factory=connector_factory,
        registry=registry,
    )
    server = ThreadingHTTPServer(
        (host or config.api.host, port or config.api.port),
        build_handler(dependencies),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return ApiServerHandle(server=server, thread=thread)


def build_handler(dependencies: ServerDependencies) -> type[BaseHTTPRequestHandler]:
    class ThreadSenseApiHandler(BaseHTTPRequestHandler):
        server_version = "ThreadSenseHTTP/1.0"

        def do_GET(self) -> None:  # noqa: N802
            trace = TraceContext.create(run_id="api", source_name="http")
            if self.path == "/v1/healthz":
                self._write_json(HTTPStatus.OK, {"status": "ready"})
                return
            if self.path == "/v1/metrics":
                with observe_stage(
                    registry=dependencies.registry,
                    logger=dependencies.logger,
                    trace=trace,
                    stage="api_metrics",
                ):
                    body = dependencies.registry.render_prometheus().encode("utf-8")
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", "text/plain; version=0.0.4")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                return
            self._write_error(
                HTTPStatus.NOT_FOUND,
                ApiInputError("API route does not exist", details={"path": self.path}),
            )

        def do_POST(self) -> None:  # noqa: N802
            trace = TraceContext.create(run_id="api", source_name="http")
            try:
                with observe_stage(
                    registry=dependencies.registry,
                    logger=dependencies.logger,
                    trace=trace,
                    stage="api_request",
                    labels={"path": self.path},
                ):
                    payload = self._read_json_body(dependencies.config.api.max_request_bytes)
                    response = self._dispatch(payload, trace)
            except ThreadSenseError as error:
                self._write_error(HTTPStatus.BAD_REQUEST, error)
                return
            self._write_json(HTTPStatus.OK, response)

        def log_message(self, format: str, *args: object) -> None:
            return

        def _dispatch(self, payload: dict[str, Any], trace: TraceContext) -> dict[str, Any]:
            if self.path == "/v1/fetch/reddit":
                return fetch_reddit_thread(
                    config=dependencies.config,
                    logger=dependencies.logger,
                    trace=trace,
                    url=required_str(payload, "url"),
                    output_path=optional_path(payload, "output_path"),
                    expand_more=optional_bool(payload, "expand_more", False),
                    flat=optional_bool(payload, "flat", False),
                    connector_factory=dependencies.connector_factory,
                    registry=dependencies.registry,
                ).to_dict()
            if self.path in {"/v1/fetch/hackernews", "/v1/fetch/hn"}:
                return fetch_source_thread(
                    config=dependencies.config,
                    logger=dependencies.logger,
                    trace=trace,
                    url=required_str(payload, "url"),
                    output_path=optional_path(payload, "output_path"),
                    source_name="hackernews",
                    registry_factory=build_source_registry,
                    registry=dependencies.registry,
                ).to_dict()
            if self.path == "/v1/normalize/reddit":
                return normalize_reddit_workflow(
                    config=dependencies.config,
                    logger=dependencies.logger,
                    trace=trace,
                    input_path=required_path(payload, "input_path"),
                    output_path=optional_path(payload, "output_path"),
                    registry=dependencies.registry,
                ).to_dict()
            if self.path in {"/v1/normalize/hackernews", "/v1/normalize/hn"}:
                return normalize_source_thread(
                    config=dependencies.config,
                    logger=dependencies.logger,
                    trace=trace,
                    input_path=required_path(payload, "input_path"),
                    output_path=optional_path(payload, "output_path"),
                    registry_factory=build_source_registry,
                    registry=dependencies.registry,
                ).to_dict()
            if self.path == "/v1/analyze/normalized":
                return analyze_normalized_thread(
                    config=dependencies.config,
                    logger=dependencies.logger,
                    trace=trace,
                    input_path=required_path(payload, "input_path"),
                    output_path=optional_path(payload, "output_path"),
                    contract=optional_contract(dependencies.config, payload),
                    registry=dependencies.registry,
                ).to_dict()
            if self.path == "/v1/report/analysis":
                return report_analysis(
                    config=dependencies.config,
                    logger=dependencies.logger,
                    trace=trace,
                    input_path=required_path(payload, "input_path"),
                    output_path=optional_path(payload, "output_path"),
                    report_format=optional_str(payload, "format", "markdown"),
                    with_summary=optional_bool(payload, "with_summary", False),
                    summary_required=optional_bool(payload, "summary_required", False),
                    registry=dependencies.registry,
                ).to_dict()
            if self.path == "/v1/infer/analysis":
                return infer_analysis(
                    config=dependencies.config,
                    logger=dependencies.logger,
                    trace=trace,
                    input_path=required_path(payload, "input_path"),
                    task=InferenceTask(optional_str(payload, "task", "analysis_summary")),
                    required=optional_bool(payload, "required", False),
                    registry=dependencies.registry,
                ).to_dict()
            if self.path == "/v1/run":
                return run_source_pipeline(
                    config=dependencies.config,
                    logger=dependencies.logger,
                    trace=trace,
                    url=required_str(payload, "url"),
                    source_name=optional_source(payload),
                    report_format=optional_str(payload, "format", "markdown"),
                    with_summary=optional_bool(payload, "with_summary", False),
                    summary_required=optional_bool(payload, "summary_required", False),
                    contract=optional_contract(dependencies.config, payload),
                    registry_factory=build_source_registry,
                    registry=dependencies.registry,
                ).to_dict()
            raise ApiInputError("API route does not exist", details={"path": self.path})

        def _read_json_body(self, max_request_bytes: int) -> dict[str, Any]:
            content_length_header = self.headers.get("Content-Length", "0")
            try:
                content_length = int(content_length_header)
            except ValueError as error:
                raise ApiInputError(
                    "Content-Length header is invalid",
                    details={"content_length": content_length_header},
                ) from error
            if content_length <= 0:
                raise ApiInputError("request body is required")
            if content_length > max_request_bytes:
                raise ApiInputError(
                    "request body exceeds max_request_bytes",
                    details={
                        "content_length": content_length,
                        "max_request_bytes": max_request_bytes,
                    },
                )
            raw_body = self.rfile.read(content_length)
            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except json.JSONDecodeError as error:
                raise ApiInputError("request body must be valid JSON") from error
            if not isinstance(payload, dict):
                raise ApiInputError("request body must decode to an object")
            return payload

        def _write_json(self, status_code: HTTPStatus, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, indent=2).encode("utf-8")
            self.send_response(status_code.value)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _write_error(self, status_code: HTTPStatus, error: ThreadSenseError) -> None:
            self._write_json(status_code, {"status": "error", "error": error.to_dict()})

    return ThreadSenseApiHandler


def required_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ApiInputError("request field is invalid", details={"key": key})
    return value


def optional_str(payload: dict[str, Any], key: str, default: str) -> str:
    value = payload.get(key, default)
    if not isinstance(value, str) or not value:
        raise ApiInputError("request field is invalid", details={"key": key})
    return value


def optional_bool(payload: dict[str, Any], key: str, default: bool) -> bool:
    value = payload.get(key, default)
    if not isinstance(value, bool):
        raise ApiInputError("request field is invalid", details={"key": key})
    return value


def required_path(payload: dict[str, Any], key: str) -> Path:
    return Path(required_str(payload, key))


def optional_path(payload: dict[str, Any], key: str) -> Path | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ApiInputError("request field is invalid", details={"key": key})
    return Path(value)


def optional_source(payload: dict[str, Any]) -> str | None:
    value = payload.get("source")
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ApiInputError("request field is invalid", details={"key": "source"})
    if value == "hn":
        return "hackernews"
    return value


def optional_contract(config: AppConfig, payload: dict[str, Any]) -> Any:
    domain = payload.get("domain")
    objective = payload.get("objective")
    abstraction_level = payload.get("abstraction_level", payload.get("level"))
    if domain is None and objective is None and abstraction_level is None:
        return None
    analysis_config = config.analysis.model_copy(
        update={
            "domain": DomainType(str(domain)) if domain is not None else config.analysis.domain,
            "objective": (
                ObjectiveType(str(objective))
                if objective is not None
                else config.analysis.objective
            ),
            "abstraction_level": (
                AbstractionLevel(str(abstraction_level))
                if abstraction_level is not None
                else config.analysis.abstraction_level
            ),
        }
    )
    return contract_from_config(analysis_config)
