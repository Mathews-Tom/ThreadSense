from __future__ import annotations

import json
import logging


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
        }
        try:
            message = json.loads(record.getMessage())
        except json.JSONDecodeError:
            message = {"message": record.getMessage()}
        if isinstance(message, dict):
            payload.update(message)
        else:
            payload["message"] = message
        return json.dumps(payload, sort_keys=True)


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger("threadsense")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = JsonLogFormatter()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger
