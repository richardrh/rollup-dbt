from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sys
from collections.abc import Iterator


LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S"
LogFormat = str

_LOG_RECORD_FIELDS = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}


def _json_safe(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    return str(value)


def normalize_log_format(log_format: str | None = None) -> str:
    value = (log_format or "text").lower()
    if value == "json":
        return "jsonl"
    if value not in {"text", "jsonl"}:
        raise ValueError("log format must be 'text' or 'jsonl'")
    return value


class JsonLineFormatter(logging.Formatter):
    """Format log records as newline-delimited JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key in _LOG_RECORD_FIELDS or key.startswith("_"):
                continue
            payload[key] = _json_safe(value)
        return json.dumps(payload, ensure_ascii=False)


def make_formatter(log_format: LogFormat = "text") -> logging.Formatter:
    normalized = normalize_log_format(log_format)
    if normalized == "jsonl":
        return JsonLineFormatter()
    return logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT)


def _matching_file_handler(logger: logging.Logger, log_file: Path) -> logging.FileHandler | None:
    try:
        resolved_log_file = log_file.resolve()
    except FileNotFoundError:
        resolved_log_file = log_file.absolute()
    for handler in logger.handlers:
        if not isinstance(handler, logging.FileHandler):
            continue
        try:
            handler_path = Path(handler.baseFilename).resolve()
        except FileNotFoundError:
            handler_path = Path(handler.baseFilename).absolute()
        if handler_path == resolved_log_file:
            return handler
    return None


def make_file_handler(
    log_file: str | Path,
    *,
    level: int = logging.INFO,
    log_format: LogFormat = "text",
) -> logging.FileHandler:
    path = Path(log_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setLevel(level)
    handler.setFormatter(make_formatter(log_format))
    return handler


def configure_console_logging(
    log_level: str,
    *,
    log_file: str | Path | None = None,
    log_format: LogFormat = "text",
) -> None:
    level = getattr(logging, log_level)
    logging.basicConfig(
        level=level,
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    for handler in logging.getLogger().handlers:
        handler.setFormatter(make_formatter(log_format))
    if log_file is not None:
        root = logging.getLogger()
        if _matching_file_handler(root, Path(log_file)) is None:
            root.addHandler(make_file_handler(log_file, level=level, log_format=log_format))


@contextmanager
def temporary_file_logging(
    log_file: str | Path | None,
    *,
    log_format: LogFormat = "text",
) -> Iterator[None]:
    if log_file is None:
        yield
        return

    root = logging.getLogger()
    previous_level = root.level
    handler = _matching_file_handler(root, Path(log_file))
    owns_handler = handler is None
    if handler is None:
        handler = make_file_handler(log_file, level=logging.INFO, log_format=log_format)
        root.addHandler(handler)
    if root.level > logging.INFO:
        root.setLevel(logging.INFO)
    try:
        yield
    finally:
        root.setLevel(previous_level)
        if owns_handler:
            root.removeHandler(handler)
            handler.close()
