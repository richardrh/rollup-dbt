from __future__ import annotations

from contextlib import contextmanager
import logging
from pathlib import Path
import sys
from collections.abc import Iterator


LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S"


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


def make_file_handler(log_file: str | Path, *, level: int = logging.INFO) -> logging.FileHandler:
    path = Path(log_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT))
    return handler


def configure_console_logging(log_level: str, *, log_file: str | Path | None = None) -> None:
    level = getattr(logging, log_level)
    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        datefmt=LOG_DATEFMT,
        stream=sys.stdout,
        force=True,
    )
    if log_file is not None:
        root = logging.getLogger()
        if _matching_file_handler(root, Path(log_file)) is None:
            root.addHandler(make_file_handler(log_file, level=level))


@contextmanager
def temporary_file_logging(log_file: str | Path | None) -> Iterator[None]:
    if log_file is None:
        yield
        return

    root = logging.getLogger()
    previous_level = root.level
    handler = _matching_file_handler(root, Path(log_file))
    owns_handler = handler is None
    if handler is None:
        handler = make_file_handler(log_file, level=logging.INFO)
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
