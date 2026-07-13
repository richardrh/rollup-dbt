from __future__ import annotations
# mypy: ignore-errors

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import polars as pl

from rollup.columns import RawCol


logger = logging.getLogger(__name__)


@contextmanager
def logged_phase(phase: str) -> Iterator[None]:
    started = time.perf_counter()
    logger.info("start phase=%s", phase, extra={"event": "phase_start", "phase": phase})
    try:
        yield
    except Exception:
        elapsed_seconds = time.perf_counter() - started
        logger.exception(
            "failed phase=%s elapsed=%.2fs",
            phase,
            elapsed_seconds,
            extra={"event": "phase_failed", "phase": phase, "elapsed_seconds": elapsed_seconds},
        )
        raise
    elapsed_seconds = time.perf_counter() - started
    logger.info(
        "done phase=%s elapsed=%.2fs",
        phase,
        elapsed_seconds,
        extra={"event": "phase_done", "phase": phase, "elapsed_seconds": elapsed_seconds},
    )


def _verisk_string(column: RawCol) -> pl.Expr:
    return pl.col(column).cast(pl.String).str.strip_chars()


def _sql_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def forecast_tag(value: object) -> str:
    if hasattr(value, "strftime"):
        return value.strftime("%Y%m")
    return str(value).replace("-", "")[:6]


def hisco_vendor_label(base_model: str) -> str:
    if base_model == "verisk":
        return "AIR"
    if base_model == "risklink":
        return "RMS"
    raise ValueError(f"unknown base model: {base_model}")
