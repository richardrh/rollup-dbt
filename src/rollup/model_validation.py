from __future__ import annotations

import polars as pl


def validate_schema(model: str, expected: pl.Schema, frame: pl.LazyFrame) -> None:
    try:
        actual = frame.collect_schema()
    except Exception as exc:  # pragma: no cover - preserves original Polars context
        raise ValueError(f"{model}: could not resolve output schema: {exc}") from exc
    if actual != expected:
        raise ValueError(
            f"{model}: output schema mismatch; expected {expected}, got {actual}"
        )
