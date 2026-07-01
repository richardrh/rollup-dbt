from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import polars as pl

from rollup.columns import Col
from rollup.metrics import final_main_metric


def write_fanouts(
    marts_dir: Path,
    frame: pl.DataFrame | pl.LazyFrame,
    fanout_prefixes: Mapping[str, str],
    target_currency: str = "GBP",
    suffix: str = "main",
) -> tuple[Path, ...]:
    paths: list[Path] = []
    source = frame.lazy() if isinstance(frame, pl.DataFrame) else frame
    fanout = source.filter(pl.col(Col.metric) == final_main_metric(target_currency)) if suffix == "main" else source
    keys = fanout.select(Col.base_model, Col.forecast_date).unique().sort(Col.base_model, Col.forecast_date).collect()
    if keys.is_empty():
        return ()
    for row in keys.iter_rows(named=True):
        subset = fanout.filter(
            (pl.col(Col.base_model) == row[Col.base_model])
            & (pl.col(Col.forecast_date) == row[Col.forecast_date])
        )
        prefix = fanout_prefix(row[Col.base_model], fanout_prefixes)
        forecast = str(row[Col.forecast_date]).replace("-", "")
        path = marts_dir / f"{prefix}_{forecast}_{suffix}.parquet"
        _write_parquet(subset, path)
        paths.append(path)
    return tuple(sorted(paths))


def fanout_prefix(base_model: str, fanout_prefixes: Mapping[str, str]) -> str:
    key = str(base_model).lower()
    try:
        return fanout_prefixes[key]
    except KeyError as exc:
        known = ", ".join(sorted(fanout_prefixes))
        raise ValueError(
            f"unsupported base model for fanout {base_model!r}; expected one of: {known}"
        ) from exc


def _write_parquet(frame: pl.DataFrame | pl.LazyFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(frame, pl.LazyFrame):
        frame.sink_parquet(path, mkdir=True)
        return
    frame.write_parquet(path)
