from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import polars as pl

from rollup.columns import Col, FanoutCol
from rollup.metrics import final_main_metric


CDS_FANOUT_COLUMNS = [
    FanoutCol.ModelEventID,
    FanoutCol.ModelYear,
    FanoutCol.CurrencyCode,
    FanoutCol.ModelYOA,
    FanoutCol.ModelGrossLoss,
    FanoutCol.ModelInwardsReinstatement,
    FanoutCol.ModelEventDay,
    FanoutCol.LossClassName,
]


def write_fanouts(
    marts_dir: Path,
    frame: pl.DataFrame | pl.LazyFrame,
    fanout_prefixes: Mapping[str, str],
    target_currency: str = "GBP",
    suffix: str = "main",
    risklink_flood_events: pl.DataFrame | pl.LazyFrame | None = None,
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
        _write_parquet(shape_cds_fanout(subset, risklink_flood_events), path)
        paths.append(path)
    return tuple(sorted(paths))


def shape_cds_fanout(
    frame: pl.DataFrame | pl.LazyFrame,
    risklink_flood_events: pl.DataFrame | pl.LazyFrame | None = None,
) -> pl.LazyFrame:
    ylt = frame.lazy() if isinstance(frame, pl.DataFrame) else frame
    events = _risklink_flood_events(risklink_flood_events)
    return ylt.join(
        events,
        on=[Col.event_id, Col.region_peril_id],
        how="left",
    ).select(
        pl.when(pl.col(Col.base_model) == "risklink")
        .then(pl.col(Col.event_id))
        .otherwise(pl.col(Col.model_event_id))
        .cast(pl.Int64)
        .alias(FanoutCol.ModelEventID),
        pl.col(Col.year_id).cast(pl.Int64).alias(FanoutCol.ModelYear),
        pl.col(Col.target_currency).alias(FanoutCol.CurrencyCode),
        pl.lit(0).cast(pl.Int64).alias(FanoutCol.ModelYOA),
        pl.col(Col.loss).cast(pl.Float64).alias(FanoutCol.ModelGrossLoss),
        pl.lit(0).cast(pl.Int64).alias(FanoutCol.ModelInwardsReinstatement),
        pl.when(pl.col(Col.base_model) == "risklink")
        .then(pl.col(Col.risklink_event_day))
        .otherwise(pl.col(Col.event_day))
        .cast(pl.Int64)
        .alias(FanoutCol.ModelEventDay),
        pl.col(Col.cds_cat_class_name).alias(FanoutCol.LossClassName),
    ).select(*CDS_FANOUT_COLUMNS)


def _risklink_flood_events(
    frame: pl.DataFrame | pl.LazyFrame | None,
) -> pl.LazyFrame:
    if frame is None:
        return pl.DataFrame(
            schema={
                Col.event_id: pl.Int64,
                Col.region_peril_id: pl.Int64,
                Col.risklink_event_day: pl.Int64,
            }
        ).lazy()
    return frame.lazy() if isinstance(frame, pl.DataFrame) else frame


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
