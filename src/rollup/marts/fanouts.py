from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import polars as pl

from rollup.columns import Col, FanoutCol
from rollup.metrics import final_main_metric
from rollup.intermediate.build_dialsup import dialsup_metric


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

INTERNAL_FANOUT_SOURCE_FILE = "_fanout_source.parquet"
_FANOUT_SUFFIX_COLUMN = "__fanout_suffix"
_FANOUT_SPLIT_COLUMNS = [Col.base_model, Col.forecast_date, _FANOUT_SUFFIX_COLUMN]
_FANOUT_PAYLOAD_COLUMNS = [
    Col.base_model,
    Col.forecast_date,
    Col.target_currency,
    Col.year_id,
    Col.event_id,
    Col.model_event_id,
    Col.event_day,
    Col.region_peril_id,
    Col.cds_cat_class_name,
    Col.loss,
]
_FANOUT_VALIDATION_COLUMNS = [
    Col.event_id,
    Col.region_peril_id,
    Col.risklink_event_day,
]
_CDS_FANOUT_REQUIRED_COLUMNS = [
    FanoutCol.ModelEventID,
    FanoutCol.ModelYear,
    FanoutCol.CurrencyCode,
    FanoutCol.ModelGrossLoss,
    FanoutCol.ModelEventDay,
    FanoutCol.LossClassName,
]


def main_fanout_source(
    frame: pl.DataFrame | pl.LazyFrame,
    target_currency: str = "GBP",
) -> pl.LazyFrame:
    source = frame.lazy() if isinstance(frame, pl.DataFrame) else frame
    return source.select(
        Col.base_model,
        Col.forecast_date,
        Col.target_currency,
        Col.year_id,
        Col.event_id,
        Col.model_event_id,
        Col.event_day,
        Col.region_peril_id,
        Col.cds_cat_class_name,
        pl.lit(final_main_metric(target_currency)).alias(Col.metric),
        pl.col("euws_loss").cast(pl.Float64).alias(Col.loss),
    )


def dialsup_fanout_source(
    frame: pl.DataFrame | pl.LazyFrame,
    target_currency: str = "GBP",
) -> pl.LazyFrame:
    source = frame.lazy() if isinstance(frame, pl.DataFrame) else frame
    return source.filter(pl.col(Col.is_dialsup) == 1).select(
        Col.base_model,
        Col.forecast_date,
        Col.target_currency,
        Col.year_id,
        Col.event_id,
        Col.model_event_id,
        Col.event_day,
        Col.region_peril_id,
        Col.cds_cat_class_name,
        pl.lit(dialsup_metric(target_currency)).alias(Col.metric),
        (pl.col(Col.loss) * pl.col(Col.fx_rate) * pl.col(Col.forecast_factor))
        .cast(pl.Float64)
        .alias(Col.loss),
    )


def write_fanouts(
    marts_dir: Path,
    frame: pl.DataFrame | pl.LazyFrame,
    fanout_prefixes: Mapping[str, str],
    target_currency: str = "GBP",
    suffix: str = "main",
    verisk_events: pl.DataFrame | pl.LazyFrame | None = None,
    risklink_flood_events: pl.DataFrame | pl.LazyFrame | None = None,
) -> tuple[Path, ...]:
    marts_dir.mkdir(parents=True, exist_ok=True)
    source_path = marts_dir / INTERNAL_FANOUT_SOURCE_FILE
    source = shape_cds_fanout_with_split_columns(
        _fanout_payload(frame, suffix, target_currency),
        risklink_flood_events,
    )
    try:
        _write_parquet(source, source_path)
        validate_materialized_fanout_source(source_path, verisk_events)
        paths = split_materialized_fanouts(source_path, marts_dir, fanout_prefixes)
        return paths
    finally:
        source_path.unlink(missing_ok=True)


def write_materialized_fanouts(
    marts_dir: Path,
    main_frame: pl.DataFrame | pl.LazyFrame,
    dialsup_frame: pl.DataFrame | pl.LazyFrame | None,
    fanout_prefixes: Mapping[str, str],
    target_currency: str = "GBP",
    dialsup_suffix: str = "dialsup",
    verisk_events: pl.DataFrame | pl.LazyFrame | None = None,
    risklink_flood_events: pl.DataFrame | pl.LazyFrame | None = None,
) -> tuple[Path, ...]:
    marts_dir.mkdir(parents=True, exist_ok=True)
    source_path = marts_dir / INTERNAL_FANOUT_SOURCE_FILE
    source = materialized_fanout_source(
        main_frame,
        dialsup_frame,
        target_currency,
        dialsup_suffix,
        risklink_flood_events,
    )
    try:
        _write_parquet(source, source_path)
        validate_materialized_fanout_source(source_path, verisk_events)
        paths = split_materialized_fanouts(source_path, marts_dir, fanout_prefixes)
        return paths
    finally:
        source_path.unlink(missing_ok=True)


def materialized_fanout_source(
    main_frame: pl.DataFrame | pl.LazyFrame,
    dialsup_frame: pl.DataFrame | pl.LazyFrame | None,
    target_currency: str = "GBP",
    dialsup_suffix: str = "dialsup",
    risklink_flood_events: pl.DataFrame | pl.LazyFrame | None = None,
) -> pl.LazyFrame:
    frames = [_fanout_payload(main_frame, "main", target_currency)]
    if dialsup_frame is not None:
        frames.append(_fanout_payload(dialsup_frame, dialsup_suffix, target_currency))
    fanout = pl.concat(frames, how="vertical_relaxed")
    return shape_cds_fanout_with_split_columns(fanout, risklink_flood_events)


def split_materialized_fanouts(
    source_path: Path,
    marts_dir: Path,
    fanout_prefixes: Mapping[str, str],
) -> tuple[Path, ...]:
    fanout = pl.scan_parquet(source_path)
    keys = fanout.select(*_FANOUT_SPLIT_COLUMNS).unique().collect()
    if keys.is_empty():
        return ()
    paths: list[Path] = []
    key_rows = sorted(
        keys.iter_rows(named=True),
        key=lambda row: (
            str(row[Col.base_model]),
            str(row[Col.forecast_date]),
            0 if row[_FANOUT_SUFFIX_COLUMN] == "main" else 1,
            str(row[_FANOUT_SUFFIX_COLUMN]),
        ),
    )
    for row in key_rows:
        subset = fanout.filter(
            (pl.col(Col.base_model) == row[Col.base_model])
            & (pl.col(Col.forecast_date) == row[Col.forecast_date])
            & (pl.col(_FANOUT_SUFFIX_COLUMN) == row[_FANOUT_SUFFIX_COLUMN])
        ).select(*CDS_FANOUT_COLUMNS)
        prefix = fanout_prefix(row[Col.base_model], fanout_prefixes)
        forecast = str(row[Col.forecast_date]).replace("-", "")
        path = marts_dir / f"{prefix}_{forecast}_{row[_FANOUT_SUFFIX_COLUMN]}.parquet"
        _write_parquet(subset, path)
        paths.append(path)
    return tuple(paths)


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


def shape_cds_fanout_with_split_columns(
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
        Col.base_model,
        Col.forecast_date,
        _FANOUT_SUFFIX_COLUMN,
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
        pl.col(Col.event_id).cast(pl.Int64),
        pl.col(Col.region_peril_id).cast(pl.Int64),
        pl.col(Col.risklink_event_day).cast(pl.Int64),
    ).select(*_FANOUT_SPLIT_COLUMNS, *CDS_FANOUT_COLUMNS, *_FANOUT_VALIDATION_COLUMNS)


def validate_materialized_fanout_source(
    source_path: Path,
    verisk_events: pl.DataFrame | pl.LazyFrame | None = None,
) -> None:
    source = pl.scan_parquet(source_path)
    validate_risklink_fanout_events(source)
    validate_cds_fanout_frame(source.select(*CDS_FANOUT_COLUMNS))
    if verisk_events is not None:
        validate_verisk_fanout_events(source, verisk_events)


def validate_cds_fanout_frame(frame: pl.DataFrame | pl.LazyFrame) -> None:
    fanout = frame.lazy() if isinstance(frame, pl.DataFrame) else frame
    columns = fanout.collect_schema().names()
    if columns != CDS_FANOUT_COLUMNS:
        raise ValueError(
            "CDS fanout schema mismatch; "
            f"expected {CDS_FANOUT_COLUMNS}, got {columns}"
        )
    null_counts = fanout.select(
        pl.col(column).null_count().alias(column)
        for column in _CDS_FANOUT_REQUIRED_COLUMNS
    ).collect()
    nulls = {
        column: int(null_counts.select(column).item())
        for column in _CDS_FANOUT_REQUIRED_COLUMNS
        if int(null_counts.select(column).item()) > 0
    }
    if nulls:
        details = ", ".join(f"{column}={count}" for column, count in nulls.items())
        raise ValueError(f"CDS fanout required field nulls: {details}")


def validate_verisk_fanout_events(
    source: pl.DataFrame | pl.LazyFrame,
    verisk_events: pl.DataFrame | pl.LazyFrame,
) -> None:
    fanout = source.lazy() if isinstance(source, pl.DataFrame) else source
    events = verisk_events.lazy() if isinstance(verisk_events, pl.DataFrame) else verisk_events
    mismatches = fanout.filter(pl.col(Col.base_model) == "verisk").join(
        events.select(
            pl.col(Col.model_event_id).cast(pl.Int64),
            pl.col(Col.year_id).cast(pl.Int64),
            pl.col(Col.event_day).cast(pl.Int64),
        ).unique(),
        left_on=[FanoutCol.ModelEventID, FanoutCol.ModelYear, FanoutCol.ModelEventDay],
        right_on=[Col.model_event_id, Col.year_id, Col.event_day],
        how="anti",
    )
    _raise_for_mismatches("Verisk fanout event validation failed", mismatches)


def validate_risklink_fanout_events(source: pl.DataFrame | pl.LazyFrame) -> None:
    fanout = source.lazy() if isinstance(source, pl.DataFrame) else source
    mismatches = fanout.filter(pl.col(Col.base_model) == "risklink").filter(
        pl.col(Col.risklink_event_day).is_null()
        | (pl.col(FanoutCol.ModelEventDay) != pl.col(Col.risklink_event_day))
    )
    _raise_for_mismatches("RiskLink fanout event validation failed", mismatches)


def _raise_for_mismatches(message: str, mismatches: pl.LazyFrame) -> None:
    count = mismatches.select(pl.len()).collect().item()
    if count == 0:
        return
    sample = mismatches.head(5).collect().to_dicts()
    raise ValueError(f"{message}: {count} mismatch row(s); sample={sample}")


def _fanout_payload(
    frame: pl.DataFrame | pl.LazyFrame,
    suffix: str,
    target_currency: str,
) -> pl.LazyFrame:
    source = frame.lazy() if isinstance(frame, pl.DataFrame) else frame
    if suffix == "main" and Col.metric in source.collect_schema().names():
        source = source.filter(pl.col(Col.metric) == final_main_metric(target_currency))
    return source.select(
        *_FANOUT_PAYLOAD_COLUMNS,
        pl.lit(suffix).alias(_FANOUT_SUFFIX_COLUMN),
    )


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
