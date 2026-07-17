from __future__ import annotations

import logging
import tempfile
import time
from collections.abc import Mapping
from pathlib import Path

import polars as pl

from rollup.columns import Col, FanoutCol
from rollup.writers._sql import identifier as qid
from rollup.writers._sql import literal as qlit

logger = logging.getLogger(__name__)
_HISCO_VENDOR_LABELS = {"verisk": "AIR", "risklink": "RMS"}

_CDS_FANOUT_COLUMNS = [
    FanoutCol.ModelEventID,
    FanoutCol.ModelYear,
    FanoutCol.CurrencyCode,
    FanoutCol.ModelYOA,
    FanoutCol.ModelGrossLoss,
    FanoutCol.ModelInwardsReinstatement,
    FanoutCol.ModelEventDay,
    FanoutCol.LossClassName,
]


def validate(fanouts: Mapping[str, pl.LazyFrame], output_dir: Path) -> None:
    if not isinstance(output_dir, Path):
        raise TypeError("fanout_partitions: output_dir must be a pathlib.Path")
    if not isinstance(fanouts, Mapping):
        raise TypeError("fanout_partitions: fanouts must be a mapping")
    if not fanouts:
        raise ValueError("fanout_partitions: fanouts must not be empty")
    required = [Col.forecast_date, Col.base_model, Col.metric, *_CDS_FANOUT_COLUMNS]
    for name, frame in fanouts.items():
        if not isinstance(name, str) or not name:
            raise ValueError(
                "fanout_partitions: fanout names must be non-empty strings"
            )
        if not isinstance(frame, pl.LazyFrame):
            raise TypeError(f"fanout_partitions: fanout '{name}' must be a LazyFrame")
        try:
            schema = frame.collect_schema()
        except Exception as exc:
            raise ValueError(
                f"fanout_partitions: fanout '{name}' schema could not be resolved"
            ) from exc
        missing = [column for column in required if column not in schema]
        if missing:
            raise ValueError(
                f"fanout_partitions: fanout '{name}' missing required columns {missing}"
            )
        forecast_dtype = schema[Col.forecast_date]
        if forecast_dtype.base_type() not in {pl.Date, pl.Datetime}:
            raise ValueError(
                f"fanout_partitions: fanout '{name}' column '{Col.forecast_date}' "
                f"must be date-like, got {forecast_dtype}"
            )


def write(fanouts: Mapping[str, pl.LazyFrame], output_dir: Path) -> Path:
    validate(fanouts, output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="rollup-fanouts-", dir=output_dir.parent
    ) as temp_dir:
        staging_dir = Path(temp_dir)
        materialized_dir = staging_dir / "materialized"
        partitions_dir = staging_dir / "partitions"
        materialized_dir.mkdir()
        partitions_dir.mkdir()
        for name, frame in fanouts.items():
            started = time.perf_counter()
            materialized_path = materialized_dir / f"{name}.parquet"
            logger.info(
                "materializing fanout once fanout=%s output=%s",
                name,
                materialized_path,
                extra={
                    "event": "fanout_materialize_start",
                    "fanout": name,
                    "path": materialized_path,
                },
            )
            frame.sink_parquet(materialized_path)
            logger.info(
                "materialized fanout fanout=%s elapsed=%.2fs",
                name,
                time.perf_counter() - started,
                extra={"event": "fanout_materialize_done", "fanout": name},
            )
            _write_materialized_partitions(
                name, materialized_path, partitions_dir, started
            )
        staged_paths = sorted(partitions_dir.glob("*.parquet"))
        staged_names = {path.name for path in staged_paths}
        for staged_path in staged_paths:
            staged_path.replace(output_dir / staged_path.name)
        for stale_mart in output_dir.glob("*.parquet"):
            if stale_mart.name not in staged_names:
                stale_mart.unlink()
    return output_dir


def _write_materialized_partitions(
    name: str, frame_path: Path, output_dir: Path, started: float
) -> None:
    import duckdb

    con = duckdb.connect()
    try:
        partitions = con.execute(
            f"""
            SELECT DISTINCT {qid(Col.forecast_date)}, {qid(Col.base_model)}, {qid(Col.metric)}
            FROM read_parquet({qlit(frame_path)})
            ORDER BY 1, 2, 3
            """
        ).fetchall()
        logger.info(
            "collected fanout partitions fanout=%s partitions=%d elapsed=%.2fs",
            name,
            len(partitions),
            time.perf_counter() - started,
            extra={
                "event": "fanout_partitions_done",
                "fanout": name,
                "partition_count": len(partitions),
            },
        )
        columns = ", ".join(qid(column) for column in _CDS_FANOUT_COLUMNS)
        for forecast_date, base_model, metric in partitions:
            tag = forecast_date.strftime("%Y%m")
            try:
                vendor = _HISCO_VENDOR_LABELS[base_model]
            except KeyError as exc:
                raise ValueError(f"unknown base model: {base_model}") from exc
            output_path = output_dir / f"Hisco{vendor}_{tag}_{metric}.parquet"
            logger.info(
                "writing fanout partition fanout=%s output=%s base_model=%s metric=%s forecast_date=%s",
                name,
                output_path,
                base_model,
                metric,
                forecast_date,
                extra={
                    "event": "fanout_partition_write",
                    "fanout": name,
                    "path": output_path,
                },
            )
            con.execute(
                f"""
                COPY (
                    SELECT {columns}
                    FROM read_parquet({qlit(frame_path)})
                    WHERE CAST({qid(Col.forecast_date)} AS VARCHAR) = {qlit(forecast_date)}
                      AND {qid(Col.base_model)} = {qlit(base_model)}
                      AND {qid(Col.metric)} = {qlit(metric)}
                ) TO {qlit(output_path)} (FORMAT PARQUET)
                """
            )
    finally:
        con.close()
