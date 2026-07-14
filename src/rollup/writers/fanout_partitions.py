from __future__ import annotations
# mypy: ignore-errors

import logging
import time
from pathlib import Path

from rollup.columns import Col, FanoutCol
from rollup.pipeline_utils import _sql_identifier, _sql_literal, forecast_tag, hisco_vendor_label


logger = logging.getLogger(__name__)


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


def _write_fanout_partitions(
    name: str,
    frame_path: Path,
    output_dir: Path,
    started: float,
) -> None:
    import duckdb

    logger.info("collecting fanout partitions fanout=%s", name, extra={"event": "fanout_partitions_start", "fanout": name})
    con = duckdb.connect()
    partitions = con.execute(f"SELECT DISTINCT {_sql_identifier(Col.forecast_date)}, {_sql_identifier(Col.base_model)}, {_sql_identifier(Col.metric)} FROM read_parquet({_sql_literal(frame_path)}) ORDER BY 1, 2, 3").fetchall()
    elapsed_seconds = time.perf_counter() - started
    logger.info(
        "collected fanout partitions fanout=%s partitions=%d elapsed=%.2fs",
        name,
        len(partitions),
        elapsed_seconds,
        extra={
            "event": "fanout_partitions_done",
            "fanout": name,
            "partition_count": len(partitions),
            "elapsed_seconds": elapsed_seconds,
        },
    )
    for forecast_date, base_model, metric in partitions:
        tag = forecast_tag(forecast_date)
        vendor = hisco_vendor_label(base_model)
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
                "base_model": base_model,
                "metric": metric,
                "forecast_date": forecast_date,
            },
        )
        output_path.unlink(missing_ok=True)
        columns = ", ".join(_sql_identifier(column) for column in _CDS_FANOUT_COLUMNS)
        con.execute(f"COPY (SELECT {columns} FROM read_parquet({_sql_literal(frame_path)}) WHERE CAST({_sql_identifier(Col.forecast_date)} AS VARCHAR) = {_sql_literal(forecast_date)} AND {_sql_identifier(Col.base_model)} = {_sql_literal(base_model)} AND {_sql_identifier(Col.metric)} = {_sql_literal(metric)}) TO {_sql_literal(output_path)} (FORMAT PARQUET)")
    con.close()
