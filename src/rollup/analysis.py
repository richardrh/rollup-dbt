from __future__ import annotations

import logging
import time
import tempfile
from pathlib import Path

import polars as pl

from rollup.config import RollupConfig, read_config
from rollup.output_contract import (
    ANALYSIS_DIR,
    COMBINED_YLT_FILE,
    DIALSUP_YLT_FILE,
    EP_REPORT_FILE,
)

logger = logging.getLogger(__name__)


def build_ep_report(
    output_root: Path | str = "output", *, config: RollupConfig | None = None
) -> pl.DataFrame:
    output_root = Path(output_root)
    config = config or read_config()

    loss_inputs = []
    for path, metric in [
        (output_root / COMBINED_YLT_FILE, "euws_override"),
        (output_root / DIALSUP_YLT_FILE, "dialsup_localccy_forecast"),
    ]:
        loss_inputs.append(
            pl.scan_parquet(path)
            .filter(pl.col("metric") == metric)
            .select(
                "forecast_date",
                "base_model",
                "rollup_lob",
                "rollup_peril",
                "year_id",
                "metric",
                "loss",
            )
        )

    losses = pl.concat(loss_inputs, how="vertical").with_columns(
        pl.col("base_model")
        .replace_strict(config.analysis.simulation_counts, return_dtype=pl.Int64)
        .alias("n_simulations")
    )

    aal = _build_aal(losses)
    ep = pl.concat(
        [
            _build_ranked_ep(losses, ep_type="AEP", aggregation="sum", config=config),
            _build_ranked_ep(losses, ep_type="OEP", aggregation="max", config=config),
        ],
        how="vertical",
    )

    return (
        pl.concat([aal, ep], how="vertical")
        .sort(
            [
                "forecast_date",
                "metric",
                "ep_type",
                "return_period",
                "base_model",
                "rollup_lob",
                "rollup_peril",
            ]
        )
        .collect()
    )


def write_ep_report(
    output_root: Path | str = "output", *, config: RollupConfig | None = None
) -> Path:
    output_root = Path(output_root)
    output_path = output_root / ANALYSIS_DIR / EP_REPORT_FILE
    output_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    logger.info(
        "writing output=%s",
        output_path,
        extra={"event": "analysis_report_start", "path": output_path},
    )
    report = build_ep_report(output_root, config=config)
    with tempfile.NamedTemporaryFile(
        suffix=".csv",
        prefix=f".{output_path.stem}-",
        dir=output_path.parent,
        delete=False,
    ) as handle:
        staged_path = Path(handle.name)
    try:
        report.write_csv(staged_path)
        staged_path.replace(output_path)
    finally:
        staged_path.unlink(missing_ok=True)
    elapsed_seconds = time.perf_counter() - started
    logger.info(
        "wrote output=%s rows=%d elapsed=%.2fs",
        output_path,
        report.height,
        elapsed_seconds,
        extra={
            "event": "analysis_report_write",
            "path": output_path,
            "rows": report.height,
            "elapsed_seconds": elapsed_seconds,
        },
    )
    return output_path


def _build_aal(losses: pl.LazyFrame) -> pl.LazyFrame:
    keys = ["forecast_date", "metric", "base_model", "rollup_lob", "rollup_peril"]
    return (
        losses.group_by(keys)
        .agg(
            pl.col("loss").sum().alias("total_loss"),
            pl.col("n_simulations").first().alias("n_simulations"),
        )
        .with_columns(
            pl.lit("AAL").alias("ep_type"),
            pl.lit(0).cast(pl.Int64).alias("return_period"),
            pl.lit(0).cast(pl.Int64).alias("rank"),
            pl.lit(0.0).alias("rp"),
            (pl.col("total_loss") / pl.col("n_simulations")).alias("loss"),
        )
        .select(
            "forecast_date",
            "metric",
            "ep_type",
            "return_period",
            "base_model",
            "rollup_lob",
            "rollup_peril",
            "rank",
            "rp",
            "loss",
        )
    )


def _build_ranked_ep(
    losses: pl.LazyFrame,
    *,
    ep_type: str,
    aggregation: str,
    config: RollupConfig,
) -> pl.LazyFrame:
    keys = ["forecast_date", "metric", "base_model", "rollup_lob", "rollup_peril"]
    annual = losses.group_by(*keys, "year_id").agg(
        _aggregation_expr(aggregation).alias("loss"),
        pl.col("n_simulations").first().alias("n_simulations"),
    )
    ranked = annual.with_columns(
        pl.col("loss")
        .rank(method="ordinal", descending=True)
        .over(keys)
        .cast(pl.Int64)
        .alias("rank")
    )
    targets = _target_ranks(config)
    key_targets = (
        losses.select(*keys, "n_simulations")
        .unique()
        .join(targets, on="base_model", how="inner")
    )

    return (
        key_targets.join(
            ranked.select(*keys, "rank", "loss"),
            on=[*keys, "rank"],
            how="left",
        )
        .with_columns(
            pl.lit(ep_type).alias("ep_type"),
            (pl.col("n_simulations") / pl.col("rank")).alias("rp"),
            pl.col("loss").fill_null(0.0),
        )
        .select(
            "forecast_date",
            "metric",
            "ep_type",
            "return_period",
            "base_model",
            "rollup_lob",
            "rollup_peril",
            "rank",
            "rp",
            "loss",
        )
    )


def _aggregation_expr(aggregation: str) -> pl.Expr:
    if aggregation == "sum":
        return pl.col("loss").sum()
    if aggregation == "max":
        return pl.col("loss").max()
    raise ValueError(f"unknown aggregation: {aggregation}")


def _target_ranks(config: RollupConfig) -> pl.LazyFrame:
    rows = [
        {
            "base_model": base_model,
            "return_period": return_period,
            "rank": round(n_simulations / return_period),
        }
        for base_model, n_simulations in config.analysis.simulation_counts.items()
        for return_period in config.analysis.return_periods
    ]
    return pl.DataFrame(rows).lazy()
