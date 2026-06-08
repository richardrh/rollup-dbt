from __future__ import annotations

import logging
import time
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

from rollup.config import load_config
cfg = load_config()


def build_ep_report(output_root: Path | str = "output") -> pl.DataFrame:
    output_root = Path(output_root)

    losses = pl.concat(
        [
            _main_losses(output_root / "mts_tbl_ylt_combined_all_factors.parquet"),
            _dialsup_losses(output_root / "mts_tbl_ylt_dialsup.parquet"),
        ],
        how="vertical",
    ).with_columns(
        pl.col("base_model")
        .replace_strict(SIMULATION_COUNTS, return_dtype=pl.Int64)
        .alias("n_simulations")
    )

    aal = _build_aal(losses)
    ep = pl.concat(
        [
            _build_ranked_ep(losses, ep_type="AEP", aggregation="sum"),
            _build_ranked_ep(losses, ep_type="OEP", aggregation="max"),
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


def write_ep_report(output_root: Path | str = "output") -> Path:
    output_root = Path(output_root)
    output_path = output_root / "analysis" / "ep_report.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    logger.info("writing output=%s", output_path)
    report = build_ep_report(output_root)
    report.write_csv(output_path)
    logger.info(
        "wrote output=%s rows=%d elapsed=%.2fs",
        output_path,
        report.height,
        time.perf_counter() - started,
    )
    return output_path


def _main_losses(path: Path) -> pl.LazyFrame:
    return pl.scan_parquet(path).filter(
        pl.col("metric") == "euws_override"
    ).select(
        "forecast_date",
        "base_model",
        "rollup_lob",
        "rollup_peril",
        "year_id",
        "metric",
        "loss",
    )


def _dialsup_losses(path: Path) -> pl.LazyFrame:
    return pl.scan_parquet(path).filter(
        pl.col("metric") == "dialsup_gbp_forecast"
    ).select(
        "forecast_date",
        "base_model",
        "rollup_lob",
        "rollup_peril",
        "year_id",
        "metric",
        "loss",
    )


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
    targets = _target_ranks()
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


def _target_ranks() -> pl.LazyFrame:
    rows = [
        {
            "base_model": base_model,
            "return_period": return_period,
            "rank": round(n_simulations / return_period),
        }
        for base_model, n_simulations in SIMULATION_COUNTS.items()
        for return_period in RETURN_PERIODS
    ]
    return pl.DataFrame(rows).lazy()
