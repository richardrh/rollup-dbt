from __future__ import annotations

from pathlib import Path

import polars as pl


RETURN_PERIODS = [30, 200, 1000]
SIMULATION_COUNTS = {
    "verisk": 10_000,
    "risklink": 100_000,
}


def build_ep_report(data_root: Path | str = "data") -> pl.DataFrame:
    data_root = Path(data_root)
    output_dir = data_root / "output"

    losses = pl.concat(
        [
            _main_losses(output_dir / "mts_tbl_ylt_combined_all_factors.parquet"),
            _dialsup_losses(output_dir / "mts_tbl_ylt_dialsup.parquet"),
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


def write_ep_report(data_root: Path | str = "data") -> Path:
    data_root = Path(data_root)
    output_path = data_root / "output" / "analysis" / "ep_report.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    build_ep_report(data_root).write_csv(output_path)
    return output_path


def _main_losses(path: Path) -> pl.LazyFrame:
    return pl.scan_parquet(path).select(
        "forecast_date",
        "base_model",
        "rollup_lob",
        "rollup_peril",
        "year_id",
        pl.lit("main").alias("metric"),
        pl.col("original_ylt_loss_blended_gbp_forecast_euws").alias("loss"),
    )


def _dialsup_losses(path: Path) -> pl.LazyFrame:
    return pl.scan_parquet(path).select(
        "forecast_date",
        "base_model",
        "rollup_lob",
        "rollup_peril",
        "year_id",
        pl.lit("dialsup").alias("metric"),
        pl.col("dialsup_loss_gbp_forecast").alias("loss"),
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
