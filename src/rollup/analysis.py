from __future__ import annotations

from pathlib import Path

import polars as pl

from rollup.columns import Col
from rollup.config import RollupConfig, load_config


def build_ep_report(
    output_root: Path | str = "output",
    *,
    config: RollupConfig | None = None,
    config_path: str | Path | None = None,
) -> pl.DataFrame:
    config = config or load_config(config_path)
    output_root = Path(output_root)
    losses = _losses(output_root, config).with_columns(
        pl.col(Col.base_model)
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
    return pl.concat([aal, ep], how="vertical").sort(
        Col.forecast_date,
        Col.metric,
        Col.ep_type,
        Col.return_period,
        Col.base_model,
        Col.rollup_lob,
        Col.rollup_peril,
    )


def write_ep_report(
    output_root: Path | str = "output",
    *,
    config: RollupConfig | None = None,
    config_path: str | Path | None = None,
) -> Path:
    config = config or load_config(config_path)
    output_root = Path(output_root)
    output_path = config.outputs.analysis_path(output_root) / config.outputs.ep_report_file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    build_ep_report(output_root, config=config).write_csv(output_path)
    return output_path


def _losses(output_root: Path, config: RollupConfig) -> pl.LazyFrame:
    marts = config.outputs.marts_path(output_root)
    main = pl.scan_parquet(marts / config.outputs.combined_file).filter(
        pl.col(Col.metric) == "euws_override"
    )
    dialsup_path = marts / config.outputs.dialsup_file
    if dialsup_path.exists():
        dialsup = pl.scan_parquet(dialsup_path)
        return pl.concat([main, dialsup], how="diagonal_relaxed")
    return main


def _build_aal(losses: pl.LazyFrame) -> pl.DataFrame:
    keys = [Col.forecast_date, Col.metric, Col.base_model, Col.rollup_lob, Col.rollup_peril]
    return losses.group_by(keys).agg(
        pl.col(Col.loss).sum().alias("total_loss"),
        pl.col("n_simulations").first().alias("n_simulations"),
    ).with_columns(
        pl.lit("AAL").alias(Col.ep_type),
        pl.lit(0).cast(pl.Int64).alias(Col.return_period),
        pl.lit(0).cast(pl.Int64).alias("rank"),
        pl.lit(0.0).alias(Col.rp),
        (pl.col("total_loss") / pl.col("n_simulations")).alias(Col.loss),
    ).select(_REPORT_COLUMNS).collect()


def _build_ranked_ep(
    losses: pl.LazyFrame,
    *,
    ep_type: str,
    aggregation: str,
    config: RollupConfig,
) -> pl.DataFrame:
    keys = [Col.forecast_date, Col.metric, Col.base_model, Col.rollup_lob, Col.rollup_peril]
    annual = losses.group_by(*keys, Col.year_id).agg(
        _aggregation_expr(aggregation).alias(Col.loss),
        pl.col("n_simulations").first().alias("n_simulations"),
    )
    ranked = annual.with_columns(
        pl.col(Col.loss).rank(method="ordinal", descending=True).over(keys).cast(pl.Int64).alias("rank")
    )
    targets = _target_ranks(config)
    key_targets = losses.select(*keys, "n_simulations").unique().join(targets, on=Col.base_model)
    return key_targets.join(ranked.select(*keys, "rank", Col.loss), on=[*keys, "rank"], how="left").with_columns(
        pl.lit(ep_type).alias(Col.ep_type),
        (pl.col("n_simulations") / pl.col("rank")).alias(Col.rp),
        pl.col(Col.loss).fill_null(0.0),
    ).select(_REPORT_COLUMNS).collect()


def _aggregation_expr(aggregation: str) -> pl.Expr:
    if aggregation == "sum":
        return pl.col(Col.loss).sum()
    if aggregation == "max":
        return pl.col(Col.loss).max()
    raise ValueError(f"unknown aggregation: {aggregation}")


def _target_ranks(config: RollupConfig) -> pl.LazyFrame:
    rows = [
        {
            Col.base_model: base_model,
            Col.return_period: return_period,
            "rank": max(1, round(simulation_count / return_period)),
        }
        for base_model, simulation_count in config.analysis.simulation_counts.items()
        for return_period in config.analysis.return_periods
    ]
    return pl.DataFrame(rows).lazy()


_REPORT_COLUMNS = [
    Col.forecast_date,
    Col.metric,
    Col.ep_type,
    Col.return_period,
    Col.base_model,
    Col.rollup_lob,
    Col.rollup_peril,
    "rank",
    Col.rp,
    Col.loss,
]
