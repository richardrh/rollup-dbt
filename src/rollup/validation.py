from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from rollup.columns import Col, RawCol
from rollup.intermediate import (
    int_ep_summaries_enriched,
    int_ep_summaries_main,
    int_ylt_enriched,
    int_ylt_normalized,
)
from rollup.sources import ep_summaries, seeds, ylt
from rollup.staging import stg_risklink_ylt, stg_verisk_ylt

_INPUT_YLT_AAL_SUMMARY_SCHEMA = [
    "vendor",
    "rollup_lob",
    "rollup_peril",
    "modelled_lob",
    "modelled_peril",
    "row_count",
    "loss_sum",
    "simulation_count",
    "raw_aal",
]
_REQUIRED_SEED_STEMS = (
    "lobs",
    "perils",
    "verisk_events",
    "risklink_flood22_model_events",
    "fx_rates",
    "forecast_factors",
    "euws_rate_factors",
    "euws_rank_overrides",
    "blending_factors",
)


@dataclass(frozen=True)
class ValidationInputs:
    seeds: dict[str, pl.LazyFrame]
    ylts: dict[str, pl.LazyFrame]
    ep_summaries: pl.LazyFrame
    coverage_report: pl.DataFrame


def _empty_modelled_dimension_coverage_report() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "severity": pl.String,
            "direction": pl.String,
            "source_group": pl.String,
            "dimension": pl.String,
            "field": pl.String,
            "path": pl.String,
            "value": pl.String,
            "count": pl.Int64,
            "message": pl.String,
            "error": pl.String,
        }
    )


def modelled_dimension_coverage_report(
    seeds: dict[str, pl.LazyFrame],
    ylt: dict[str, pl.LazyFrame],
    ep_summaries: pl.LazyFrame,
    data_root: Path | str = "data",
) -> pl.DataFrame:
    if "lobs" not in seeds or "perils" not in seeds:
        missing = [key for key in ("lobs", "perils") if key not in seeds]
        raise ValueError(
            "modelled dimension coverage requires seed mappings: " + ", ".join(missing)
        )
    data_root = Path(data_root)
    seed_lobs = seeds["lobs"].select(Col.modelled_lob).unique()
    seed_perils = seeds["perils"].select(Col.modelled_peril).unique()
    reports: list[pl.LazyFrame] = []

    sources = [
        (
            "verisk_ylt",
            ylt["verisk"]
            .filter(
                pl.col(RawCol.CatalogTypeCode).cast(pl.String).str.strip_chars()
                == "STC"
            )
            .select(
                pl.col(RawCol.ExposureAttribute)
                .cast(pl.String)
                .str.strip_chars()
                .alias(Col.modelled_lob),
                pl.col(RawCol.Analysis)
                .cast(pl.String)
                .str.strip_chars()
                .alias(Col.modelled_peril),
            ),
        ),
        (
            "ep_summaries",
            ep_summaries.select(Col.modelled_lob, Col.modelled_peril),
        ),
    ]
    for source_group, source in sources:
        for dimension, seed_values in (
            (Col.modelled_lob, seed_lobs),
            (Col.modelled_peril, seed_perils),
        ):
            missing_values = (
                source.select(pl.col(dimension).cast(pl.String).alias("value"))
                .drop_nulls()
                .unique()
                .join(
                    seed_values.select(
                        pl.col(dimension).cast(pl.String).alias("value")
                    ),
                    on="value",
                    how="anti",
                )
                .with_columns(pl.len().over("value").alias("count"))
            )
            message = f"{dimension} value is missing from seed file"
            reports.append(
                missing_values.with_columns(
                    pl.lit("error").alias("severity"),
                    pl.lit("input_missing_from_seed").alias("direction"),
                    pl.lit(source_group).alias("source_group"),
                    pl.lit(dimension).alias("dimension"),
                    pl.lit(dimension).alias("field"),
                    pl.lit(str(data_root)).alias("path"),
                    pl.lit(message).alias("message"),
                    pl.lit(message).alias("error"),
                )
            )
    report = pl.concat(reports, how="diagonal_relaxed").collect()
    if report.is_empty():
        return _empty_modelled_dimension_coverage_report()
    return report.sort(["severity", "direction", "source_group", "dimension", "value"])


def inspect_inputs(
    data_root: Path | str = "data",
) -> ValidationInputs:
    seed_frames = seeds.load(data_root)
    validate_required_seed_inventory(seed_frames)
    ylt_frames = ylt.load(data_root)
    ep_summary_frame = ep_summaries.load(data_root)
    coverage_report = modelled_dimension_coverage_report(
        seed_frames, ylt_frames, ep_summary_frame, data_root
    )
    return ValidationInputs(
        seeds=seed_frames,
        ylts=ylt_frames,
        ep_summaries=ep_summary_frame,
        coverage_report=coverage_report,
    )


def validate_required_seed_inventory(seeds: dict[str, pl.LazyFrame]) -> None:
    missing = [stem for stem in _REQUIRED_SEED_STEMS if stem not in seeds]
    if missing:
        raise ValueError("missing required seed files: " + ", ".join(missing))


def validate_inputs(inputs: ValidationInputs) -> None:
    if _validation_has_blocking_errors(inputs):
        raise ValueError("pipeline input validation failed")


def _validation_has_blocking_errors(inputs: ValidationInputs) -> bool:
    return inputs.coverage_report.filter(pl.col("severity") == "error").height > 0


def _input_ylt_aal_group(frame: pl.LazyFrame, simulation_count: int) -> pl.LazyFrame:
    keys = [
        Col.vendor,
        Col.rollup_lob,
        Col.rollup_peril,
        Col.modelled_lob,
        Col.modelled_peril,
    ]
    return (
        frame.group_by(keys)
        .agg(
            pl.len().cast(pl.Int64).alias(Col.row_count),
            pl.col(Col.loss).sum().cast(pl.Float64).alias(Col.loss_sum),
        )
        .with_columns(
            pl.lit(simulation_count).cast(pl.Int64).alias("simulation_count"),
            (pl.col(Col.loss_sum) / simulation_count).cast(pl.Float64).alias("raw_aal"),
        )
        .select(*_INPUT_YLT_AAL_SUMMARY_SCHEMA)
    )


def _empty_input_ylt_aal_by_lob_peril_summary() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            Col.vendor: pl.String,
            Col.rollup_lob: pl.String,
            Col.rollup_peril: pl.String,
            Col.modelled_lob: pl.String,
            Col.modelled_peril: pl.String,
            Col.row_count: pl.Int64,
            Col.loss_sum: pl.Float64,
            "simulation_count": pl.Int64,
            "raw_aal": pl.Float64,
        }
    )


def input_ylt_aal_by_lob_peril_summary(
    inputs: ValidationInputs,
) -> pl.DataFrame:
    if _validation_has_blocking_errors(inputs):
        return _empty_input_ylt_aal_by_lob_peril_summary()

    if "lobs" not in inputs.seeds or "perils" not in inputs.seeds:
        missing = [key for key in ("lobs", "perils") if key not in inputs.seeds]
        raise ValueError(
            "input YLT AAL summary requires seed mappings: " + ", ".join(missing)
        )

    lobs = inputs.seeds["lobs"].select(Col.modelled_lob, Col.rollup_lob)
    perils = inputs.seeds["perils"].select(Col.modelled_peril, Col.rollup_peril)
    verisk_ylt = stg_verisk_ylt.Model.transform(inputs.ylts["verisk"])
    risklink_ylt = stg_risklink_ylt.Model.transform(inputs.ylts["risklink"])
    normalized_ylt = int_ylt_normalized.Model.transform(verisk_ylt, risklink_ylt)
    selected_ep_summaries = int_ep_summaries_main.Model.transform(
        int_ep_summaries_enriched.Model.transform(inputs.ep_summaries, inputs.seeds)
    )
    enriched_ylt = int_ylt_enriched.Model.transform(
        normalized_ylt, selected_ep_summaries
    )
    verisk = (
        verisk_ylt.join(lobs, on=Col.modelled_lob, how="inner")
        .join(perils, on=Col.modelled_peril, how="inner")
        .select(
            Col.vendor,
            Col.rollup_lob,
            Col.rollup_peril,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.loss,
        )
    )
    risklink = enriched_ylt.filter(pl.col(Col.vendor) == "risklink").select(
        Col.vendor,
        Col.rollup_lob,
        Col.rollup_peril,
        Col.modelled_lob,
        Col.modelled_peril,
        Col.loss,
    )

    return (
        pl.concat(
            [
                _input_ylt_aal_group(verisk, 10_000),
                _input_ylt_aal_group(risklink, 100_000),
            ],
            how="vertical",
        )
        .sort(
            [
                "raw_aal",
                Col.vendor,
                Col.rollup_lob,
                Col.rollup_peril,
                Col.modelled_lob,
                Col.modelled_peril,
            ],
            descending=[True, False, False, False, False, False],
        )
        .collect()
    )
