from __future__ import annotations
# mypy: ignore-errors

from pathlib import Path

import polars as pl

from rollup.columns import Col, RawCol
from rollup.pipeline_types import PipelineValidationInputs
from rollup.pipeline_utils import _verisk_string
from rollup.sources.catalog import load_seed_frames
from rollup.sources.ylt import load_ylt_frames
from rollup.staging.stg_ep_summaries import load_ep_summaries, enrich_ep_summaries, select_main_ep_summaries


_INPUT_YLT_AAL_SUMMARY_SCHEMA = ['vendor', 'rollup_lob', 'rollup_peril', 'modelled_lob', 'modelled_peril', 'row_count', 'loss_sum', 'simulation_count', 'raw_aal']


def empty_modelled_dimension_coverage_report() -> pl.DataFrame:
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


def ylt_loss_validation_summary(data_root: Path | str = "data") -> pl.DataFrame:
    return pl.DataFrame(schema={"valid": pl.Boolean, "error": pl.String})


def _coverage_rows(groups: pl.LazyFrame, **values: object) -> pl.LazyFrame:
    if "message" in values and "error" not in values:
        values["error"] = values["message"]
    values = {key: str(value) if isinstance(value, Path) else value for key, value in values.items()}
    return groups.with_columns(
        *(pl.lit(value).alias(key) for key, value in values.items())
    )


def modelled_dimension_coverage_report(
    seeds: dict[str, pl.LazyFrame],
    ylt: dict[str, pl.LazyFrame],
    ep_summaries: pl.LazyFrame,
    data_root: Path | str = "data",
) -> pl.DataFrame:
    if "lobs" not in seeds or "perils" not in seeds:
        return empty_modelled_dimension_coverage_report()
    data_root = Path(data_root)
    seed_lobs = seeds["lobs"].select(Col.modelled_lob).unique()
    seed_perils = seeds["perils"].select(Col.modelled_peril).unique()
    reports: list[pl.LazyFrame] = []

    sources = [
        (
            "verisk_ylt",
            ylt["verisk"].filter(_verisk_string(RawCol.CatalogTypeCode) == "STC").select(
                _verisk_string(RawCol.ExposureAttribute).alias(Col.modelled_lob),
                _verisk_string(RawCol.Analysis).alias(Col.modelled_peril),
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
            missing = (
                source.select(pl.col(dimension).cast(pl.String).alias("value"))
                .drop_nulls()
                .unique()
                .join(seed_values.select(pl.col(dimension).cast(pl.String).alias("value")), on="value", how="anti")
                .with_columns(pl.len().over("value").alias("count"))
            )
            reports.append(
                _coverage_rows(
                    missing,
                    severity="error",
                    direction="input_missing_from_seed",
                    source_group=source_group,
                    dimension=dimension,
                    field=dimension,
                    path=data_root,
                    message=f"{dimension} value is missing from seed file",
                )
            )
    if not reports:
        return empty_modelled_dimension_coverage_report()
    report = pl.concat(reports, how="diagonal_relaxed").collect()
    if report.is_empty():
        return empty_modelled_dimension_coverage_report()
    return report.sort(["severity", "direction", "source_group", "dimension", "value"])


def ensure_modelled_dimension_coverage(report: pl.DataFrame) -> None:
    if "severity" in report.columns and report.filter(pl.col("severity") == "error").height:
        raise ValueError("rollup validate failed: modelled LOB/peril coverage validation failed")


def load_pipeline_validation_inputs(data_root: Path | str = "data") -> PipelineValidationInputs:
    seeds = load_seed_frames(data_root)
    ylts = load_ylt_frames(data_root)
    ep_summaries = load_ep_summaries(data_root)
    coverage_report = modelled_dimension_coverage_report(seeds, ylts, ep_summaries, data_root)
    return PipelineValidationInputs(
        seeds=seeds,
        ylts=ylts,
        ep_summaries=ep_summaries,
        coverage_report=coverage_report,
    )


def ensure_pipeline_validation_inputs(inputs: PipelineValidationInputs) -> None:
    if _validation_has_blocking_errors(inputs):
        raise ValueError("pipeline input validation failed")


def _validation_has_blocking_errors(inputs: PipelineValidationInputs) -> bool:
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


def empty_input_ylt_aal_by_lob_peril_summary() -> pl.DataFrame:
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


def input_ylt_aal_by_lob_peril_summary(inputs: PipelineValidationInputs) -> pl.DataFrame:
    if _validation_has_blocking_errors(inputs):
        return empty_input_ylt_aal_by_lob_peril_summary()

    if "lobs" not in inputs.seeds or "perils" not in inputs.seeds:
        return empty_input_ylt_aal_by_lob_peril_summary()

    lobs = inputs.seeds["lobs"].select(
        Col.modelled_lob,
        Col.rollup_lob,
    )
    perils = inputs.seeds["perils"].select(
        Col.modelled_peril,
        Col.rollup_peril,
    )
    verisk = (
        inputs.ylts["verisk"].filter(_verisk_string(RawCol.CatalogTypeCode) == "STC")
        .select(
            pl.lit("verisk").alias(Col.vendor),
            _verisk_string(RawCol.ExposureAttribute).alias(Col.modelled_lob),
            _verisk_string(RawCol.Analysis).alias(Col.modelled_peril),
            pl.col(RawCol.GroundUpLoss).cast(pl.Float64).alias(Col.loss),
        )
        .join(lobs, on=Col.modelled_lob, how="inner")
        .join(perils, on=Col.modelled_peril, how="inner")
    )

    staged_ep_summaries = select_main_ep_summaries(enrich_ep_summaries(inputs.ep_summaries, inputs.seeds))
    risklink_lookup = (
        staged_ep_summaries.filter(pl.col(Col.vendor) == "risklink")
        .select(
            Col.vendor,
            Col.analysis_id,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.rollup_lob,
            Col.rollup_peril,
        )
        .unique()
    )
    risklink = (
        inputs.ylts["risklink"].select(
            pl.lit("risklink").alias(Col.vendor),
            pl.col(RawCol.anlsid).cast(pl.String).alias(Col.analysis_id),
            pl.col(RawCol.loss).cast(pl.Float64).alias(Col.loss),
        )
        .join(risklink_lookup, on=[Col.vendor, Col.analysis_id], how="inner")
        .select(
            Col.vendor,
            Col.rollup_lob,
            Col.rollup_peril,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.loss,
        )
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
