from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from rollup.analysis import write_ep_report
from rollup.ep_summary_generator import generate_vendor_ep_summary
from rollup.pipeline import (
    PipelineValidationInputs,
    empty_input_ylt_aal_by_lob_peril_summary,
    input_ylt_aal_by_lob_peril_summary,
    load_pipeline_validation_inputs,
    run,
    ylt_loss_validation_summary,
)


@dataclass(frozen=True)
class RollupValidationResult:
    data_root: Path
    is_valid: bool
    validation_report: pl.DataFrame
    coverage_report: pl.DataFrame
    ylt_loss_report: pl.DataFrame
    input_ylt_aal_report: pl.DataFrame

    def raise_for_errors(self) -> None:
        if not self.is_valid:
            raise RollupValidationError(self)


@dataclass(frozen=True)
class RollupOutputPaths:
    mts_combined: Path
    mts_wide: Path
    mts_dialsup: Path
    event_validation: Path
    marts_dir: Path
    mart_files: tuple[Path, ...]
    debug_dir: Path | None = None


@dataclass(frozen=True)
class RollupRunResult:
    data_root: Path
    output_root: Path
    validation: RollupValidationResult
    outputs: RollupOutputPaths
    ep_report_path: Path | None


class RollupValidationError(ValueError):
    def __init__(self, validation: RollupValidationResult) -> None:
        self.validation = validation
        invalid_count = validation.validation_report.filter(~pl.col("valid")).height
        coverage_error_count = validation.coverage_report.filter(
            pl.col("severity") == "error"
        ).height
        super().__init__(
            "Rollup input validation failed "
            f"({invalid_count} invalid file(s), {coverage_error_count} coverage error(s))"
        )


@dataclass(frozen=True)
class _ValidationBundle:
    inputs: PipelineValidationInputs
    result: RollupValidationResult


def validate_rollup_inputs(data_root: str | Path = "data") -> RollupValidationResult:
    return _collect_validation(data_root).result


def run_rollup(
    data_root: str | Path = "data",
    output_root: str | Path = "output",
    *,
    debug: bool = False,
    validate: bool = True,
    write_analysis: bool = True,
    validation_callback: Callable[[RollupValidationResult], None] | None = None,
) -> RollupRunResult:
    data_root = Path(data_root)
    output_root = Path(output_root)
    validation_bundle = _collect_validation(data_root)
    validation = validation_bundle.result
    if validation_callback is not None:
        validation_callback(validation)
    if validate:
        validation.raise_for_errors()

    run(
        data_root,
        output_root=output_root,
        debug=debug,
        validation_inputs=validation_bundle.inputs,
    )
    ep_report_path = write_ep_report(output_root) if write_analysis else None
    return RollupRunResult(
        data_root=data_root,
        output_root=output_root,
        validation=validation,
        outputs=collect_output_paths(output_root, debug=debug),
        ep_report_path=ep_report_path,
    )


def build_ep_report(output_root: str | Path = "output") -> Path:
    return write_ep_report(output_root)


def generate_ep_summary(
    data_root: str | Path,
    vendor: str,
    csv_path: str | Path,
    *,
    status_callback: Callable[[str], None] | None = None,
) -> Path:
    return generate_vendor_ep_summary(
        data_root,
        vendor,
        csv_path,
        status_callback=status_callback,
    )


def collect_output_paths(
    output_root: str | Path = "output",
    *,
    debug: bool = False,
) -> RollupOutputPaths:
    output_root = Path(output_root)
    marts_dir = output_root / "marts"
    return RollupOutputPaths(
        mts_combined=output_root / "mts_tbl_ylt_combined_all_factors.parquet",
        mts_wide=output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet",
        mts_dialsup=output_root / "mts_tbl_ylt_dialsup.parquet",
        event_validation=output_root / "mts_event_validation.parquet",
        marts_dir=marts_dir,
        mart_files=tuple(sorted(marts_dir.glob("*.parquet"))),
        debug_dir=output_root / "debug" if debug else None,
    )


def _collect_validation(data_root: str | Path = "data") -> _ValidationBundle:
    data_root = Path(data_root)
    inputs = load_pipeline_validation_inputs(data_root)
    validation_report = _validation_report(inputs)
    try:
        ylt_loss_report = ylt_loss_validation_summary(data_root)
    except Exception as exc:
        ylt_loss_report = pl.DataFrame(
            [{"valid": False, "error": f"YLT loss validation summary failed: {exc}"}]
        )
    try:
        input_ylt_aal_report = input_ylt_aal_by_lob_peril_summary(inputs)
    except Exception:
        input_ylt_aal_report = empty_input_ylt_aal_by_lob_peril_summary()

    result = RollupValidationResult(
        data_root=data_root,
        is_valid=_is_valid(validation_report, inputs.coverage_report),
        validation_report=validation_report,
        coverage_report=inputs.coverage_report,
        ylt_loss_report=ylt_loss_report,
        input_ylt_aal_report=input_ylt_aal_report,
    )
    return _ValidationBundle(inputs=inputs, result=result)


def _validation_report(inputs: PipelineValidationInputs) -> pl.DataFrame:
    report_parts = [
        inputs.seeds.report.with_columns(pl.lit("seeds").alias("source_group")),
        inputs.ylts.report.with_columns(pl.lit("ylt").alias("source_group")),
        inputs.ep_summaries.report.with_columns(pl.lit("ep_summaries").alias("source_group")),
    ]
    return pl.concat(
        [part.with_columns(pl.col("error").cast(pl.String)) for part in report_parts],
        how="diagonal",
    )


def _is_valid(validation_report: pl.DataFrame, coverage_report: pl.DataFrame) -> bool:
    invalid_count = validation_report.filter(~pl.col("valid")).height
    coverage_error_count = coverage_report.filter(pl.col("severity") == "error").height
    return invalid_count == 0 and coverage_error_count == 0
