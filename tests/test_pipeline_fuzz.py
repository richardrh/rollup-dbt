from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rollup.columns import Col, RawCol
from rollup.pipeline import (
    EpSummaryValidationResult,
    PipelineValidationInputs,
    SeedValidationResult,
    YltFrames,
    YltValidationResult,
    _write_combined_outputs,
    input_ylt_aal_by_lob_peril_summary,
    modelled_dimension_coverage_report,
)


small_name = st.text(alphabet="ABCDEF012345", min_size=1, max_size=6)
small_loss = st.floats(min_value=0.0, max_value=1_000_000.0, allow_nan=False, allow_infinity=False)


def _valid_report() -> pl.DataFrame:
    return pl.DataFrame({"valid": [True], "error": [None]})


def _seed_result(lobs: list[str], perils: list[str]) -> SeedValidationResult:
    return SeedValidationResult(
        frames={
            "lobs.csv": pl.DataFrame(
                {
                    Col.modelled_lob: lobs,
                    Col.rollup_lob: [f"Rollup {value}" for value in lobs],
                    Col.cds_cat_class_name: ["Class"] * len(lobs),
                    Col.class_: ["CLASS"] * len(lobs),
                    Col.office: ["Office"] * len(lobs),
                    Col.currency: ["GBP"] * len(lobs),
                }
            ),
            "perils.csv": pl.DataFrame(
                {
                    Col.modelled_peril: perils,
                    Col.rollup_peril: [f"Rollup {value}" for value in perils],
                    "region": ["Region"] * len(perils),
                    "peril": ["Peril"] * len(perils),
                    Col.region_peril_id: list(range(1, len(perils) + 1)),
                    Col.blend_subregion_peril_id: [str(value) for value in range(1, len(perils) + 1)],
                    Col.base_model: ["verisk"] * len(perils),
                    Col.selection_priority: [1] * len(perils),
                    Col.is_dialsup: [1] * len(perils),
                    Col.is_euws: [0] * len(perils),
                }
            ),
        },
        report=_valid_report(),
    )


@pytest.mark.fuzz
@settings(max_examples=25, deadline=None)
@given(
    lobs=st.lists(small_name, min_size=1, max_size=6, unique=True),
    perils=st.lists(small_name, min_size=1, max_size=6, unique=True),
)
def test_fuzz_coverage_has_no_errors_when_dimensions_exist(
    lobs: list[str],
    perils: list[str],
) -> None:
    ylt = YltValidationResult(
        frames=YltFrames(
            verisk=pl.DataFrame(
                {
                    RawCol.CatalogTypeCode: ["STC"] * len(lobs),
                    RawCol.ExposureAttribute: lobs,
                    RawCol.Analysis: [perils[index % len(perils)] for index in range(len(lobs))],
                }
            ).lazy(),
            risklink=pl.DataFrame({RawCol.anlsid: [1]}).lazy(),
        ),
        report=_valid_report(),
    )
    ep_summaries = EpSummaryValidationResult(
        frame=pl.DataFrame(
            {
                Col.modelled_lob: lobs,
                Col.modelled_peril: [perils[index % len(perils)] for index in range(len(lobs))],
            }
        ).lazy(),
        report=_valid_report(),
    )

    report = modelled_dimension_coverage_report(_seed_result(lobs, perils), ylt, ep_summaries)

    assert report.is_empty()


def _aal_inputs(losses: list[float], unmapped_losses: list[float]) -> PipelineValidationInputs:
    rows = {
        RawCol.CatalogTypeCode: ["STC"] * (len(losses) + len(unmapped_losses)),
        RawCol.ExposureAttribute: ["LOB_A"] * len(losses) + ["LOB_UNMAPPED"] * len(unmapped_losses),
        RawCol.Analysis: ["PERIL_A"] * len(losses) + ["PERIL_UNMAPPED"] * len(unmapped_losses),
        RawCol.GroundUpLoss: losses + unmapped_losses,
    }
    return PipelineValidationInputs(
        seeds=_seed_result(["LOB_A"], ["PERIL_A"]),
        ylts=YltValidationResult(
            frames=YltFrames(
                verisk=pl.DataFrame(rows).lazy(),
                risklink=pl.DataFrame(schema={RawCol.anlsid: pl.Int64, RawCol.loss: pl.Float64}).lazy(),
            ),
            report=_valid_report(),
        ),
        ep_summaries=EpSummaryValidationResult(
            frame=pl.DataFrame(
                {
                    Col.vendor: ["verisk"],
                    Col.analysis_id: ["analysis"],
                    Col.modelled_lob: ["LOB_A"],
                    Col.modelled_peril: ["PERIL_A"],
                    Col.ep_type: ["AAL"],
                    Col.return_period: [0],
                    Col.loss: [1.0],
                }
            ).lazy(),
            report=_valid_report(),
        ),
        coverage_report=pl.DataFrame(schema={"severity": pl.String, "valid": pl.Boolean}),
    )


def _verisk_aal(report: pl.DataFrame) -> dict[str, object]:
    return report.filter(pl.col(Col.vendor) == "verisk").row(0, named=True)


@pytest.mark.fuzz
@settings(max_examples=25, deadline=None)
@given(
    base_losses=st.lists(small_loss, min_size=1, max_size=8),
    added_losses=st.lists(small_loss, min_size=0, max_size=8),
    unmapped_losses=st.lists(small_loss, min_size=0, max_size=8),
)
def test_fuzz_input_ylt_aal_is_additive_and_ignores_unmapped_rows(
    base_losses: list[float],
    added_losses: list[float],
    unmapped_losses: list[float],
) -> None:
    base = _verisk_aal(input_ylt_aal_by_lob_peril_summary(_aal_inputs(base_losses, [])))
    expanded = _verisk_aal(
        input_ylt_aal_by_lob_peril_summary(_aal_inputs(base_losses + added_losses, unmapped_losses))
    )

    assert expanded[Col.row_count] == len(base_losses) + len(added_losses)
    assert expanded[Col.loss_sum] == pytest.approx(sum(base_losses) + sum(added_losses))
    assert expanded["raw_aal"] >= base["raw_aal"]


@pytest.mark.fuzz
@settings(max_examples=25, deadline=None)
@given(losses=st.lists(small_loss, min_size=1, max_size=10))
def test_fuzz_wide_output_preserves_total_loss(losses: list[float]) -> None:
    ylt = pl.DataFrame(
        {
            Col.vendor: ["verisk"] * len(losses),
            Col.base_model: ["verisk"] * len(losses),
            Col.region_peril_id: [1] * len(losses),
            Col.rollup_peril: ["PERIL_A"] * len(losses),
            Col.rollup_lob: ["LOB_A"] * len(losses),
            Col.cds_cat_class_name: ["Class"] * len(losses),
            Col.model_code: [1] * len(losses),
            Col.year_id: list(range(1, len(losses) + 1)),
            Col.event_id: list(range(1, len(losses) + 1)),
            Col.model_event_id: list(range(1, len(losses) + 1)),
            Col.event_day: [1] * len(losses),
            Col.target_currency: ["GBP"] * len(losses),
            Col.metric: ["euws_override"] * len(losses),
            Col.forecast_date: [date(2026, 1, 1)] * len(losses),
            Col.loss: losses,
            Col.risklink_blended_contribution: [10.0] * len(losses),
            Col.verisk_blended_contribution: [20.0] * len(losses),
            Col.uplift_factor_on_base_model: [1.5] * len(losses),
        }
    )
    dialsup = ylt.drop(
        Col.risklink_blended_contribution,
        Col.verisk_blended_contribution,
        Col.uplift_factor_on_base_model,
    ).with_columns(pl.lit("dialsup_gbp_forecast").alias(Col.metric))

    with TemporaryDirectory() as temp_dir:
        _write_combined_outputs(Path(temp_dir), ylt, dialsup)
        wide = pl.read_parquet(Path(temp_dir) / "mts_tbl_ylt_combined_all_factors_wide.parquet")

    loss_columns = ["euws_override_202601_loss", "dialsup_gbp_forecast_202601_loss"]
    assert set(loss_columns).issubset(wide.columns)
    assert Col.risklink_blended_contribution in wide.columns
    assert Col.verisk_blended_contribution in wide.columns
    assert Col.uplift_factor_on_base_model in wide.columns
    assert "rl_blended_contribution" not in wide.columns
    assert "vk_blended_contribution" not in wide.columns
    assert wide.select(pl.sum_horizontal(*loss_columns).sum()).item() == pytest.approx(sum(losses) * 2)
