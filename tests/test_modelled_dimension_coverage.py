from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from rollup import validation
from rollup.columns import Col, RawCol
from rollup.validation import (
    ValidationInputs,
    input_ylt_aal_by_lob_peril_summary,
    modelled_dimension_coverage_report,
    validate_required_seed_inventory,
)


def _basic_seed_mapping() -> dict[str, pl.LazyFrame]:
    return {
        "lobs": pl.DataFrame({Col.modelled_lob: ["LOB_A", "LOB_UNUSED"]}).lazy(),
        "perils": pl.DataFrame(
            {Col.modelled_peril: ["PERIL_A", "PERIL_UNUSED"]}
        ).lazy(),
    }


def _coverage_ylt_mapping() -> dict[str, pl.LazyFrame]:
    return {
        "verisk": pl.DataFrame(
            {
                RawCol.CatalogTypeCode: ["STC     ", "STC", "NON_STC"],
                RawCol.ExposureAttribute: ["LOB_A   ", "LOB_MISSING", "LOB_NON_STC"],
                RawCol.Analysis: ["PERIL_A   ", "PERIL_MISSING", "PERIL_NON_STC"],
            }
        ).lazy(),
        "risklink": pl.DataFrame({RawCol.anlsid: [1]}).lazy(),
    }


def _coverage_ep_summaries() -> pl.LazyFrame:
    return pl.DataFrame(
        {
            Col.modelled_lob: ["LOB_A", "EP_LOB_MISSING"],
            Col.modelled_peril: ["PERIL_A", "EP_PERIL_MISSING"],
        }
    ).lazy()


def _summary_seed_mapping() -> dict[str, pl.LazyFrame]:
    return {
        "lobs": pl.DataFrame(
            {
                Col.modelled_lob: ["LOB_A", "LOB_B", "LOB_RISK"],
                Col.rollup_lob: ["Rollup A", "Rollup B", "Rollup Risk"],
                Col.cds_cat_class_name: ["Class A", "Class B", "Class Risk"],
                Col.class_: ["A", "B", "R"],
                Col.office: ["London", "London", "London"],
                Col.currency: ["GBP", "GBP", "GBP"],
            }
        ).lazy(),
        "perils": pl.DataFrame(
            {
                Col.modelled_peril: ["PERIL_A", "PERIL_B", "PERIL_RISK"],
                Col.rollup_peril: ["Rollup EQ", "Rollup WS", "Rollup FL"],
                "region": ["Region", "Region", "Region"],
                "peril": ["EQ", "WS", "FL"],
                Col.region_peril_id: [101, 102, 103],
                Col.blend_subregion_peril_id: ["101", "102", "103"],
                Col.base_model: ["verisk", "verisk", "risklink"],
                Col.selection_priority: [1, 1, 1],
                Col.is_dialsup: [1, 1, 1],
                Col.is_euws: [0, 1, 0],
            }
        ).lazy(),
    }


def _empty_risklink_ylt() -> pl.LazyFrame:
    return pl.DataFrame(
        schema={
            RawCol.anlsid: pl.Int64,
            RawCol.yearid: pl.Int64,
            RawCol.eventid: pl.Int64,
            RawCol.loss: pl.Float64,
        }
    ).lazy()


def _empty_verisk_ylt() -> pl.LazyFrame:
    return pl.DataFrame(
        schema={
            RawCol.CatalogTypeCode: pl.String,
            RawCol.ExposureAttribute: pl.String,
            RawCol.Analysis: pl.String,
            RawCol.ModelCode: pl.Int64,
            RawCol.YearID: pl.Int64,
            RawCol.EventID: pl.Int64,
            RawCol.GroundUpLoss: pl.Float64,
        }
    ).lazy()


def _summary_inputs(
    *,
    verisk: pl.LazyFrame | None = None,
    risklink: pl.LazyFrame | None = None,
    ep_rows: dict[str, list[object]] | None = None,
) -> ValidationInputs:
    ep_frame = pl.DataFrame(
        ep_rows
        or {
            Col.vendor: ["risklink"],
            Col.analysis_id: ["9001"],
            Col.modelled_lob: ["LOB_RISK"],
            Col.modelled_peril: ["PERIL_RISK"],
            Col.ep_type: ["AAL"],
            Col.return_period: [0],
            Col.loss: [1.0],
        }
    ).lazy()
    return ValidationInputs(
        seeds=_summary_seed_mapping(),
        ylts={
            "verisk": verisk if verisk is not None else _empty_verisk_ylt(),
            "risklink": risklink if risklink is not None else _empty_risklink_ylt(),
        },
        ep_summaries=ep_frame,
        coverage_report=pl.DataFrame(
            schema={"severity": pl.String, "valid": pl.Boolean}
        ),
    )


def test_modelled_dimension_coverage_report_returns_only_input_missing_errors() -> None:
    report = modelled_dimension_coverage_report(
        _basic_seed_mapping(),
        _coverage_ylt_mapping(),
        _coverage_ep_summaries(),
    )

    rows = {
        (
            row["severity"],
            row["direction"],
            row["source_group"],
            row["dimension"],
            row["value"],
        )
        for row in report.iter_rows(named=True)
    }

    assert (
        "error",
        "input_missing_from_seed",
        "verisk_ylt",
        Col.modelled_lob,
        "LOB_MISSING",
    ) in rows
    assert (
        "error",
        "input_missing_from_seed",
        "verisk_ylt",
        Col.modelled_peril,
        "PERIL_MISSING",
    ) in rows
    assert (
        "error",
        "input_missing_from_seed",
        "ep_summaries",
        Col.modelled_lob,
        "EP_LOB_MISSING",
    ) in rows
    assert (
        "error",
        "input_missing_from_seed",
        "ep_summaries",
        Col.modelled_peril,
        "EP_PERIL_MISSING",
    ) in rows
    assert {row[0] for row in rows} == {"error"}
    assert {row[1] for row in rows} == {"input_missing_from_seed"}
    assert "LOB_UNUSED" not in set(report["value"])
    assert "PERIL_UNUSED" not in set(report["value"])
    assert "LOB_NON_STC" not in set(report["value"])


@pytest.mark.parametrize("missing_key", ["lobs", "perils"])
def test_modelled_dimension_coverage_report_requires_dimension_seed_mappings(
    missing_key: str,
) -> None:
    seeds = _basic_seed_mapping()
    del seeds[missing_key]

    with pytest.raises(ValueError, match="modelled dimension coverage requires"):
        modelled_dimension_coverage_report(
            seeds,
            _coverage_ylt_mapping(),
            _coverage_ep_summaries(),
        )


def test_input_ylt_aal_summary_computes_verisk_raw_aal_sorted_descending() -> None:
    inputs = _summary_inputs(
        verisk=pl.DataFrame(
            {
                RawCol.CatalogTypeCode: ["STC     ", "STC", "STC", "NON_STC"],
                RawCol.ExposureAttribute: ["LOB_A   ", "LOB_A", "LOB_B", "LOB_B"],
                RawCol.Analysis: ["PERIL_A   ", "PERIL_A", "PERIL_B", "PERIL_B"],
                RawCol.ModelCode: [1, 1, 1, 1],
                RawCol.YearID: [2026, 2026, 2026, 2026],
                RawCol.EventID: [1, 2, 3, 4],
                RawCol.GroundUpLoss: [15_000.0, 5_000.0, 10_000.0, 90_000.0],
            }
        ).lazy()
    )

    report = input_ylt_aal_by_lob_peril_summary(inputs)

    assert list(
        report.filter(pl.col(Col.vendor) == "verisk").iter_rows(named=True)
    ) == [
        {
            Col.vendor: "verisk",
            Col.rollup_lob: "Rollup A",
            Col.rollup_peril: "Rollup EQ",
            Col.modelled_lob: "LOB_A",
            Col.modelled_peril: "PERIL_A",
            Col.row_count: 2,
            Col.loss_sum: 20_000.0,
            "simulation_count": 10_000,
            "raw_aal": 2.0,
        },
        {
            Col.vendor: "verisk",
            Col.rollup_lob: "Rollup B",
            Col.rollup_peril: "Rollup WS",
            Col.modelled_lob: "LOB_B",
            Col.modelled_peril: "PERIL_B",
            Col.row_count: 1,
            Col.loss_sum: 10_000.0,
            "simulation_count": 10_000,
            "raw_aal": 1.0,
        },
    ]


def test_input_ylt_aal_summary_does_not_duplicate_risklink_losses_by_ep_return_period() -> (
    None
):
    inputs = _summary_inputs(
        risklink=pl.DataFrame(
            {
                RawCol.anlsid: [9001, 9001],
                RawCol.yearid: [2026, 2026],
                RawCol.eventid: [1, 2],
                RawCol.loss: [100.0, 200.0],
            }
        ).lazy(),
        ep_rows={
            Col.vendor: ["risklink", "risklink", "risklink"],
            Col.analysis_id: ["9001", "9001", "9001"],
            Col.modelled_lob: ["LOB_RISK", "LOB_RISK", "LOB_RISK"],
            Col.modelled_peril: ["PERIL_RISK", "PERIL_RISK", "PERIL_RISK"],
            Col.ep_type: ["AAL", "OEP", "OEP"],
            Col.return_period: [0, 200, 1000],
            Col.loss: [1.0, 10.0, 20.0],
        },
    )

    report = input_ylt_aal_by_lob_peril_summary(inputs)

    risklink = report.filter(pl.col(Col.vendor) == "risklink")
    assert risklink.height == 1
    assert risklink.item(0, Col.row_count) == 2
    assert risklink.item(0, Col.loss_sum) == 300.0
    assert risklink.item(0, "simulation_count") == 100_000
    assert risklink.item(0, "raw_aal") == 0.003


@pytest.mark.parametrize("missing_key", ["lobs", "perils"])
def test_input_ylt_aal_summary_requires_dimension_seed_mappings(
    missing_key: str,
) -> None:
    inputs = _summary_inputs()
    seeds = dict(inputs.seeds)
    del seeds[missing_key]
    incomplete_inputs = ValidationInputs(
        seeds=seeds,
        ylts=inputs.ylts,
        ep_summaries=inputs.ep_summaries,
        coverage_report=inputs.coverage_report,
    )

    with pytest.raises(ValueError, match="input YLT AAL summary requires"):
        input_ylt_aal_by_lob_peril_summary(incomplete_inputs)


def test_validate_required_seed_inventory_lists_all_missing_requirements() -> None:
    seeds = {"lobs": pl.DataFrame().lazy(), "perils": pl.DataFrame().lazy()}

    try:
        validate_required_seed_inventory(seeds)
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected missing required seeds to raise")

    assert "missing required seed files:" in message
    for missing in (
        "verisk_events",
        "risklink_flood22_model_events",
        "fx_rates",
        "forecast_factors",
        "euws_rate_factors",
        "euws_rank_overrides",
        "blending_factors",
    ):
        assert missing in message


def test_validate_required_seed_inventory_requires_blending_factors() -> None:
    base_seeds = {
        stem: pl.DataFrame().lazy()
        for stem in (
            "lobs",
            "perils",
            "verisk_events",
            "risklink_flood22_model_events",
            "fx_rates",
            "forecast_factors",
            "euws_rate_factors",
            "euws_rank_overrides",
        )
    }

    validate_required_seed_inventory(
        {**base_seeds, "blending_factors": pl.DataFrame().lazy()}
    )


def test_inspect_inputs_rejects_missing_required_seeds_before_reports(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        validation.seeds,
        "load",
        lambda data_root: {"lobs": pl.DataFrame().lazy()},
    )

    try:
        validation.inspect_inputs(Path("data"))
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected missing required seeds to raise")

    assert "missing required seed files:" in message
    assert "perils" in message
    assert "Traceback" not in message
