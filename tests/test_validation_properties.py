from __future__ import annotations

import polars as pl
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rollup.columns import Col, RawCol
from rollup.validation import (
    ValidationInputs,
    input_ylt_aal_by_lob_peril_summary,
    modelled_dimension_coverage_report,
)


small_name = st.text(alphabet="ABCDEF012345", min_size=1, max_size=6)
small_loss = st.floats(
    min_value=0.0, max_value=1_000_000.0, allow_nan=False, allow_infinity=False
)


def _seed_mapping(lobs: list[str], perils: list[str]) -> dict[str, pl.LazyFrame]:
    return {
        "lobs": pl.DataFrame(
            {
                Col.modelled_lob: lobs,
                Col.rollup_lob: [f"Rollup {value}" for value in lobs],
                Col.cds_cat_class_name: ["Class"] * len(lobs),
                Col.class_: ["CLASS"] * len(lobs),
                Col.office: ["Office"] * len(lobs),
                Col.currency: ["GBP"] * len(lobs),
            }
        ).lazy(),
        "perils": pl.DataFrame(
            {
                Col.modelled_peril: perils,
                Col.rollup_peril: [f"Rollup {value}" for value in perils],
                "region": ["Region"] * len(perils),
                "peril": ["Peril"] * len(perils),
                Col.region_peril_id: list(range(1, len(perils) + 1)),
                Col.blend_subregion_peril_id: [
                    str(value) for value in range(1, len(perils) + 1)
                ],
                Col.base_model: ["verisk"] * len(perils),
                Col.selection_priority: [1] * len(perils),
                Col.is_dialsup: [1] * len(perils),
                Col.is_euws: [0] * len(perils),
            }
        ).lazy(),
    }


def _aal_inputs(losses: list[float], unmapped_losses: list[float]) -> ValidationInputs:
    rows = {
        RawCol.CatalogTypeCode: ["STC"] * (len(losses) + len(unmapped_losses)),
        RawCol.ExposureAttribute: ["LOB_A"] * len(losses)
        + ["LOB_UNMAPPED"] * len(unmapped_losses),
        RawCol.Analysis: ["PERIL_A"] * len(losses)
        + ["PERIL_UNMAPPED"] * len(unmapped_losses),
        RawCol.ModelCode: [1] * (len(losses) + len(unmapped_losses)),
        RawCol.YearID: [2026] * (len(losses) + len(unmapped_losses)),
        RawCol.EventID: list(range(1, len(losses) + len(unmapped_losses) + 1)),
        RawCol.GroundUpLoss: losses + unmapped_losses,
    }
    return ValidationInputs(
        seeds=_seed_mapping(["LOB_A"], ["PERIL_A"]),
        ylts={
            "verisk": pl.DataFrame(rows).lazy(),
            "risklink": pl.DataFrame(
                schema={
                    RawCol.anlsid: pl.Int64,
                    RawCol.yearid: pl.Int64,
                    RawCol.eventid: pl.Int64,
                    RawCol.loss: pl.Float64,
                }
            ).lazy(),
        },
        ep_summaries=pl.DataFrame(
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
        coverage_report=pl.DataFrame(
            schema={"severity": pl.String, "valid": pl.Boolean}
        ),
    )


def _verisk_aal(report: pl.DataFrame) -> dict[str, object]:
    return report.filter(pl.col(Col.vendor) == "verisk").row(0, named=True)


@pytest.mark.fuzz
@settings(max_examples=25, deadline=None)
@given(
    lobs=st.lists(small_name, min_size=1, max_size=6, unique=True),
    perils=st.lists(small_name, min_size=1, max_size=6, unique=True),
)
def test_fuzz_coverage_has_no_errors_when_dimensions_exist(
    lobs: list[str], perils: list[str]
) -> None:
    ylt = {
        "verisk": pl.DataFrame(
            {
                RawCol.CatalogTypeCode: ["STC"] * len(lobs),
                RawCol.ExposureAttribute: lobs,
                RawCol.Analysis: [
                    perils[index % len(perils)] for index in range(len(lobs))
                ],
            }
        ).lazy(),
        "risklink": pl.DataFrame({RawCol.anlsid: [1]}).lazy(),
    }
    ep_summaries = pl.DataFrame(
        {
            Col.modelled_lob: lobs,
            Col.modelled_peril: [
                perils[index % len(perils)] for index in range(len(lobs))
            ],
        }
    ).lazy()

    report = modelled_dimension_coverage_report(
        _seed_mapping(lobs, perils), ylt, ep_summaries
    )

    assert report.is_empty()


@pytest.mark.fuzz
@settings(max_examples=20, deadline=None)
@given(
    base_losses=st.lists(small_loss, min_size=1, max_size=10),
    added_losses=st.lists(small_loss, min_size=1, max_size=10),
    unmapped_losses=st.lists(small_loss, min_size=0, max_size=10),
)
def test_fuzz_input_ylt_aal_is_additive_and_ignores_unmapped_rows(
    base_losses: list[float],
    added_losses: list[float],
    unmapped_losses: list[float],
) -> None:
    base = _verisk_aal(input_ylt_aal_by_lob_peril_summary(_aal_inputs(base_losses, [])))
    expanded = _verisk_aal(
        input_ylt_aal_by_lob_peril_summary(
            _aal_inputs(base_losses + added_losses, unmapped_losses)
        )
    )

    assert expanded[Col.row_count] == len(base_losses) + len(added_losses)
    assert expanded[Col.loss_sum] == pytest.approx(sum(base_losses) + sum(added_losses))
    expanded_raw_aal = expanded["raw_aal"]
    base_raw_aal = base["raw_aal"]
    assert isinstance(expanded_raw_aal, int | float)
    assert isinstance(base_raw_aal, int | float)
    assert expanded_raw_aal >= base_raw_aal
