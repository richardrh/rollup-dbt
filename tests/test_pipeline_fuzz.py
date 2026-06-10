from __future__ import annotations

import polars as pl
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rollup.columns import Col
from rollup.marts.wide import wide, wide_column_name
from rollup.staging.load_sources import StagingFrames
from rollup.staging.stage_ep_summaries import stage_ep_summaries


small_name = st.text(alphabet="ABCDEF012345", min_size=1, max_size=6)
small_loss = st.floats(
    min_value=0.0, max_value=1_000_000.0, allow_nan=False, allow_infinity=False
)


@pytest.mark.fuzz
@settings(max_examples=25, deadline=None)
@given(
    lobs=st.lists(small_name, min_size=1, max_size=6, unique=True),
    perils=st.lists(small_name, min_size=1, max_size=6, unique=True),
)
def test_fuzz_stage_ep_summaries_enriches_known_dimensions(
    lobs: list[str],
    perils: list[str],
) -> None:
    ep_rows = [
        {
            Col.vendor: "verisk",
            Col.analysis_id: f"A{index}",
            Col.modelled_lob: lob,
            Col.modelled_peril: perils[index % len(perils)],
            Col.ep_type: "AAL",
            Col.return_period: 0,
            Col.loss: float(index + 1),
        }
        for index, lob in enumerate(lobs)
    ]

    result = stage_ep_summaries(
        staging_frames(
            ep_summaries=pl.DataFrame(ep_rows),
            lobs=lob_lookup(lobs),
            perils=peril_lookup(perils),
        )
    ).collect()

    assert result.height == len(lobs)
    assert result.select(
        pl.all_horizontal(
            pl.col(Col.rollup_lob).is_not_null(),
            pl.col(Col.rollup_peril).is_not_null(),
            pl.col(Col.region_peril_id).is_not_null(),
        ).all()
    ).item()


@pytest.mark.fuzz
@settings(max_examples=25, deadline=None)
@given(losses=st.lists(small_loss, min_size=1, max_size=10))
def test_fuzz_wide_output_preserves_metric_loss_totals(losses: list[float]) -> None:
    metric = "loss_blended_fx_gbp_forecast_euws_override"
    frame = pl.DataFrame(
        {
            Col.vendor: ["verisk"] * len(losses),
            Col.base_model: ["verisk"] * len(losses),
            Col.analysis_id: ["A"] * len(losses),
            Col.modelled_lob: ["LOB_A"] * len(losses),
            Col.modelled_peril: ["PERIL_A"] * len(losses),
            Col.rollup_lob: ["Rollup LOB A"] * len(losses),
            Col.rollup_peril: ["Rollup PERIL A"] * len(losses),
            Col.region_peril_id: [1] * len(losses),
            Col.class_: ["CLASS"] * len(losses),
            Col.office: ["Office"] * len(losses),
            Col.currency: ["GBP"] * len(losses),
            Col.target_currency: ["GBP"] * len(losses),
            Col.year_id: list(range(1, len(losses) + 1)),
            Col.event_id: list(range(1, len(losses) + 1)),
            Col.forecast_date: ["2026-01-01"] * len(losses),
            Col.is_dialsup: [0] * len(losses),
            Col.metric: [metric] * len(losses),
            Col.loss: losses,
        }
    )

    result = wide(frame)
    value_column = wide_column_name(metric, "2026-01-01")

    assert value_column in result.columns
    assert result.select(pl.col(value_column).sum()).item() == pytest.approx(
        sum(losses)
    )


def lob_lookup(lobs: list[str]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "lob_id": list(range(1, len(lobs) + 1)),
            Col.modelled_lob: lobs,
            Col.rollup_lob: [f"Rollup {value}" for value in lobs],
            "lob_type": ["property"] * len(lobs),
            Col.cds_cat_class_name: ["Class"] * len(lobs),
            Col.class_: ["CLASS"] * len(lobs),
            Col.office: ["Office"] * len(lobs),
            Col.currency: ["GBP"] * len(lobs),
        }
    )


def peril_lookup(perils: list[str]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            Col.modelled_peril: perils,
            Col.rollup_peril: [f"Rollup {value}" for value in perils],
            "region": ["Region"] * len(perils),
            "peril": ["Peril"] * len(perils),
            Col.region_peril_id: list(range(1, len(perils) + 1)),
            Col.base_model: ["verisk"] * len(perils),
            Col.selection_priority: [1] * len(perils),
            Col.is_dialsup: [0] * len(perils),
            Col.is_euws: [0] * len(perils),
        }
    )


def staging_frames(
    *,
    ep_summaries: pl.DataFrame,
    lobs: pl.DataFrame,
    perils: pl.DataFrame,
) -> StagingFrames:
    empty_lazy = pl.DataFrame().lazy()
    return StagingFrames(
        verisk_ylt=empty_lazy,
        risklink_ylt=empty_lazy,
        verisk_events=empty_lazy,
        ep_summaries=ep_summaries,
        lobs=lobs,
        perils=perils,
        blending=pl.DataFrame(),
        fx_rates=pl.DataFrame(),
        forecast_factors=pl.DataFrame(),
        euws_factors=pl.DataFrame(),
        euws_overrides=pl.DataFrame(),
    )
