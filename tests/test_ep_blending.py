from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.staging.load_sources import StagingFrames
from rollup.staging.stage_ep_summaries import stage_ep_summaries


def test_stage_ep_summaries_selects_lowest_priority_modelled_peril() -> None:
    frames = staging_frames(
        ep_summaries=pl.DataFrame(
            {
                Col.vendor: ["Verisk", "Verisk", "RiskLink"],
                Col.analysis_id: ["A", "A", "R1"],
                Col.modelled_lob: ["ML", "ML", "ML"],
                Col.modelled_peril: ["HIGH", "LOW", "LOW"],
                Col.ep_type: ["AAL", "AAL", "AAL"],
                Col.return_period: [0, 0, 0],
                Col.loss: [10.0, 20.0, 30.0],
            }
        ),
        lobs=pl.DataFrame(
            {
                Col.modelled_lob: ["ML"],
                Col.rollup_lob: ["RL"],
                Col.class_: ["FA"],
                Col.office: ["UK"],
                Col.currency: ["GBP"],
            }
        ),
        perils=pl.DataFrame(
            {
                Col.modelled_peril: ["HIGH", "LOW"],
                Col.rollup_peril: ["UK_WS", "UK_WS"],
                Col.region_peril_id: [216, 216],
                Col.selection_priority: [2, 1],
                Col.is_dialsup: [0, 1],
            }
        ),
    )

    result = stage_ep_summaries(frames).collect().sort([Col.vendor, Col.loss])

    assert result.select(Col.vendor, Col.modelled_peril, Col.selection_priority, Col.is_dialsup).rows() == [
        ("risklink", "LOW", 1, 1),
        ("verisk", "LOW", 1, 1),
    ]


def staging_frames(
    *,
    ep_summaries: pl.DataFrame,
    lobs: pl.DataFrame,
    perils: pl.DataFrame,
    blending: pl.DataFrame | None = None,
) -> StagingFrames:
    empty_lazy = pl.DataFrame().lazy()
    return StagingFrames(
        verisk_ylt=empty_lazy,
        risklink_ylt=empty_lazy,
        ep_summaries=ep_summaries,
        lobs=lobs,
        perils=perils,
        blending=blending or pl.DataFrame(),
        fx_rates=pl.DataFrame(),
        forecast_factors=pl.DataFrame(),
        euws_factors=pl.DataFrame(),
    )
