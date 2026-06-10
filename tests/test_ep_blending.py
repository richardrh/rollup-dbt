from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.intermediate.apply_blending import apply_blending
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
                "lob_id": [1],
                Col.modelled_lob: ["ML"],
                Col.rollup_lob: ["RL"],
                "lob_type": ["property"],
                Col.cds_cat_class_name: ["FA"],
                Col.class_: ["FA"],
                Col.office: ["UK"],
                Col.currency: ["GBP"],
            }
        ),
        perils=pl.DataFrame(
            {
                Col.modelled_peril: ["HIGH", "LOW"],
                Col.rollup_peril: ["UK_WS", "UK_WS"],
                "region": ["UK", "UK"],
                "peril": ["WS", "WS"],
                Col.region_peril_id: [216, 216],
                Col.base_model: ["verisk", "verisk"],
                Col.selection_priority: [2, 1],
                Col.is_dialsup: [1, 0],
                Col.is_euws: [0, 0],
            }
        ),
    )

    result = stage_ep_summaries(frames).collect().sort([Col.vendor, Col.loss])

    assert result.select(
        Col.vendor, Col.modelled_peril, Col.selection_priority, Col.is_dialsup
    ).rows() == [
        ("risklink", "LOW", 1, 1),
        ("verisk", "LOW", 1, 1),
    ]


def test_apply_blending_uses_ep_targets_base_model_and_rp_bucket() -> None:
    enriched = enriched_ylt_frame()
    staged_ep = pl.DataFrame(
        {
            Col.vendor: ["verisk", "risklink"],
            Col.analysis_id: ["V", "R"],
            Col.modelled_lob: ["ML", "ML"],
            Col.modelled_peril: ["FL", "FL"],
            Col.ep_type: ["OEP", "OEP"],
            Col.return_period: [1000, 1000],
            Col.loss: [200.0, 100.0],
            Col.rollup_lob: ["RL", "RL"],
            Col.class_: ["FA", "FA"],
            Col.office: ["UK", "UK"],
            Col.currency: ["GBP", "GBP"],
            Col.rollup_peril: ["Spain_FL", "Spain_FL"],
            Col.region_peril_id: [101, 101],
            Col.base_model: ["risklink", "risklink"],
            Col.selection_priority: [1, 1],
            Col.is_dialsup: [0, 0],
            Col.is_euws: [0, 0],
        }
    )
    blending = pl.DataFrame(
        {
            "RegionPerilID": [101],
            "SubRegionPerilID": ["101a"],
            "SubRegionPeril": ["Flood"],
            "AIRBlend": [0.5],
            "RMSBlend": [1.0],
        }
    )

    result = (
        apply_blending(
            enriched.lazy(),
            staged_ep.lazy(),
            blending,
            {"verisk": 123, "risklink": 2_000},
        )
        .collect()
        .sort(Col.loss)
    )

    assert result.select(
        Col.vendor,
        Col.base_model,
        Col.rp_bucket,
        Col.uplift_factor_on_base_model,
        "blended_loss",
    ).rows() == [
        ("risklink", "risklink", 1000, 2.0, 50.0),
        ("risklink", "risklink", 1000, 2.0, 100.0),
    ]


def enriched_ylt_frame() -> pl.DataFrame:
    rows = []
    for vendor, analysis_id, losses in [
        ("verisk", "V", [10.0]),
        ("risklink", "R", [25.0, 50.0]),
    ]:
        for event_id, loss in enumerate(losses, start=1):
            rows.append(
                {
                    Col.vendor: vendor,
                    Col.analysis_id: analysis_id,
                    Col.modelled_lob: "ML",
                    Col.modelled_peril: "FL",
                    Col.model_code: 7,
                    Col.year_id: event_id,
                    Col.event_id: event_id,
                    Col.loss: loss,
                    Col.base_model: "risklink",
                    Col.rollup_lob: "RL",
                    Col.rollup_peril: "Spain_FL",
                    Col.region_peril_id: 101,
                    Col.class_: "FA",
                    Col.office: "UK",
                    Col.currency: "GBP",
                    Col.selection_priority: 1,
                    Col.is_dialsup: 0,
                    Col.is_euws: 0,
                }
            )
    return pl.DataFrame(rows)


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
        verisk_events=empty_lazy,
        ep_summaries=ep_summaries,
        lobs=lobs,
        perils=perils,
        blending=blending if blending is not None else pl.DataFrame(),
        fx_rates=pl.DataFrame(),
        forecast_factors=pl.DataFrame(),
        euws_factors=pl.DataFrame(),
        euws_overrides=pl.DataFrame(),
    )
