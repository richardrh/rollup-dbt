from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol
from rollup.config import BlendingConfig, BlendingTargetPoint, RollupConfig
from rollup.intermediate import (
    int_ep_blending_target_points,
    int_ep_blending_targets,
    int_ep_blending_weights,
    int_ep_summaries_dialsup,
    int_ep_summaries_enriched,
    int_ep_summaries_main,
    int_ep_vendor_joined,
    int_ylt_ranked,
)


def _peril_selection_seed_mapping() -> dict[str, pl.LazyFrame]:
    return {
        "lobs": pl.DataFrame(
            {
                Col.modelled_lob: ["LOB"],
                Col.rollup_lob: ["Property"],
                Col.cds_cat_class_name: ["Class"],
                Col.class_: ["CLASS"],
                Col.office: ["Office"],
                Col.currency: ["GBP"],
            }
        ).lazy(),
        "perils": pl.DataFrame(
            {
                Col.modelled_peril: ["HIGH", "LOW", "DIAL_A", "DIAL_B"],
                Col.rollup_peril: ["Europe_FL"] * 4,
                "region": ["Europe"] * 4,
                "peril": ["FL"] * 4,
                Col.region_peril_id: [216] * 4,
                Col.blend_subregion_peril_id: ["216b"] * 4,
                Col.base_model: ["risklink"] * 4,
                Col.selection_priority: [20, 10, 30, 40],
                Col.is_dialsup: [0, 0, 1, 1],
                Col.is_euws: [0, 0, 0, 0],
            }
        ).lazy(),
    }


def _peril_selection_ep_summaries() -> pl.LazyFrame:
    return pl.DataFrame(
        {
            Col.vendor: ["risklink"] * 4,
            Col.analysis_id: ["1", "2", "3", "4"],
            Col.modelled_lob: ["LOB"] * 4,
            Col.modelled_peril: ["HIGH", "LOW", "DIAL_A", "DIAL_B"],
            Col.ep_type: ["AAL"] * 4,
            Col.return_period: [0] * 4,
            Col.loss: [1.0, 2.0, 3.0, 4.0],
        }
    ).lazy()


def _selected_low_seed_mapping() -> dict[str, pl.LazyFrame]:
    seeds = _peril_selection_seed_mapping()
    return {
        "lobs": seeds["lobs"],
        "perils": seeds["perils"].filter(pl.col(Col.modelled_peril) == "LOW"),
    }


def _selected_low_with_region_one_seed_mapping() -> dict[str, pl.LazyFrame]:
    seeds = _selected_low_seed_mapping()
    return {
        "lobs": seeds["lobs"],
        "perils": seeds["perils"].with_columns(
            pl.lit(1).alias(Col.region_peril_id),
            pl.lit("1").alias(Col.blend_subregion_peril_id),
        ),
    }


def _blending_factor_mapping() -> dict[str, pl.LazyFrame]:
    return {
        "blending_factors": pl.DataFrame(
            {
                RawCol.RegionPerilID: [216, 216],
                RawCol.SubRegionPerilID: ["216a", "216b"],
                RawCol.SubRegionPeril: ["unused", "selected"],
                RawCol.AIRBlend: [1.0, 0.25],
                RawCol.RMSBlend: [0.0, 0.75],
            }
        ).lazy()
    }


def _ep_summary_rows(
    vendors: list[str],
    losses: list[float],
    *,
    ep_type: str = "AAL",
    return_period: int = 0,
) -> pl.LazyFrame:
    return pl.DataFrame(
        {
            Col.vendor: vendors,
            Col.analysis_id: [
                "AIR" if vendor == "verisk" else "RMS" for vendor in vendors
            ],
            Col.modelled_lob: ["LOB"] * len(vendors),
            Col.modelled_peril: ["LOW"] * len(vendors),
            Col.ep_type: [ep_type] * len(vendors),
            Col.return_period: [return_period] * len(vendors),
            Col.loss: losses,
        }
    ).lazy()


def test_ep_intermediate_models_validate_final_candidate_schemas() -> None:
    enriched = int_ep_summaries_enriched.transform(
        _peril_selection_ep_summaries(), _peril_selection_seed_mapping()
    )
    main = int_ep_summaries_main.transform(enriched)
    dialsup = int_ep_summaries_dialsup.transform(enriched)
    joined = int_ep_vendor_joined.transform(main)
    target_points = int_ep_blending_target_points.transform(joined)
    targets = int_ep_blending_targets.transform(
        target_points, int_ep_blending_weights.transform(_blending_factor_mapping())
    )

    for model, candidate in (
        (int_ep_summaries_enriched, enriched),
        (int_ep_summaries_main, main),
        (int_ep_summaries_dialsup, dialsup),
        (int_ep_vendor_joined, joined),
        (int_ep_blending_targets, targets),
    ):
        assert candidate.collect_schema() == model.schema()
        model.validate(candidate)


def test_ep_main_selection_chooses_lowest_selection_priority() -> None:
    enriched = int_ep_summaries_enriched.transform(
        _peril_selection_ep_summaries(), _peril_selection_seed_mapping()
    )

    selected = (
        int_ep_summaries_main.transform(enriched)
        .select(Col.modelled_peril)
        .collect()
        .to_series()
        .to_list()
    )

    assert selected == ["LOW"]


def test_ep_dialsup_selection_keeps_all_is_dialsup_candidates() -> None:
    enriched = int_ep_summaries_enriched.transform(
        _peril_selection_ep_summaries(), _peril_selection_seed_mapping()
    )

    selected = (
        int_ep_summaries_dialsup.transform(enriched)
        .select(Col.modelled_peril)
        .collect()
        .to_series()
        .sort()
        .to_list()
    )

    assert selected == ["DIAL_A", "DIAL_B"]


def test_ep_blending_joins_weights_by_blend_subregion_peril_id() -> None:
    enriched = int_ep_summaries_enriched.transform(
        _ep_summary_rows(["verisk", "risklink"], [100.0, 200.0]),
        _selected_low_seed_mapping(),
    )
    joined = int_ep_vendor_joined.transform(int_ep_summaries_main.transform(enriched))
    weights = int_ep_blending_weights.transform(_blending_factor_mapping())

    blended = int_ep_blending_targets.transform(
        int_ep_blending_target_points.transform(joined), weights
    ).collect()

    assert blended.item(0, Col.blend_subregion_peril_id) == "216b"
    assert blended.item(0, Col.sub_region_peril) == "selected"
    assert blended.item(0, Col.risklink_blended_contribution) == 150.0
    assert blended.item(0, Col.verisk_blended_contribution) == 25.0
    assert blended.item(0, Col.target_loss) == 175.0
    assert blended.item(0, Col.base_model) == "risklink"


def test_missing_blending_factors_seed_raises_clear_error() -> None:
    try:
        int_ep_blending_weights.transform({})
    except ValueError as exc:
        assert "missing required key 'blending_factors'" in str(exc)
    else:
        raise AssertionError("missing blending_factors seed did not raise")


def test_ep_blending_falls_back_to_base_model_loss_when_counterparty_missing() -> None:
    seeds = {
        "blending_factors": pl.DataFrame(
            {
                RawCol.RegionPerilID: [216],
                RawCol.SubRegionPerilID: ["216b"],
                RawCol.SubRegionPeril: ["selected"],
                RawCol.AIRBlend: [0.25],
                RawCol.RMSBlend: [0.75],
            }
        ).lazy()
    }
    enriched = int_ep_summaries_enriched.transform(
        _ep_summary_rows(["risklink"], [200.0]), _selected_low_seed_mapping()
    )
    joined = int_ep_vendor_joined.transform(int_ep_summaries_main.transform(enriched))

    blended = int_ep_blending_targets.transform(
        int_ep_blending_target_points.transform(joined),
        int_ep_blending_weights.transform(seeds),
    ).collect()

    assert blended.height == 1
    assert blended.item(0, Col.target_loss) == 200.0
    assert blended.item(0, Col.uplift_factor_on_base_model) == 1.0
    assert blended.item(0, Col.risklink_blended_contribution) == 200.0
    assert blended.item(0, Col.verisk_blended_contribution) == 0.0


def test_ep_blending_uses_configured_target_points_caps_and_vendor_years() -> None:
    config = RollupConfig(
        blending=BlendingConfig(
            vendor_years={"verisk": 4, "risklink": 8},
            target_points=(
                BlendingTargetPoint("AAL", 0),
                BlendingTargetPoint("OEP", 2),
            ),
            uplift_factor_min=0.5,
            uplift_factor_max=2.0,
        )
    )
    ranked = int_ylt_ranked.transform(
        pl.DataFrame(
            {
                Col.vendor: ["verisk", "verisk"],
                Col.modelled_lob: ["LOB", "LOB"],
                Col.rollup_peril: ["PERIL", "PERIL"],
                Col.year_id: [2026, 2026],
                Col.event_id: [1, 2],
                Col.analysis_id: ["A", "A"],
                Col.model_code: [1, 1],
                Col.loss: [100.0, 50.0],
                Col.modelled_peril: ["PERIL", "PERIL"],
                Col.rollup_lob: ["LOB", "LOB"],
                Col.region_peril_id: [1, 1],
                Col.blend_subregion_peril_id: ["1", "1"],
                Col.base_model: ["verisk", "verisk"],
                Col.selection_priority: [1, 1],
                Col.is_dialsup: [0, 0],
                Col.is_euws: [0, 0],
                Col.cds_cat_class_name: ["Wind", "Wind"],
                Col.class_: ["COMM", "COMM"],
                Col.office: ["DE", "DE"],
                Col.currency: ["EUR", "EUR"],
                Col.metric: ["original", "original"],
            }
        ).lazy(),
        config,
    ).collect()
    assert ranked.sort(Col.loss, descending=True)[Col.rp].to_list() == [4.0, 2.0]
    assert ranked.sort(Col.loss, descending=True)[Col.rp_bucket].to_list() == [2, 2]

    seeds = {
        "blending_factors": pl.DataFrame(
            {
                RawCol.RegionPerilID: [1],
                RawCol.SubRegionPerilID: ["1"],
                RawCol.SubRegionPeril: ["x"],
                RawCol.AIRBlend: [10.0],
                RawCol.RMSBlend: [10.0],
            }
        ).lazy()
    }
    enriched = int_ep_summaries_enriched.transform(
        _ep_summary_rows(
            ["verisk", "risklink"], [100.0, 1000.0], ep_type="OEP", return_period=2
        ),
        _selected_low_with_region_one_seed_mapping(),
    )
    joined = int_ep_vendor_joined.transform(int_ep_summaries_main.transform(enriched))
    blended = int_ep_blending_targets.transform(
        int_ep_blending_target_points.transform(joined, config),
        int_ep_blending_weights.transform(seeds),
        config,
    ).collect()
    assert blended.item(0, Col.uplift_factor_on_base_model) == 2.0
