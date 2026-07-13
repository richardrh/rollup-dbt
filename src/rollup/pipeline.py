from __future__ import annotations
# mypy: ignore-errors

import logging
from pathlib import Path

import polars as pl

from rollup.columns import Col
from rollup.config import RollupConfig
from rollup.intermediate.int_ep import (
    calculate_ep_blending_targets,
    join_ep_summaries,
    prepare_ep_blending_weights,
    select_blending_factor_seed,
    select_ep_blending_target_points,
)
from rollup.intermediate.int_ylt_dialsup import (
    apply_forecast_factors_to_dialsup_ylt,
    convert_dialsup_to_local_currency,
    drop_dialsup_factor_columns,
    enrich_dialsup_ylt_with_factors,
)
from rollup.intermediate.int_ylt_main import (
    apply_ep_blending_to_ylt,
    apply_euws_factors_to_ylt,
    apply_euws_overrides_to_ylt,
    apply_forecast_factors_to_ylt,
    convert_ylt_to_local_currency,
    enrich_ylt_with_ep_summaries,
    rank_ylt,
)
from rollup.marts.mart_fanout import build_event_validation_report, build_fanout
from rollup.pipeline_types import (
    PipelineRunResult,
    PipelineValidationInputs,
)
from rollup.pipeline_utils import (
    logged_phase,
)
from rollup.staging.stg_event_catalogues import stg_event_catalogue__risklink_flood, stg_event_catalogue__verisk
from rollup.staging.stg_ep_summaries import (
    enrich_ep_summaries,
    select_dialsup_ep_summaries,
    select_main_ep_summaries,
)
from rollup.staging.stg_factors import stg_forecast_dates, stg_forecast_factors, stg_gbp_fx_rates
from rollup.staging.stg_ylt import normalize_ylt
from rollup.validation import (
    ensure_pipeline_validation_inputs,
    load_pipeline_validation_inputs,
)
from rollup.writers.debug import write_debug_outputs
from rollup.writers.mart_outputs import write_mart_outputs
from rollup.writers.parquet import write_parquet_with_log


logger = logging.getLogger(__name__)


def run(
    data_root: Path | str = "data",
    *,
    output_root: Path | str = "output",
    debug: bool = False,
    config: RollupConfig | None = None,
    validation_inputs: PipelineValidationInputs | None = None,
) -> PipelineRunResult:
    data_root = Path(data_root)
    output_root = Path(output_root)
    work_dir = output_root / ".rollup_work"
    work_dir.mkdir(parents=True, exist_ok=True)
    config = config or RollupConfig()
    seed_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {}
    staging_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {}
    intermediate_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {}
    mart_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {}

    with logged_phase("validation"):
        if validation_inputs is None:
            validation_inputs = load_pipeline_validation_inputs(data_root)
        ensure_pipeline_validation_inputs(validation_inputs)

        seeds = validation_inputs.seeds
        ylts = validation_inputs.ylts
        ep_summaries = validation_inputs.ep_summaries
        coverage_report = validation_inputs.coverage_report
        logger.info(
            "validation summary seed_files=%d ylt_vendors=%d coverage_errors=%d",
            len(seeds),
            len(ylts),
            coverage_report.filter(pl.col("severity") == "error").height,
            extra={
                "event": "validation_summary",
                "seed_files": len(seeds),
                "ylt_vendors": len(ylts),
                "coverage_errors": coverage_report.filter(pl.col("severity") == "error").height,
            },
        )

    with logged_phase("staging"):
        verisk_events = stg_event_catalogue__verisk(seeds["verisk_events"])
        risklink_events = stg_event_catalogue__risklink_flood(seeds["risklink_flood22_model_events"])
        normalized_ylt = normalize_ylt(ylts)
        ep_enriched = enrich_ep_summaries(ep_summaries, seeds)
        ep_selected_main = select_main_ep_summaries(ep_enriched)
        ep_selected_dialsup = select_dialsup_ep_summaries(ep_enriched)
        gbp_fx_rates = stg_gbp_fx_rates(seeds["fx_rates"])
        forecast_factors = stg_forecast_factors(seeds["forecast_factors"])
        forecast_dates = stg_forecast_dates(forecast_factors)
        if debug:
            for name, frame in seeds.items():
                seed_frames[name] = frame
            seed_frames["verisk_events"] = verisk_events
            seed_frames["risklink_flood_events"] = risklink_events
            staging_frames["modelled_dimension_coverage"] = coverage_report
            staging_frames["ylt_normalized"] = normalized_ylt
            staging_frames["ep_summaries"] = ep_summaries
            staging_frames["ep_summaries_enriched"] = ep_enriched
            staging_frames["ep_summaries_selected"] = ep_selected_main
            staging_frames["ep_summaries_selected_dialsup"] = ep_selected_dialsup
            staging_frames["gbp_fx_rates"] = gbp_fx_rates
            staging_frames["forecast_factors"] = forecast_factors
            staging_frames["forecast_dates"] = forecast_dates
        logger.info(
            "staging summary seed_frames=%d staging_frames=%d",
            len(seed_frames),
            len(staging_frames),
            extra={"event": "staging_summary", "seed_frames": len(seed_frames), "staging_frames": len(staging_frames)},
        )

    with logged_phase("intermediate"):
        enriched_ylt = enrich_ylt_with_ep_summaries(normalized_ylt, ep_selected_main)
        enriched_ylt_dialsup = enrich_ylt_with_ep_summaries(normalized_ylt, ep_selected_dialsup)
        joined_ep_summaries = join_ep_summaries(ep_selected_main)
        ep_blending_target_points = select_ep_blending_target_points(joined_ep_summaries, config)
        ep_blending_weights = prepare_ep_blending_weights(select_blending_factor_seed(seeds))
        ep_blending_targets = calculate_ep_blending_targets(ep_blending_target_points, ep_blending_weights, config)
        ylt_original = enriched_ylt.with_columns(
            pl.lit("original").alias(Col.metric),
        ).filter((pl.col(Col.vendor) == pl.col(Col.base_model)) & (pl.col(Col.loss) >= config.outputs.minimum_event_loss_threshold / 5))

        ylt_ranked = rank_ylt(ylt_original, config)

        ylt_original_dialsup = enriched_ylt_dialsup.with_columns(
            pl.lit("original").alias(Col.metric),
        ).filter((pl.col(Col.vendor) == pl.col(Col.base_model)) & (pl.col(Col.loss) >= config.outputs.minimum_event_loss_threshold / 5))
        ylt_ranked_dialsup = rank_ylt(ylt_original_dialsup, config)

        dialsup_factor_base = enrich_dialsup_ylt_with_factors(
            ylt_ranked_dialsup,
            verisk_events,
            gbp_fx_rates,
            forecast_dates,
            forecast_factors,
        )
        dialsup_original = drop_dialsup_factor_columns(
            dialsup_factor_base.with_columns(pl.lit("dialsup_original").alias(Col.metric))
        )
        dialsup_localccy_with_factors = convert_dialsup_to_local_currency(dialsup_factor_base)
        dialsup_localccy = drop_dialsup_factor_columns(dialsup_localccy_with_factors)
        dialsup_localccy_forecast = drop_dialsup_factor_columns(
            apply_forecast_factors_to_dialsup_ylt(dialsup_localccy_with_factors)
        )
        ylt_dialsup = pl.concat([dialsup_original, dialsup_localccy, dialsup_localccy_forecast])
        ylt_dialsup_path = work_dir / "ylt_dialsup.parquet"
        write_parquet_with_log(ylt_dialsup, ylt_dialsup_path)
        ylt_dialsup = pl.scan_parquet(ylt_dialsup_path)

        ylt_blended = apply_ep_blending_to_ylt(ylt_ranked, ep_blending_targets)
        ylt_localccy = convert_ylt_to_local_currency(ylt_blended, gbp_fx_rates)
        ylt_localccy_forecast = apply_forecast_factors_to_ylt(
            ylt_localccy,
            forecast_dates,
            forecast_factors,
        )
        ylt_euws = apply_euws_factors_to_ylt(
            ylt_localccy_forecast,
            verisk_events,
            seeds,
        )
        ylt_euws_override = apply_euws_overrides_to_ylt(ylt_euws, seeds)
        ylt = pl.concat(
            [ylt_ranked, ylt_blended, ylt_localccy, ylt_localccy_forecast, ylt_euws, ylt_euws_override],
            how="diagonal",
        )
        ylt_path = work_dir / "ylt_combined_all_factors.parquet"
        write_parquet_with_log(ylt, ylt_path)
        ylt = pl.scan_parquet(ylt_path)
        if debug:
            intermediate_frames["ylt_combined_enriched"] = enriched_ylt
            intermediate_frames["ylt_combined_enriched_dialsup"] = enriched_ylt_dialsup
            intermediate_frames["ep_summaries_enriched"] = ep_enriched
            intermediate_frames["ep_vendor_joined"] = joined_ep_summaries
            intermediate_frames["ep_blending_target_points"] = ep_blending_target_points
            intermediate_frames["ep_blending_weights"] = ep_blending_weights
            intermediate_frames["ep_blending_targets"] = ep_blending_targets
            intermediate_frames["ylt_original"] = ylt_original
            intermediate_frames["ylt_ranked"] = ylt_ranked
            intermediate_frames["ylt_original_dialsup"] = ylt_original_dialsup
            intermediate_frames["ylt_ranked_dialsup"] = ylt_ranked_dialsup
            intermediate_frames["ylt_dialsup"] = ylt_dialsup
            intermediate_frames["ylt_blending_applied"] = ylt_blended
            intermediate_frames["ylt_fx_applied"] = ylt_localccy
            intermediate_frames["ylt_forecast_applied"] = ylt_localccy_forecast
            intermediate_frames["ylt_euws_applied"] = ylt_euws
            intermediate_frames["ylt_euws_override_applied"] = ylt_euws_override
        logger.info(
            "intermediate summary frames=%d",
            len(intermediate_frames),
            extra={"event": "intermediate_summary", "frames": len(intermediate_frames)},
        )

    with logged_phase("marts"):
        threshold = config.outputs.minimum_event_loss_threshold
        ylt_thresholded = ylt.filter(
            (pl.col(Col.metric) != "euws_override")
            | (pl.col(Col.loss).is_not_null() if threshold <= 0 else pl.col(Col.loss) >= threshold)
        )
        ylt_dialsup_thresholded = ylt_dialsup.filter(
            (pl.col(Col.metric) != "dialsup_localccy_forecast")
            | (pl.col(Col.loss).is_not_null() if threshold <= 0 else pl.col(Col.loss) >= threshold)
        )
        main_fanout = build_fanout(
            ylt_thresholded.filter(pl.col(Col.metric) == "euws_override"),
            risklink_events,
        )
        mart_frames["main_fanout"] = main_fanout

        dialsup_fanout = build_fanout(
            ylt_dialsup_thresholded.filter(pl.col(Col.metric) == "dialsup_localccy_forecast"),
            risklink_events,
        )
        mart_frames["dialsup_fanout"] = dialsup_fanout

        mart_frames["event_validation"] = build_event_validation_report(
            main_fanout,
            dialsup_fanout,
        )
        mart_frames["ylt_long"] = ylt_thresholded
        mart_frames["ylt_dialsup"] = ylt_dialsup_thresholded

    result = PipelineRunResult(
        seeds=seed_frames,
        staging=staging_frames,
        intermediate=intermediate_frames,
        marts=mart_frames,
    )

    if debug:
        with logged_phase("debug_outputs"):
            write_debug_outputs(
                output_root,
                seeds=seed_frames,
                staging=staging_frames,
                intermediate=intermediate_frames,
                marts=mart_frames,
            )

    with logged_phase("write_outputs"):
        write_mart_outputs(output_root, mart_frames)

    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
