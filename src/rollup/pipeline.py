from __future__ import annotations

import logging
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import polars as pl

from rollup import validation
from rollup.config import RollupConfig
from rollup.intermediate import (
    int_ep_blending_target_points,
    int_ep_blending_targets,
    int_ep_blending_weights,
    int_ep_summaries_dialsup,
    int_ep_summaries_enriched,
    int_ep_summaries_main,
    int_ep_vendor_joined,
    int_forecast_dates,
    int_ylt_base_selected,
    int_ylt_dialsup_factor_base,
    int_ylt_dialsup_forecast_metric,
    int_ylt_dialsup_local_currency_metric,
    int_ylt_dialsup_metric_stream,
    int_ylt_dialsup_original_metric,
    int_ylt_enriched,
    int_ylt_main_blended,
    int_ylt_main_euws,
    int_ylt_main_euws_override,
    int_ylt_main_forecast,
    int_ylt_main_local_currency,
    int_ylt_main_metric_stream,
    int_ylt_normalized,
    int_ylt_ranked,
)
from rollup.marts import (
    mart_dialsup_fanout,
    mart_event_validation,
    mart_main_fanout,
    mart_ylt_dialsup_long,
    mart_ylt_main_long,
)
from rollup.output_contract import (
    COMBINED_YLT_FILE,
    DIALSUP_YLT_FILE,
    EVENT_VALIDATION_FILE,
    MARTS_DIR,
    WIDE_YLT_FILE,
)
from rollup.staging import (
    stg_forecast_factors,
    stg_gbp_fx_rates,
    stg_risklink_flood_events,
    stg_risklink_ylt,
    stg_verisk_events,
    stg_verisk_ylt,
)
from rollup.writers import debug as debug_writer
from rollup.writers import fanout_partitions, parquet, wide_output

logger = logging.getLogger(__name__)


@contextmanager
def _logged_phase(phase: str) -> Iterator[None]:
    started = time.perf_counter()
    logger.info("start phase=%s", phase, extra={"event": "phase_start", "phase": phase})
    try:
        yield
    except Exception:
        elapsed_seconds = time.perf_counter() - started
        logger.exception(
            "failed phase=%s elapsed=%.2fs",
            phase,
            elapsed_seconds,
            extra={
                "event": "phase_failed",
                "phase": phase,
                "elapsed_seconds": elapsed_seconds,
            },
        )
        raise
    elapsed_seconds = time.perf_counter() - started
    logger.info(
        "done phase=%s elapsed=%.2fs",
        phase,
        elapsed_seconds,
        extra={
            "event": "phase_done",
            "phase": phase,
            "elapsed_seconds": elapsed_seconds,
        },
    )


def run(
    data_root: Path | str = "data",
    *,
    output_root: Path | str = "output",
    debug: bool = False,
    config: RollupConfig | None = None,
) -> None:
    data_root = Path(data_root)
    output_root = Path(output_root)
    config = config or RollupConfig()
    output_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="rollup-work-", dir=output_root
    ) as work_dir_name:
        work_dir = Path(work_dir_name)
        with _logged_phase("validation"):
            validation_inputs = validation.inspect_inputs(data_root)
            validation.validate_inputs(validation_inputs)

            seeds = validation_inputs.seeds
            ylts = validation_inputs.ylts
            ep_summaries = validation_inputs.ep_summaries
            coverage_report = validation_inputs.coverage_report
            coverage_errors = int(
                (coverage_report.get_column("severity") == "error").sum()
            )
            logger.info(
                "validation summary seed_files=%d ylt_vendors=%d coverage_errors=%d",
                len(seeds),
                len(ylts),
                coverage_errors,
                extra={
                    "event": "validation_summary",
                    "seed_files": len(seeds),
                    "ylt_vendors": len(ylts),
                    "coverage_errors": coverage_errors,
                },
            )

        with _logged_phase("staging"):
            verisk_events = stg_verisk_events.Model.transform(seeds["verisk_events"])
            risklink_events = stg_risklink_flood_events.Model.transform(
                seeds["risklink_flood22_model_events"]
            )
            verisk_ylt = stg_verisk_ylt.Model.transform(ylts["verisk"])
            risklink_ylt = stg_risklink_ylt.Model.transform(ylts["risklink"])
            gbp_fx_rates = stg_gbp_fx_rates.Model.transform(seeds["fx_rates"])
            forecast_factors = stg_forecast_factors.Model.transform(
                seeds["forecast_factors"]
            )
            source_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {
                "ep_summaries": ep_summaries,
            }
            seed_frames: dict[str, pl.DataFrame | pl.LazyFrame] = dict(seeds)
            staging_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {
                "forecast_factors": forecast_factors,
                "gbp_fx_rates": gbp_fx_rates,
                "risklink_flood_events": risklink_events,
                "risklink_ylt": risklink_ylt,
                "verisk_events": verisk_events,
                "verisk_ylt": verisk_ylt,
            }
            logger.info(
                "staging summary seed_frames=%d staging_frames=%d",
                len(seed_frames),
                len(staging_frames),
                extra={
                    "event": "staging_summary",
                    "seed_frames": len(seed_frames),
                    "staging_frames": len(staging_frames),
                },
            )

        with _logged_phase("intermediate"):
            normalized_ylt = int_ylt_normalized.Model.transform(
                verisk_ylt, risklink_ylt
            )
            ep_enriched = int_ep_summaries_enriched.Model.transform(ep_summaries, seeds)
            ep_selected_main = int_ep_summaries_main.Model.transform(ep_enriched)
            ep_selected_dialsup = int_ep_summaries_dialsup.Model.transform(ep_enriched)
            forecast_dates = int_forecast_dates.Model.transform(forecast_factors)
            enriched_ylt = int_ylt_enriched.Model.transform(
                normalized_ylt, ep_selected_main
            )
            enriched_ylt_dialsup = int_ylt_enriched.Model.transform(
                normalized_ylt, ep_selected_dialsup
            )
            joined_ep_summaries = int_ep_vendor_joined.Model.transform(ep_selected_main)
            ep_blending_target_points = int_ep_blending_target_points.Model.transform(
                joined_ep_summaries, config
            )
            ep_blending_weights = int_ep_blending_weights.Model.transform(seeds)
            ep_blending_targets = int_ep_blending_targets.Model.transform(
                ep_blending_target_points, ep_blending_weights, config
            )
            ylt_original = int_ylt_base_selected.Model.transform(enriched_ylt, config)
            ylt_ranked = int_ylt_ranked.Model.transform(ylt_original, config)
            ylt_original_dialsup = int_ylt_base_selected.Model.transform(
                enriched_ylt_dialsup, config
            )
            ylt_ranked_dialsup = int_ylt_ranked.Model.transform(
                ylt_original_dialsup, config
            )
            dialsup_factor_base = int_ylt_dialsup_factor_base.Model.transform(
                ylt_ranked_dialsup,
                verisk_events,
                gbp_fx_rates,
                forecast_dates,
                forecast_factors,
            )
            dialsup_original = int_ylt_dialsup_original_metric.Model.transform(
                dialsup_factor_base
            )
            dialsup_localccy = int_ylt_dialsup_local_currency_metric.Model.transform(
                dialsup_factor_base
            )
            dialsup_localccy_forecast = int_ylt_dialsup_forecast_metric.Model.transform(
                dialsup_factor_base
            )
            ylt_dialsup = int_ylt_dialsup_metric_stream.Model.transform(
                dialsup_original, dialsup_localccy, dialsup_localccy_forecast
            )
            ylt_dialsup_path = work_dir / "ylt_dialsup.parquet"
            parquet.write(ylt_dialsup, ylt_dialsup_path)
            ylt_dialsup = pl.scan_parquet(ylt_dialsup_path)

            ylt_blended = int_ylt_main_blended.Model.transform(
                ylt_ranked, ep_blending_targets
            )
            ylt_localccy = int_ylt_main_local_currency.Model.transform(
                ylt_blended, gbp_fx_rates
            )
            ylt_localccy_forecast = int_ylt_main_forecast.Model.transform(
                ylt_localccy,
                forecast_dates,
                forecast_factors,
            )
            ylt_euws = int_ylt_main_euws.Model.transform(
                ylt_localccy_forecast,
                verisk_events,
                seeds,
            )
            ylt_euws_override = int_ylt_main_euws_override.Model.transform(
                ylt_euws, seeds
            )
            ylt = int_ylt_main_metric_stream.Model.transform(
                ylt_ranked,
                ylt_blended,
                ylt_localccy,
                ylt_localccy_forecast,
                ylt_euws,
                ylt_euws_override,
            )
            ylt_path = work_dir / "ylt_combined_all_factors.parquet"
            parquet.write(ylt, ylt_path)
            ylt = pl.scan_parquet(ylt_path)
            intermediate_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {
                "ep_blending_target_points": ep_blending_target_points,
                "ep_blending_targets": ep_blending_targets,
                "ep_blending_weights": ep_blending_weights,
                "ep_summaries_dialsup": ep_selected_dialsup,
                "ep_summaries_enriched": ep_enriched,
                "ep_summaries_main": ep_selected_main,
                "ep_vendor_joined": joined_ep_summaries,
                "forecast_dates": forecast_dates,
                "ylt_dialsup_base_selected": ylt_original_dialsup,
                "ylt_dialsup_enriched": enriched_ylt_dialsup,
                "ylt_dialsup_factor_base": dialsup_factor_base,
                "ylt_dialsup_forecast_metric": dialsup_localccy_forecast,
                "ylt_dialsup_local_currency_metric": dialsup_localccy,
                "ylt_dialsup_metric_stream": ylt_dialsup,
                "ylt_dialsup_original_metric": dialsup_original,
                "ylt_dialsup_ranked": ylt_ranked_dialsup,
                "ylt_main_base_selected": ylt_original,
                "ylt_main_blended": ylt_blended,
                "ylt_main_enriched": enriched_ylt,
                "ylt_main_euws": ylt_euws,
                "ylt_main_euws_override": ylt_euws_override,
                "ylt_main_forecast": ylt_localccy_forecast,
                "ylt_main_local_currency": ylt_localccy,
                "ylt_main_metric_stream": ylt,
                "ylt_main_ranked": ylt_ranked,
                "ylt_normalized": normalized_ylt,
            }
            logger.info(
                "intermediate summary frames=%d",
                len(intermediate_frames),
                extra={
                    "event": "intermediate_summary",
                    "frames": len(intermediate_frames),
                },
            )

        with _logged_phase("marts"):
            threshold = config.outputs.minimum_event_loss_threshold
            ylt_thresholded = mart_ylt_main_long.Model.transform(ylt, threshold)
            ylt_dialsup_thresholded = mart_ylt_dialsup_long.Model.transform(
                ylt_dialsup, threshold
            )
            main_fanout = mart_main_fanout.Model.transform(
                ylt_thresholded, risklink_events
            )

            dialsup_fanout = mart_dialsup_fanout.Model.transform(
                ylt_dialsup_thresholded, risklink_events
            )

            event_validation = mart_event_validation.Model.transform(
                main_fanout, dialsup_fanout
            )
            mart_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {
                "dialsup_fanout": dialsup_fanout,
                "event_validation": event_validation,
                "main_fanout": main_fanout,
                "ylt_dialsup_long": ylt_dialsup_thresholded,
                "ylt_main_long": ylt_thresholded,
            }

        if debug:
            with _logged_phase("debug_outputs"):
                debug_writer.write(
                    output_root,
                    sources=source_frames,
                    seeds=seed_frames,
                    staging=staging_frames,
                    intermediate=intermediate_frames,
                    marts=mart_frames,
                )

        with _logged_phase("write_outputs"):
            combined_path = output_root / COMBINED_YLT_FILE
            dialsup_path = output_root / DIALSUP_YLT_FILE
            event_path = output_root / EVENT_VALIDATION_FILE
            wide_path = output_root / WIDE_YLT_FILE
            marts_dir = output_root / MARTS_DIR
            for frame, path in [
                (ylt_thresholded, combined_path),
                (ylt_dialsup_thresholded, dialsup_path),
                (event_validation, event_path),
            ]:
                parquet.write(frame, path)
            wide_output.write(combined_path, dialsup_path, wide_path)
            fanout_partitions.write(
                {"main_fanout": main_fanout, "dialsup_fanout": dialsup_fanout},
                marts_dir,
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
