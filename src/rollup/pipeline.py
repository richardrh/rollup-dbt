from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import logging

from rollup.config import RollupConfig, load_config
from rollup.intermediate import (
    apply_blending,
    apply_euws,
    apply_forecast,
    apply_fx,
    build_dialsup,
    build_enriched_ylt,
    build_metric_long,
)
from rollup.marts import write_marts, write_stage_frames
from rollup.staging import load_sources, normalize_ylt, stage_ep_summaries

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineRunResult:
    data_root: Path
    output_root: Path
    config: RollupConfig
    stage_paths: tuple[Path, ...]
    mart_paths: dict[str, Path | tuple[Path, ...]]


def run(
    data_root: str | Path = "data",
    *,
    output_root: str | Path = "output",
    config_path: str | Path | None = None,
    config: RollupConfig | None = None,
) -> PipelineRunResult:
    data_root = Path(data_root)
    output_root = Path(output_root)
    config = config or load_config(config_path)

    logger.info("loading staging inputs data_root=%s", data_root)
    sources = load_sources(data_root)
    normalized = normalize_ylt(sources)
    staged_ep = stage_ep_summaries(sources)
    enriched = build_enriched_ylt(normalized, staged_ep)
    blended = apply_blending(enriched, sources.blending)
    fx_applied = apply_fx(blended, sources.fx_rates)
    forecast_applied = apply_forecast(fx_applied, sources.forecast_factors)
    euws_applied = apply_euws(forecast_applied, sources.euws_factors)
    combined = build_metric_long(euws_applied)
    dialsup = build_dialsup(combined)

    stage_paths = (
        *write_stage_frames(
            output_root,
            config.outputs.staging_dir,
            {
                "verisk_ylt": sources.verisk_ylt,
                "risklink_ylt": sources.risklink_ylt,
                "ep_summaries": sources.ep_summaries,
                "lobs": sources.lobs,
                "perils": sources.perils,
                "normalized_ylt": normalized,
                "staged_ep_summaries": staged_ep,
            },
            config,
        ),
        *write_stage_frames(
            output_root,
            config.outputs.intermediate_dir,
            {
                "enriched_ylt": enriched,
                "blended_ylt": blended,
                "fx_applied_ylt": fx_applied,
                "forecast_applied_ylt": forecast_applied,
                "euws_applied_ylt": euws_applied,
            },
            config,
        ),
    )
    mart_paths = write_marts(output_root, combined, dialsup, config)
    return PipelineRunResult(
        data_root=data_root,
        output_root=output_root,
        config=config,
        stage_paths=stage_paths,
        mart_paths=mart_paths,
    )
