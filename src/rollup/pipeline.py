from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import logging
import polars as pl

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
from rollup.marts import write_marts
from rollup.marts.fanouts import dialsup_fanout_source, main_fanout_source
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
    blended = apply_blending(
        enriched,
        staged_ep,
        sources.blending,
        config.blending,
    )
    fx_applied = apply_fx(blended, sources.fx_rates, config.fx.target_currency)
    forecast_applied = apply_forecast(fx_applied, sources.forecast_factors)
    euws_applied = apply_euws(
        forecast_applied,
        sources.verisk_events,
        sources.euws_factors,
        sources.euws_overrides,
    )
    combined = build_metric_long(euws_applied, config.fx.target_currency)
    dialsup = build_dialsup(euws_applied, config.fx.target_currency)
    main_fanout = main_fanout_source(euws_applied, config.fx.target_currency)
    dialsup_fanout = dialsup_fanout_source(euws_applied, config.fx.target_currency)

    stage_paths = (
        *_write_stage_frames(
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
        *_write_stage_frames(
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
    mart_paths = write_marts(
        output_root,
        combined,
        dialsup,
        config,
        sources.risklink_flood_events,
        main_fanout,
        dialsup_fanout,
    )
    return PipelineRunResult(
        data_root=data_root,
        output_root=output_root,
        config=config,
        stage_paths=stage_paths,
        mart_paths=mart_paths,
    )


def _write_stage_frames(
    output_root: Path,
    section: str,
    frames: dict[str, pl.DataFrame | pl.LazyFrame],
    config: RollupConfig,
) -> tuple[Path, ...]:
    if not config.outputs.write_stage_outputs:
        return ()
    base = output_root / config.outputs.stage_output_dir / section
    base.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, frame in frames.items():
        path = base / f"{name}.parquet"
        _write_parquet(frame, path)
        paths.append(path)
    return tuple(paths)


def _write_parquet(frame: pl.DataFrame | pl.LazyFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(frame, pl.LazyFrame):
        frame.sink_parquet(path, mkdir=True)
        return
    frame.write_parquet(path)
