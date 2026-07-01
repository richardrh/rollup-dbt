from __future__ import annotations

from pathlib import Path
import logging

import polars as pl

from rollup.columns import Col
from rollup.config import RollupConfig
from rollup.marts.fanouts import write_fanouts
from rollup.marts.wide import wide
from rollup.metrics import final_main_metric

logger = logging.getLogger(__name__)


def write_marts(
    output_root: Path,
    combined: pl.LazyFrame,
    dialsup: pl.LazyFrame,
    config: RollupConfig,
    risklink_flood_events: pl.LazyFrame | None = None,
) -> dict[str, Path | tuple[Path, ...]]:
    marts_dir = config.outputs.marts_path(output_root)
    marts_dir.mkdir(parents=True, exist_ok=True)
    combined_path = marts_dir / config.outputs.combined_file
    wide_path = marts_dir / config.outputs.wide_file
    dialsup_path = marts_dir / config.outputs.dialsup_file
    target_currency = config.fx.target_currency
    main_metric = final_main_metric(target_currency)

    logger.info("writing combined mart path=%s", combined_path)
    _write_parquet(combined, combined_path)

    combined_scan = pl.scan_parquet(combined_path)
    logger.info("writing dialsup mart path=%s", dialsup_path)
    _write_parquet(dialsup, dialsup_path)

    dialsup_scan = pl.scan_parquet(dialsup_path)
    final_main = combined_scan.filter(pl.col(Col.metric) == main_metric)

    logger.info("writing operational wide mart path=%s", wide_path)
    _write_parquet(wide(combined_scan, target_currency), wide_path)
    logger.info("writing fanout marts dir=%s", marts_dir)
    fanout_paths = write_fanouts(
        marts_dir,
        final_main,
        config.outputs.fanout_prefixes,
        target_currency,
        risklink_flood_events=risklink_flood_events,
    )
    dialsup_fanout_paths = write_fanouts(
        marts_dir,
        dialsup_scan,
        config.outputs.fanout_prefixes,
        target_currency,
        suffix="dialsup",
        risklink_flood_events=risklink_flood_events,
    )
    fanout_paths = (*fanout_paths, *dialsup_fanout_paths)
    logger.info("wrote %s fanout mart(s)", len(fanout_paths))

    return {
        "combined": combined_path,
        "wide": wide_path,
        "dialsup": dialsup_path,
        "fanouts": fanout_paths,
    }


def _write_parquet(frame: pl.DataFrame | pl.LazyFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(frame, pl.LazyFrame):
        frame.sink_parquet(path, mkdir=True)
        return
    frame.write_parquet(path)
