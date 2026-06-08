from __future__ import annotations

from pathlib import Path

import polars as pl

from rollup.config import RollupConfig
from rollup.intermediate.build_dialsup import DIALSUP_SCHEMA
from rollup.intermediate.build_metric_long import METRIC_LONG_SCHEMA
from rollup.marts.event_validation import event_validation
from rollup.marts.fanouts import write_fanouts
from rollup.marts.wide import wide
from rollup.schemas import require_columns


COMBINED_MART_INPUT_SCHEMA = METRIC_LONG_SCHEMA
DIALSUP_MART_INPUT_SCHEMA = DIALSUP_SCHEMA


def write_marts(
    output_root: Path,
    combined: pl.LazyFrame,
    dialsup: pl.LazyFrame,
    config: RollupConfig,
) -> dict[str, Path | tuple[Path, ...]]:
    require_columns(combined, COMBINED_MART_INPUT_SCHEMA)
    require_columns(dialsup, DIALSUP_MART_INPUT_SCHEMA)

    marts_dir = config.outputs.marts_path(output_root)
    marts_dir.mkdir(parents=True, exist_ok=True)
    combined_path = marts_dir / config.outputs.combined_file
    wide_path = marts_dir / config.outputs.wide_file
    dialsup_path = marts_dir / config.outputs.dialsup_file
    event_validation_path = marts_dir / config.outputs.event_validation_file

    combined_df = combined.collect()
    dialsup_df = dialsup.collect()
    combined_df.write_parquet(combined_path)
    wide(combined_df).write_parquet(wide_path)
    dialsup_df.write_parquet(dialsup_path)
    event_validation(combined_df).write_parquet(event_validation_path)
    fanout_paths = write_fanouts(marts_dir, combined_df)

    return {
        "combined": combined_path,
        "wide": wide_path,
        "dialsup": dialsup_path,
        "event_validation": event_validation_path,
        "fanouts": fanout_paths,
    }
