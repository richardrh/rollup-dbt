from __future__ import annotations

from pathlib import Path

import polars as pl

from rollup.columns import Col
from rollup.config import RollupConfig


def write_stage_frames(
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
        write_parquet(frame, path)
        paths.append(path)
    return tuple(paths)


def write_marts(
    output_root: Path,
    combined: pl.LazyFrame,
    dialsup: pl.LazyFrame,
    config: RollupConfig,
) -> dict[str, Path | tuple[Path, ...]]:
    marts_dir = config.outputs.marts_path(output_root)
    marts_dir.mkdir(parents=True, exist_ok=True)
    combined_path = marts_dir / config.outputs.combined_file
    wide_path = marts_dir / config.outputs.wide_file
    dialsup_path = marts_dir / config.outputs.dialsup_file
    event_validation_path = marts_dir / config.outputs.event_validation_file

    combined_df = combined.collect()
    dialsup_df = dialsup.collect()
    combined_df.write_parquet(combined_path)
    _wide(combined_df).write_parquet(wide_path)
    dialsup_df.write_parquet(dialsup_path)
    _event_validation(combined_df).write_parquet(event_validation_path)
    fanout_paths = _write_fanouts(marts_dir, combined_df)

    return {
        "combined": combined_path,
        "wide": wide_path,
        "dialsup": dialsup_path,
        "event_validation": event_validation_path,
        "fanouts": fanout_paths,
    }


def write_parquet(frame: pl.DataFrame | pl.LazyFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(frame, pl.LazyFrame):
        frame.collect().write_parquet(path)
    else:
        frame.write_parquet(path)


def _wide(frame: pl.DataFrame) -> pl.DataFrame:
    index = [
        Col.base_model,
        Col.analysis_id,
        Col.rollup_lob,
        Col.rollup_peril,
        Col.year_id,
        Col.event_id,
        Col.forecast_date,
    ]
    return frame.pivot(index=index, on=Col.metric, values=Col.loss, aggregate_function="sum")


def _event_validation(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.select(
        Col.base_model,
        Col.event_id,
        pl.col(Col.year_id).is_null().alias(Col.missing_model_event_day),
    ).unique()


def _write_fanouts(marts_dir: Path, frame: pl.DataFrame) -> tuple[Path, ...]:
    paths: list[Path] = []
    if frame.is_empty():
        return ()
    for row in frame.select(Col.base_model, Col.forecast_date).unique().iter_rows(named=True):
        subset = frame.filter(
            (pl.col(Col.base_model) == row[Col.base_model])
            & (pl.col(Col.forecast_date) == row[Col.forecast_date])
            & (pl.col(Col.metric) == "euws_override")
        )
        vendor = "HiscoAIR" if row[Col.base_model] == "verisk" else "HiscoRMS"
        forecast = str(row[Col.forecast_date]).replace("-", "")
        path = marts_dir / f"{vendor}_{forecast}_main.parquet"
        subset.write_parquet(path)
        paths.append(path)
    return tuple(sorted(paths))
