"""Example anti-join helpers for input sanity checks.

These functions are not part of the packaged rollup runtime. They are provided
as a reference for the upstream calling application that now owns input/seed
validation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Tuple

import polars as pl


def anti_join_dimensions(
    input_file: str | Path,
    peril_seed_file: str | Path,
    lob_seed_file: str | Path,
) -> Tuple[list[str], list[str]]:
    """Return modelled_perils and modelled_lobs present in ``input_file`` but missing from seeds.

    The input file may be an EP summary CSV or a YLT parquet file. The function
    reads ``modelled_peril`` and ``modelled_lob`` columns from the input and
    performs an anti-join against the ``perils.csv``/``lobs.csv`` style seed
    files, which are expected to contain ``modelled_peril`` and ``modelled_lob``
    columns respectively.

    Returns:
        A tuple of ``(missing_perils, missing_lobs)``. Each list contains the
        distinct dimension values from the input that do not exist in the
        corresponding seed file.
    """
    input_frame = _read_input(Path(input_file))

    def read_seed(path: Path, dimension: str) -> pl.LazyFrame:
        if path.suffix.lower() == ".parquet":
            return pl.scan_parquet(path).select(pl.col(dimension).cast(pl.String))
        if path.suffix.lower() == ".csv":
            return pl.scan_csv(path).select(pl.col(dimension).cast(pl.String))
        raise ValueError(f"unsupported seed file format: {path}")

    perils_seed = read_seed(Path(peril_seed_file), "modelled_peril")
    lobs_seed = read_seed(Path(lob_seed_file), "modelled_lob")

    missing_perils = _missing_values(
        input_frame, perils_seed, dimension="modelled_peril"
    )
    missing_lobs = _missing_values(
        input_frame, lobs_seed, dimension="modelled_lob"
    )

    return missing_perils, missing_lobs


def _read_input(path: Path) -> pl.LazyFrame:
    if path.suffix.lower() == ".parquet":
        return pl.scan_parquet(path)
    if path.suffix.lower() == ".csv":
        return pl.scan_csv(path)
    raise ValueError(f"unsupported input file format: {path}")


def _missing_values(
    input_frame: pl.LazyFrame,
    seed_frame: pl.LazyFrame,
    *,
    dimension: str,
) -> list[str]:
    if dimension not in input_frame.collect_schema().names():
        return []

    missing = (
        input_frame.select(pl.col(dimension).cast(pl.String).alias(dimension))
        .unique()
        .join(
            seed_frame.select(pl.col(dimension)),
            on=dimension,
            how="anti",
        )
        .filter(pl.col(dimension).is_not_null())
        .sort(dimension)
        .collect()
    )

    return missing[dimension].to_list()
