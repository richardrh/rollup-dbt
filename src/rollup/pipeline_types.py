from __future__ import annotations
# mypy: ignore-errors

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class PipelineRunResult:
    seeds: dict[str, pl.DataFrame | pl.LazyFrame]
    staging: dict[str, pl.DataFrame | pl.LazyFrame]
    intermediate: dict[str, pl.DataFrame | pl.LazyFrame]
    marts: dict[str, pl.DataFrame | pl.LazyFrame]


@dataclass(frozen=True)
class PipelineValidationInputs:
    seeds: dict[str, pl.LazyFrame]
    ylts: dict[str, pl.LazyFrame]
    ep_summaries: pl.LazyFrame
    coverage_report: pl.DataFrame
