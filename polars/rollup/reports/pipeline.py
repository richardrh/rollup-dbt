"""Pipeline XLSX reporting and summary EP statistics helpers."""

from __future__ import annotations

from pathlib import Path

import polars as pl


def prepare_summary_ep_stats(loss_summary: pl.LazyFrame) -> pl.LazyFrame:
    """Prepare summary EP-style statistics for report generation hooks."""

    return loss_summary.select(
        pl.len().cast(pl.UInt32).alias("analysis_count"),
        pl.col("total_loss").sum().alias("portfolio_total_loss"),
        pl.col("event_count").sum().cast(pl.UInt32).alias("portfolio_event_count"),
    )


def write_xlsx_report(
    *,
    path: Path | str,
    loss_summary: pl.DataFrame,
    summary_ep_stats: pl.DataFrame,
) -> None:
    """Write the collected mart output and summary EP stats to an XLSX workbook."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pl.ExcelWriter(output_path) as workbook:
        loss_summary.write_excel(workbook=workbook, worksheet="loss_summary")
        summary_ep_stats.write_excel(workbook=workbook, worksheet="summary_ep_stats")
