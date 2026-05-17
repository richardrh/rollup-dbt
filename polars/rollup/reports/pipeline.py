"""Pipeline XLSX reporting and summary EP statistics helpers."""

from __future__ import annotations

from pathlib import Path

import polars as pl
from openpyxl import Workbook


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
    workbook = Workbook()
    default_sheet = workbook.active
    default_sheet.title = "loss_summary"
    _append_frame(default_sheet, loss_summary)
    summary_sheet = workbook.create_sheet("summary_ep_stats")
    _append_frame(summary_sheet, summary_ep_stats)
    workbook.save(output_path)


def collect_report_artifact(
    *,
    loss_summary: pl.LazyFrame,
    summary_ep_stats: pl.LazyFrame,
) -> dict[str, pl.DataFrame]:
    """Collect report-ready frames for XLSX writing."""

    return {
        "loss_summary": loss_summary.collect(),
        "summary_ep_stats": summary_ep_stats.collect(),
    }


def _append_frame(worksheet, frame: pl.DataFrame) -> None:
    worksheet.append(frame.columns)
    for row in frame.rows():
        worksheet.append(list(row))
