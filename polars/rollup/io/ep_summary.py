"""Convert wide EP-summary xlsx exports into long-format CSVs that match the
STG_RISKLINK_EP / STG_VERISK_EP schemas.

Source: 'OEPAEP Curves' sheet. Title decoration in rows 1-6, header at row 7,
data from row 8 onward. Wide layout has one column per (ep_type, return_period):
AAL, OEP_2, OEP_5, ..., OEP_10000, AEP_2, ..., AEP_10000.

Long format: one row per (id, rp, ep_type, lob, region_peril, gl) for risklink.
- ep_type in {'AAL', 'OEP', 'AEP'}
- rp = 0 for AAL rows, otherwise the return-period integer (2, 5, 10, ...)
"""
from __future__ import annotations

from pathlib import Path

import openpyxl
import polars as pl

from rollup.config import VendorName
from rollup.schemas.columns import StgRisklinkEpCol as RL
from rollup.schemas.columns import StgVeriskEpCol as VK  # noqa: F401 — reserved for future use


_HEADER_ROW = 7   # 1-indexed
_DATA_START_ROW = 8
_EP_SHEET = "OEPAEP Curves"


def _clean(v):
    """Return None for blank/empty/dash values; otherwise return v unchanged."""
    if v is None:
        return None
    if isinstance(v, str):
        stripped = v.strip()
        return None if stripped == "" or stripped == "-" else stripped
    return v


def read_risklink_ep_summary(path: Path) -> pl.DataFrame:
    """Read a RiskLink EP-summary xlsx, return long-format DataFrame matching STG_RISKLINK_EP.

    Reads the 'OEPAEP Curves' sheet. Header row is row 7 (1-indexed); data
    begins at row 8. Returns one row per (id, ep_type, rp, lob, region_peril).

    Raises KeyError if the sheet is absent, ValueError if required columns are
    missing or there are fewer rows than expected.
    """
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        ws = wb[_EP_SHEET]
        rows = list(ws.iter_rows(values_only=True))
    finally:
        wb.close()

    if len(rows) < _DATA_START_ROW:
        raise ValueError(
            f"{path}: '{_EP_SHEET}' has fewer than {_DATA_START_ROW} rows"
        )

    header = list(rows[_HEADER_ROW - 1])
    data = [list(r) for r in rows[_DATA_START_ROW - 1:]]

    # Map column name -> index. Skip leading/trailing blank columns.
    col_idx: dict[str, int] = {
        name: i
        for i, name in enumerate(header)
        if isinstance(name, str) and name.strip()
    }

    for required in ("ID", "LOB", "RegionPeril", "AAL"):
        if required not in col_idx:
            raise ValueError(
                f"{path}: missing required column {required!r} in header row {_HEADER_ROW}"
            )

    # Identify OEP_N and AEP_N columns by prefix; sort by return period.
    oep_cols = sorted(
        (
            (col_idx[c], int(c.split("_", 1)[1]), "OEP")
            for c in col_idx
            if c.startswith("OEP_")
        ),
        key=lambda t: t[1],
    )
    aep_cols = sorted(
        (
            (col_idx[c], int(c.split("_", 1)[1]), "AEP")
            for c in col_idx
            if c.startswith("AEP_")
        ),
        key=lambda t: t[1],
    )

    out_rows: list[dict] = []

    for row in data:
        # Skip entirely blank rows (trailing blank rows in the sheet).
        if all(
            c is None or (isinstance(c, str) and c.strip() == "") for c in row
        ):
            continue

        id_raw = _clean(row[col_idx["ID"]]) if col_idx["ID"] < len(row) else None
        if id_raw is None:
            continue
        try:
            id_int = int(id_raw)
        except (TypeError, ValueError):
            continue

        lob = _clean(row[col_idx["LOB"]]) if col_idx["LOB"] < len(row) else None
        region_peril = (
            _clean(row[col_idx["RegionPeril"]])
            if col_idx["RegionPeril"] < len(row)
            else None
        )

        # --- AAL row (rp = 0) ---
        aal_raw = _clean(row[col_idx["AAL"]]) if col_idx["AAL"] < len(row) else None
        if aal_raw is not None:
            try:
                out_rows.append(
                    {
                        RL.ID:           id_int,
                        RL.RP:           0,
                        RL.EP_TYPE:      "AAL",
                        RL.LOB:          lob,
                        RL.REGION_PERIL: region_peril,
                        RL.GL:           float(aal_raw),
                    }
                )
            except (TypeError, ValueError):
                pass

        # --- OEP and AEP rows ---
        for idx, rp, kind in oep_cols + aep_cols:
            v = _clean(row[idx]) if idx < len(row) else None
            if v is None:
                continue
            try:
                out_rows.append(
                    {
                        RL.ID:           id_int,
                        RL.RP:           rp,
                        RL.EP_TYPE:      kind,
                        RL.LOB:          lob,
                        RL.REGION_PERIL: region_peril,
                        RL.GL:           float(v),
                    }
                )
            except (TypeError, ValueError):
                continue

    return pl.DataFrame(
        out_rows,
        schema={
            RL.ID:           pl.Int64,
            RL.RP:           pl.Int64,
            RL.EP_TYPE:      pl.String,
            RL.LOB:          pl.String,
            RL.REGION_PERIL: pl.String,
            RL.GL:           pl.Float64,
        },
    )


def write_long_csv(df: pl.DataFrame, output: Path) -> None:
    """Write `df` as CSV to `output`, creating parent directories as needed."""
    output.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output)


def convert_ep_summaries_to_csv(ep_dir: Path, vendor: VendorName) -> list[Path]:
    """For each xlsx under `ep_dir`, write a sibling `<stem>.long.csv`.

    Returns the list of CSV paths written. Skips xlsx files that don't have
    an 'OEPAEP Curves' sheet (e.g. portfolio-list workbooks) and any xlsx that
    raises a ValueError during parsing.
    """
    written: list[Path] = []
    if not ep_dir.exists():
        return written

    for xlsx in sorted(ep_dir.glob("*.xlsx")):
        try:
            if vendor == VendorName.RISKLINK:
                df = read_risklink_ep_summary(xlsx)
            else:
                # Verisk reader to be added once a sample xlsx is available.
                continue
        except (KeyError, ValueError):
            continue

        out = xlsx.with_name(xlsx.stem + ".long.csv")
        write_long_csv(df, out)
        written.append(out)

    return written
