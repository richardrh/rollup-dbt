"""Tests for rollup.io.ep_summary — risklink EP-summary xlsx to long-format CSV."""

from __future__ import annotations

import shutil
from pathlib import Path

import polars as pl
import pytest

from rollup.config import VendorName
from rollup.io.ep_summary import convert_ep_summaries_to_csv, read_risklink_ep_summary
from rollup.schemas import frames as F
from rollup.schemas.columns import StgRisklinkEpCol as RL


# ---------------------------------------------------------------------------
# Shared fixture: path to the real risklink EP-summary xlsx.
# ---------------------------------------------------------------------------

_EP_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "ep_summaries" / "risklink" / "rms_ep_summary.xlsx"
)

_SKIP_IF_MISSING = pytest.mark.skipif(
    not _EP_DIR.exists(),
    reason=f"real xlsx not found at {_EP_DIR}",
)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@_SKIP_IF_MISSING
def test_read_risklink_ep_summary_returns_long_format():
    """Result has STG_RISKLINK_EP schema and at least 1 000 rows."""
    df = read_risklink_ep_summary(_EP_DIR)

    # Schema must match STG_RISKLINK_EP exactly.
    assert df.schema == F.STG_RISKLINK_EP, (
        f"schema mismatch.\nExpected: {F.STG_RISKLINK_EP}\nGot:      {df.schema}"
    )

    # 417 data rows × ~25 ep columns (1 AAL + 12 OEP + 12 AEP) > 1000.
    assert df.height >= 1000, f"expected >= 1000 rows, got {df.height}"


@_SKIP_IF_MISSING
def test_read_risklink_ep_summary_aal_rows_have_rp_zero():
    """All rows with ep_type == 'AAL' must have rp == 0."""
    df = read_risklink_ep_summary(_EP_DIR)
    aal = df.filter(pl.col(RL.EP_TYPE) == "AAL")

    assert aal.height > 0, "no AAL rows found"
    bad = aal.filter(pl.col(RL.RP) != 0)
    assert bad.height == 0, (
        f"{bad.height} AAL rows have rp != 0:\n{bad.head(5)}"
    )


@_SKIP_IF_MISSING
def test_read_risklink_ep_summary_oep_rps_are_correct():
    """Distinct rp values for ep_type == 'OEP' must include all 12 standard RPs."""
    expected_rps = {2, 5, 10, 20, 25, 50, 100, 200, 250, 500, 1000, 10000}
    df = read_risklink_ep_summary(_EP_DIR)
    oep_rps = set(
        df.filter(pl.col(RL.EP_TYPE) == "OEP")[RL.RP].unique().to_list()
    )
    missing = expected_rps - oep_rps
    assert not missing, (
        f"OEP return periods missing from output: {sorted(missing)}"
    )


@_SKIP_IF_MISSING
def test_convert_ep_summaries_writes_csv(tmp_path: Path):
    """copy xlsx → tmp_path, run converter, assert .long.csv exists and is readable."""
    # Copy the real xlsx into a temp directory.
    dest_xlsx = tmp_path / "rms_ep_summary.xlsx"
    shutil.copy2(_EP_DIR, dest_xlsx)

    written = convert_ep_summaries_to_csv(tmp_path, VendorName.RISKLINK)

    assert len(written) == 1, f"expected 1 csv written, got {len(written)}: {written}"
    csv_path = written[0]
    assert csv_path.exists(), f"csv not found at {csv_path}"
    assert csv_path.suffix == ".csv"
    assert csv_path.name == "rms_ep_summary.long.csv"

    # Must be readable as a polars DataFrame with the correct schema.
    df = pl.read_csv(csv_path, schema=F.STG_RISKLINK_EP)
    assert df.height >= 1000, f"expected >= 1000 rows, got {df.height}"
