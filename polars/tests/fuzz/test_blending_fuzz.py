"""Property-based tests for ``rollup.stages.blending``.

Targets ``derive_blending_weights`` and the underlying proportioning logic.

The proportion invariant (rl_proportion + vk_proportion == 1.0) is the
highest-priority property here — it directly guards the historical
divide-by-zero incident in blending.py:139.

All tests are marked ``@pytest.mark.fuzz`` and skipped unless
``--run-fuzz`` is passed.

# TODO (Phase 5 mutation fuzzer): The label case-sensitivity scenario
# (peril label in EP-summary does not match analyses.modelled_label due to
# case difference) is hard to assert generically because the current code
# silently drops unmapped rows and logs a warning. A Phase 5 mutation fuzzer
# would inject upper-cased labels and assert the warning is logged, or
# alternatively assert that row counts differ — deferred because testing log
# output is brittle and the functional guard is already in _aal_by_peril.
"""

from __future__ import annotations

import csv
import io
import tempfile
from pathlib import Path

import polars as pl
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import rollup.schemas.frames as F
from rollup.config import VendorName
from rollup.schemas.columns import (
    AnalysesCol as AN,
    BlendingWeightsCol as BW,
    PerilsCol as P,
)
from rollup.stages.blending import derive_blending_weights
from rollup.validate import validate_schema

from .strategies import lazyframe_from_schema, realistic_loss_strategy


# ---------------------------------------------------------------------------
# Internal helpers: build minimal EP CSV bytes per vendor
# ---------------------------------------------------------------------------

def _rl_ep_csv_bytes(peril_labels: list[str], aals: list[float]) -> bytes:
    """RiskLink EP long-format CSV with one AAL row per peril label.
    Column used as peril label: ``region_peril`` (matches ``StgRisklinkEpCol.REGION_PERIL``).
    """
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id", "rp", "ep_type", "lob", "region_peril", "gl"])
    for idx, (label, aal) in enumerate(zip(peril_labels, aals), start=1):
        writer.writerow([idx, 0, "AAL", "all", label, aal])
    return buf.getvalue().encode()


def _vk_ep_csv_bytes(peril_labels: list[str], aals: list[float]) -> bytes:
    """Verisk EP long-format CSV with one AAL row per analysis label.
    Column used as peril label: ``analysis`` (matches ``StgVeriskEpCol.ANALYSIS``).
    """
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["rp", "ep_type", "analysis", "lob", "gl"])
    for label, aal in zip(peril_labels, aals):
        writer.writerow([0, "AAL", label, "all", aal])
    return buf.getvalue().encode()


def _analyses_df(
    vendor: VendorName,
    peril_labels: list[str],
    peril_ids: list[int],
) -> pl.DataFrame:
    """Minimal analyses DataFrame: numeric IDs plus vendor modelled labels."""
    if vendor == VendorName.RISKLINK:
        analysis_ids = [str(i) for i in range(1, len(peril_labels) + 1)]
    else:
        analysis_ids = [str(900000 + i) for i in range(1, len(peril_labels) + 1)]
    return pl.DataFrame({
        AN.VENDOR:         [vendor.value] * len(peril_labels),
        AN.ANALYSIS_ID:    analysis_ids,
        AN.MODELLED_LABEL: peril_labels,
        AN.PERIL_ID:       peril_ids,
        AN.LOB_ID:         [None] * len(peril_labels),
    }, schema=F.ANALYSES)


def _perils_df(peril_ids: list[int]) -> pl.DataFrame:
    """Minimal perils DataFrame."""
    return pl.DataFrame({
        P.PERIL_ID:     peril_ids,
        P.NAME:         [f"peril_{pid}" for pid in peril_ids],
        P.REGION:       ["UK"] * len(peril_ids),
        P.PERIL_FAMILY: ["EQ"] * len(peril_ids),
    }, schema=F.PERILS)


# ---------------------------------------------------------------------------
# Properties: blending proportions sum to 1.0
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@given(
    n_perils=st.integers(min_value=1, max_value=5),
    rl_aals=st.lists(realistic_loss_strategy(), min_size=1, max_size=5),
    vk_aals=st.lists(realistic_loss_strategy(), min_size=1, max_size=5),
)
@settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_blending_weights_sum_to_one(
    n_perils: int,
    rl_aals: list[float],
    vk_aals: list[float],
) -> None:
    """For every peril row in ``derive_blending_weights`` output,
    rl_proportion + vk_proportion == 1.0 (within float tolerance).
    Even when one vendor's AAL is zero (handled by 0.5 fallback).
    """
    n_rl = min(len(rl_aals), n_perils)
    n_vk = min(len(vk_aals), n_perils)
    n_shared = max(n_rl, n_vk)

    peril_ids = list(range(100, 100 + n_shared))
    labels_rl = [f"RL_{pid}" for pid in peril_ids[:n_rl]]
    labels_vk = [f"VK_{pid}" for pid in peril_ids[:n_vk]]

    rl_csv_bytes = _rl_ep_csv_bytes(labels_rl, rl_aals[:n_rl])
    vk_csv_bytes = _vk_ep_csv_bytes(labels_vk, vk_aals[:n_vk])

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        rl_csv = tmp / "rl_ep.csv"
        vk_csv = tmp / "vk_ep.csv"
        rl_csv.write_bytes(rl_csv_bytes)
        vk_csv.write_bytes(vk_csv_bytes)

        analyses = pl.concat([
            _analyses_df(VendorName.RISKLINK, labels_rl, peril_ids[:n_rl]),
            _analyses_df(VendorName.VERISK,   labels_vk, peril_ids[:n_vk]),
        ])
        perils = _perils_df(peril_ids)

        out = derive_blending_weights([rl_csv], [vk_csv], analyses, perils)

    pivoted = out.pivot(on=BW.VENDOR, index=BW.PERIL_ID, values=BW.WEIGHT)
    weight_cols = [c for c in pivoted.columns if c != BW.PERIL_ID]
    assert len(weight_cols) == 2, f"Expected 2 vendor columns; got {weight_cols}"
    sums = (pivoted[weight_cols[0]] + pivoted[weight_cols[1]]).to_list()
    for peril_sum in sums:
        assert abs(peril_sum - 1.0) < 1e-9, (
            f"Blending weights do not sum to 1.0 for a peril: {peril_sum}"
        )


@pytest.mark.fuzz
@given(n_perils=st.integers(min_value=1, max_value=4))
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_blending_weights_handle_zero_aals(n_perils: int) -> None:
    """When a peril has zero AAL on one vendor, blending falls back to 0.5/0.5
    instead of dividing by zero — no NaN, no negative weights."""
    peril_ids = list(range(100, 100 + n_perils))
    labels_rl = [f"RL_{pid}" for pid in peril_ids]
    labels_vk = [f"VK_{pid}" for pid in peril_ids]

    # RiskLink zero AAL; Verisk positive AAL.
    rl_csv_bytes = _rl_ep_csv_bytes(labels_rl, [0.0] * n_perils)
    vk_csv_bytes = _vk_ep_csv_bytes(labels_vk, [1000.0] * n_perils)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        rl_csv = tmp / "rl_zero.csv"
        vk_csv = tmp / "vk_nonzero.csv"
        rl_csv.write_bytes(rl_csv_bytes)
        vk_csv.write_bytes(vk_csv_bytes)

        analyses = pl.concat([
            _analyses_df(VendorName.RISKLINK, labels_rl, peril_ids),
            _analyses_df(VendorName.VERISK,   labels_vk, peril_ids),
        ])
        perils = _perils_df(peril_ids)

        out = derive_blending_weights([rl_csv], [vk_csv], analyses, perils)

    assert out.height == n_perils * 2

    weight_vals = out[BW.WEIGHT].to_list()
    for w in weight_vals:
        assert not (w != w), f"NaN weight detected: {w}"
        assert w >= 0.0, f"Negative weight detected: {w}"


@pytest.mark.fuzz
@given(
    weights=lazyframe_from_schema(F.BLENDING_WEIGHTS, min_rows=0, max_rows=20),
)
def test_blending_weights_schema_valid(weights: pl.LazyFrame) -> None:
    """Any frame whose schema matches ``BLENDING_WEIGHTS`` passes
    ``validate_schema`` — verifies the strategy generates valid frames."""
    validate_schema(weights, F.BLENDING_WEIGHTS, name="blending_weights_fuzz")
