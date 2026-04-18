"""End-to-end smoke test of `ep_curve_from_ylt` on a tiny synthetic YLT."""

from __future__ import annotations

import polars as pl

from rollup.schemas import frames as F
from rollup.schemas.columns import EpCurveCol as EP
from rollup.schemas.columns import NormalizedYltCol as Y
from rollup.stages.ep import ep_curve_from_ylt


def _tiny_ylt() -> pl.LazyFrame:
    """Five synthetic years, one vendor/lob/peril, with known AAL and rankings."""
    rows = []
    losses_by_year = {1: 100.0, 2: 50.0, 3: 200.0, 4: 25.0, 5: 75.0}
    for year_id, loss in losses_by_year.items():
        rows.append({
            Y.VENDOR: "verisk",
            Y.LOB_ID: 1,
            Y.MODELLED_LOB: "lob1",
            Y.ROLLUP_LOB: "rollup1",
            Y.LOB_TYPE: "prop",
            Y.CDS_CAT_CLASS_NAME: "class1",
            Y.REGION_PERIL_ID: 1,
            Y.MODELLED_REGION_PERIL: "rp1",
            Y.ROLLUP_REGION_PERIL: "rollup_rp1",
            Y.MODEL_CODE: 0,
            Y.YEAR_ID: year_id,
            Y.EVENT_ID: year_id * 10,
            Y.LOSS: loss,
        })
    return pl.DataFrame(rows, schema=F.NORMALIZED_YLT).lazy()


def test_ep_curve_aal_equals_total_over_n():
    n = 5
    out = ep_curve_from_ylt(_tiny_ylt(), n_simulations=n).collect()
    aal = out.filter(pl.col(EP.EP_TYPE) == "AAL")
    assert aal.height == 1
    # 100 + 50 + 200 + 25 + 75 = 450; AAL = 450 / 5 = 90
    assert aal[EP.ANNUAL_LOSS][0] == 90.0
    assert aal[EP.RANK_NUM][0] == 0
    assert aal[EP.RETURN_PERIOD][0] == 0


def test_ep_curve_output_matches_schema():
    out = ep_curve_from_ylt(_tiny_ylt(), n_simulations=5).collect()
    # validate_schema already runs inside ep_curve_from_ylt but double-check:
    assert out.schema == F.EP_CURVE


def test_ep_curve_oep_ranking():
    """With one event per year the OEP = AEP loss, ranked descending."""
    out = (
        ep_curve_from_ylt(_tiny_ylt(), n_simulations=5, target_return_periods=[5, 2, 1])
        .collect()
        .filter(pl.col(EP.EP_TYPE) == "OEP")
        .sort(EP.RANK_NUM)
    )
    # losses ranked: 200, 100, 75, 50, 25
    # rank 1 -> rp=5, rank 2 -> rp=2, rank 5 -> rp=1
    losses_by_rank = dict(zip(out[EP.RANK_NUM].to_list(), out[EP.ANNUAL_LOSS].to_list()))
    assert losses_by_rank[1] == 200.0
    assert losses_by_rank[2] == 100.0
    assert losses_by_rank[5] == 25.0
