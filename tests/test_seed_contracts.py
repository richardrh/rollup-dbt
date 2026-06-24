from __future__ import annotations

from pathlib import Path

import polars as pl

from rollup.columns import Col


def test_europe_flood_region_peril_216_rolls_up_to_risklink_base() -> None:
    perils = pl.read_csv(
        Path(__file__).parents[1] / "data" / "seeds" / "business" / "perils.csv"
    )

    europe_flood = perils.filter(pl.col(Col.region_peril_id) == 216)

    assert set(europe_flood[Col.modelled_peril].to_list()) == {
        "BE FL",
        "DE FL",
        "EU FL HD",
        "EU_FL",
    }
    assert europe_flood[Col.rollup_peril].unique().to_list() == ["Europe_FL"]
    assert europe_flood[Col.base_model].unique().to_list() == ["risklink"]
