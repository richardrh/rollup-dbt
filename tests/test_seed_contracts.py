from __future__ import annotations

import csv
from pathlib import Path


def test_europe_flood_seed_mapping_contract() -> None:
    seed_path = Path(__file__).resolve().parents[1] / "data" / "seeds" / "business" / "perils.csv"

    with seed_path.open(newline="", encoding="utf-8") as seed_file:
        region_peril_rows = [
            row
            for row in csv.DictReader(seed_file)
            if row["region_peril_id"] == "216"
        ]

    assert {row["modelled_peril"] for row in region_peril_rows} == {
        "BE FL",
        "DE FL",
        "EU FL HD",
        "EU_FL",
    }
    assert {row["rollup_peril"] for row in region_peril_rows} == {"Europe_FL"}
    assert {row["base_model"] for row in region_peril_rows} == {"risklink"}
    assert {row["blend_subregion_peril_id"] for row in region_peril_rows} == {"216b"}
