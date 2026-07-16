import csv
from pathlib import Path


PERILS_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "seeds" / "business" / "perils.csv"
)


def _peril_rows() -> list[dict[str, str]]:
    with PERILS_PATH.open(newline="") as perils_file:
        return list(csv.DictReader(perils_file))


def test_perils_seed_uses_master_structure() -> None:
    with PERILS_PATH.open(newline="") as perils_file:
        header = next(csv.reader(perils_file))

    assert header == [
        "modelled_peril",
        "rollup_peril",
        "region",
        "peril",
        "region_peril_id",
        "blend_subregion_peril_id",
        "base_model",
        "selection_priority",
        "is_dialsup",
        "is_euws",
    ]


def test_peril_216_rows_map_to_europe_fl_risklink() -> None:
    rows = {
        row["modelled_peril"]: row
        for row in _peril_rows()
        if row["modelled_peril"] in {"BE FL", "DE FL", "EU_FL"}
    }

    assert set(rows) == {"BE FL", "DE FL", "EU_FL"}
    for row in rows.values():
        assert row["rollup_peril"] == "Europe_FL"
        assert row["region_peril_id"] == "216"
        assert row["blend_subregion_peril_id"] == "216b"
        assert row["base_model"] == "risklink"
