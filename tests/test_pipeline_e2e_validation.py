from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from rollup.columns import Col, RawCol
from rollup.pipeline import load_risklink_flood_events, load_validated_seed_frames


def test_seed_loading_no_longer_requires_schema_yaml(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    (data_root / "seeds" / "business").mkdir(parents=True)
    pl.DataFrame({Col.modelled_lob: ["LOB"], Col.rollup_lob: ["LOB"]}).write_csv(
        data_root / "seeds" / "business" / "lobs.csv"
    )

    result = load_validated_seed_frames(data_root)

    assert "lobs.csv" in result.frames
    assert result.report.item(0, "valid") is True
    assert not (data_root / "seeds" / "schema.yaml").exists()


def test_load_risklink_flood_events_keeps_model_occurrence_year(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    validation_dir = data_root / "seeds" / "validation"
    validation_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "ModelEventID": [1, 1],
            RawCol.ModelOccurrenceYear: [2025, 2026],
            RawCol.RegionPerilID: [70, 70],
            RawCol.ModelOccurrenceDate: [date(2025, 1, 2), date(2026, 1, 3)],
        }
    ).write_parquet(validation_dir / "risklink_flood22_model_events.parquet")

    result = load_risklink_flood_events(data_root).collect().sort(Col.model_occurrence_year)

    assert result.select(Col.event_id, Col.model_occurrence_year, Col.region_peril_id, Col.risklink_event_day).rows() == [
        (1, 2025, 70, 2),
        (1, 2026, 70, 3),
    ]
