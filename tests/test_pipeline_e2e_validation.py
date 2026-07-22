from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from rollup.columns import Col, RawCol
from rollup.sources.seeds import load
from rollup.staging import stg_risklink_flood_events


pytestmark = pytest.mark.integration


def test_seed_source_accepts_csv_seed_files(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    (data_root / "seeds" / "business").mkdir(parents=True)
    pl.DataFrame({Col.modelled_lob: ["LOB"], Col.rollup_lob: ["LOB"]}).write_csv(
        data_root / "seeds" / "business" / "lobs.csv"
    )

    result = load(data_root)

    assert "lobs" in result
    assert result["lobs"].collect().to_dict(as_series=False) == {
        Col.modelled_lob: ["LOB"],
        Col.rollup_lob: ["LOB"],
    }


def test_risklink_flood_event_staging_keeps_model_occurrence_year(
    tmp_path: Path,
) -> None:
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

    result = (
        stg_risklink_flood_events.Model.transform(
            load(data_root)["risklink_flood22_model_events"]
        )
        .collect()
        .sort(Col.model_occurrence_year)
    )

    assert result.select(
        Col.event_id,
        Col.model_occurrence_year,
        Col.region_peril_id,
        Col.risklink_event_day,
    ).rows() == [
        (1, 2025, 70, 2),
        (1, 2026, 70, 3),
    ]
