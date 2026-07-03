from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from rollup.columns import Col
from rollup.config import InputConfig, RollupConfig
from rollup.pipeline import load_risklink_flood_events, load_verisk_events


def test_load_verisk_events_reads_configured_relative_path(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    events_path = data_root / "custom" / "verisk_events.parquet"
    events_path.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "EventID": [101],
            "ModelID": ["M1"],
            "Event": [202],
            "Year": [2030],
            "Day": [42],
        }
    ).write_parquet(events_path)
    config = RollupConfig(
        inputs=InputConfig(verisk_events_file="custom/verisk_events.parquet")
    )

    frame = load_verisk_events(data_root, config).collect()

    assert frame.to_dict(as_series=False) == {
        Col.model_event_id: [101],
        Col.model_code: ["M1"],
        Col.event_id: [202],
        Col.year_id: [2030],
        Col.event_day: [42],
    }


def test_load_risklink_flood_events_reads_configured_absolute_path(
    tmp_path: Path,
) -> None:
    events_path = tmp_path / "absolute_risklink_events.parquet"
    pl.DataFrame(
        {
            "ModelEventID": [301, 301],
            "ModelOccurrenceYear": [2040, 2040],
            "RegionPerilID": [77, 77],
            "ModelOccurrenceDate": [date(2040, 3, 3), date(2040, 1, 2)],
        }
    ).write_parquet(events_path)
    config = RollupConfig(inputs=InputConfig(risklink_events_file=str(events_path)))

    frame = load_risklink_flood_events(tmp_path / "data", config).collect()

    assert frame.to_dict(as_series=False) == {
        Col.event_id: [301],
        Col.model_occurrence_year: [2040],
        Col.region_peril_id: [77],
        Col.risklink_event_day: [2],
    }
