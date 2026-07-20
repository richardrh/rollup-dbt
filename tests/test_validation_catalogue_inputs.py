from __future__ import annotations

from datetime import date

import polars as pl

from rollup.columns import Col
from rollup.staging import stg_risklink_flood_events, stg_verisk_events


def test_stg_event_catalogue_verisk_projects_raw_seed_frame() -> None:
    frame = stg_verisk_events.transform(
        pl.DataFrame(
            {
                "EventID": [101],
                "ModelID": [1],
                "Event": [202],
                "Year": [2030],
                "Day": [42],
            }
        ).lazy()
    ).collect()

    assert frame.to_dict(as_series=False) == {
        Col.model_event_id: [101],
        Col.model_code: [1],
        Col.event_id: [202],
        Col.year_id: [2030],
        Col.event_day: [42],
    }


def test_stg_event_catalogue_risklink_flood_groups_raw_seed_frame() -> None:
    frame = stg_risklink_flood_events.transform(
        pl.DataFrame(
            {
                "ModelEventID": [301, 301],
                "ModelOccurrenceYear": [2040, 2040],
                "RegionPerilID": [77, 77],
                "ModelOccurrenceDate": [date(2040, 3, 3), date(2040, 1, 2)],
            }
        ).lazy()
    ).collect()

    assert frame.to_dict(as_series=False) == {
        Col.event_id: [301],
        Col.model_occurrence_year: [2040],
        Col.region_peril_id: [77],
        Col.risklink_event_day: [2],
    }
