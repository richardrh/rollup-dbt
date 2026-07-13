from __future__ import annotations

from pathlib import Path

import polars as pl

from rollup.sources.catalog import load_seed_frames


def test_load_seed_frames_discovers_nested_csv_and_parquet_by_stem(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    business = data_root / "seeds" / "business"
    vor = data_root / "seeds" / "vor"
    business.mkdir(parents=True)
    vor.mkdir(parents=True)
    pl.DataFrame({"lob": ["fine_art"]}).write_csv(business / "lobs.csv")
    pl.DataFrame({"rate": [1.25]}).write_parquet(vor / "fx_rates.parquet")

    frames = load_seed_frames(data_root)

    assert sorted(frames) == ["fx_rates", "lobs"]
    assert all(isinstance(frame, pl.LazyFrame) for frame in frames.values())
    assert frames["lobs"].collect().to_dict(as_series=False) == {"lob": ["fine_art"]}
    assert frames["fx_rates"].collect().to_dict(as_series=False) == {"rate": [1.25]}


def test_load_seed_frames_duplicate_stems_raise_clear_error(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    (data_root / "seeds" / "a").mkdir(parents=True)
    (data_root / "seeds" / "b").mkdir(parents=True)
    pl.DataFrame({"value": [1]}).write_csv(data_root / "seeds" / "a" / "dup.csv")
    pl.DataFrame({"value": [2]}).write_parquet(data_root / "seeds" / "b" / "dup.parquet")

    try:
        load_seed_frames(data_root)
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected duplicate stem ValueError")

    assert "duplicate seed stem 'dup'" in message
    assert "a/dup.csv" in message
    assert "b/dup.parquet" in message


def test_load_seed_frames_absent_and_empty_roots_return_empty_dict(tmp_path: Path) -> None:
    assert load_seed_frames(tmp_path / "missing") == {}

    data_root = tmp_path / "data"
    (data_root / "seeds").mkdir(parents=True)
    assert load_seed_frames(data_root) == {}
