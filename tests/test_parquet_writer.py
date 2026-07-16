from __future__ import annotations

import polars as pl
import pytest

from rollup.writers import parquet


def test_parquet_write_emits_one_completion_record(tmp_path, caplog) -> None:
    output_path = tmp_path / "output.parquet"
    caplog.set_level("INFO", logger="rollup.pipeline")

    parquet.write(pl.DataFrame({"value": [1, 2]}), output_path)

    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "rollup.pipeline"
    ]
    assert len(messages) == 1
    assert messages[0].startswith(f"wrote output={output_path} rows=2 elapsed=")
    assert not any(message.startswith("writing output=") for message in messages)


def test_parquet_write_sinks_lazy_frame_with_unknown_rows(tmp_path, caplog) -> None:
    output_path = tmp_path / "nested" / "output.parquet"
    caplog.set_level("INFO", logger="rollup.pipeline")

    parquet.write(pl.DataFrame({"value": [1, 2]}).lazy(), output_path)

    assert pl.read_parquet(output_path).to_dict(as_series=False) == {"value": [1, 2]}
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "rollup.pipeline"
    ]
    assert len(messages) == 1
    assert messages[0].startswith(f"wrote output={output_path} rows=-1 elapsed=")


def test_parquet_write_preserves_existing_output_when_materialization_fails(
    tmp_path,
) -> None:
    output_path = tmp_path / "output.parquet"
    pl.DataFrame({"sentinel": ["keep"]}).write_parquet(output_path)
    failing_frame = pl.DataFrame({"value": [1]}).lazy().select("missing")

    with pytest.raises(ValueError, match="unable to resolve lazy frame schema"):
        parquet.write(failing_frame, output_path)

    assert pl.read_parquet(output_path).to_dict(as_series=False) == {
        "sentinel": ["keep"]
    }
