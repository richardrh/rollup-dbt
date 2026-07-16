from __future__ import annotations

import json
import logging
from pathlib import Path

import polars as pl

from rollup.logging import JsonLineFormatter, make_formatter, validate_log_format
from rollup.pipeline import _logged_phase
from rollup.writers import parquet


def test_jsonl_formatter_emits_parseable_json_with_expected_fields() -> None:
    formatter = JsonLineFormatter()
    record = logging.LogRecord(
        name="rollup.example",
        level=logging.ERROR,
        pathname=__file__,
        lineno=12,
        msg="failed %s",
        args=("job",),
        exc_info=None,
        func="test_function",
    )

    payload = json.loads(formatter.format(record))

    assert payload["level"] == "ERROR"
    assert payload["logger"] == "rollup.example"
    assert payload["message"] == "failed job"
    assert payload["function"] == "test_function"
    assert payload["line"] == 12
    assert "timestamp" in payload


def test_configure_console_logging_writes_to_log_file(tmp_path: Path) -> None:
    log_file = tmp_path / "logs" / "run.log"

    from rollup.logging import configure_console_logging

    configure_console_logging("INFO", log_file=log_file)
    logging.getLogger("rollup.test").info("expected log line")

    assert log_file.is_file()
    assert "expected log line" in log_file.read_text(encoding="utf-8")


def test_jsonl_formatter_includes_safe_custom_extra_fields() -> None:
    formatter = JsonLineFormatter()
    record = logging.LogRecord(
        name="rollup.example",
        level=logging.INFO,
        pathname=__file__,
        lineno=22,
        msg="wrote %s",
        args=("output",),
        exc_info=None,
        func="test_function",
    )
    record.event = "write_output"
    record.path = Path("output/example.parquet")
    record.rows = 3
    record.lazy = False

    payload = json.loads(formatter.format(record))

    assert payload["event"] == "write_output"
    assert payload["path"] == "output/example.parquet"
    assert payload["rows"] == 3
    assert payload["lazy"] is False


def test_invalid_log_format_is_rejected() -> None:
    import pytest

    with pytest.raises(ValueError, match="log format"):
        validate_log_format("yaml")


def test_jsonl_formatter_is_default_format() -> None:
    formatter = make_formatter()
    record = logging.LogRecord(
        name="rollup.example",
        level=logging.INFO,
        pathname=__file__,
        lineno=34,
        msg="plain text",
        args=(),
        exc_info=None,
        func="test_function",
    )

    payload = json.loads(formatter.format(record))
    assert payload["level"] == "INFO"
    assert payload["logger"] == "rollup.example"
    assert payload["message"] == "plain text"


def test_text_formatter_remains_available() -> None:
    formatter = make_formatter("text")
    record = logging.LogRecord(
        name="rollup.example",
        level=logging.INFO,
        pathname=__file__,
        lineno=34,
        msg="plain text",
        args=(),
        exc_info=None,
        func="test_function",
    )

    assert "INFO rollup.example plain text" in formatter.format(record)


def test_parquet_write_emits_structured_jsonl_fields(tmp_path) -> None:
    log_path = tmp_path / "rollup.jsonl"
    output_path = tmp_path / "output.parquet"
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(JsonLineFormatter())
    pipeline_logger = logging.getLogger("rollup.pipeline")
    previous_level = pipeline_logger.level
    previous_propagate = pipeline_logger.propagate
    pipeline_logger.setLevel(logging.INFO)
    pipeline_logger.addHandler(handler)
    pipeline_logger.propagate = False
    try:
        parquet.write(pl.DataFrame({"value": [1, 2]}), output_path)
    finally:
        pipeline_logger.removeHandler(handler)
        pipeline_logger.setLevel(previous_level)
        pipeline_logger.propagate = previous_propagate
        handler.close()

    payload = json.loads(log_path.read_text(encoding="utf-8").strip())
    assert payload["event"] == "write_output"
    assert payload["path"] == str(output_path)
    assert payload["rows"] == 2
    assert payload["lazy"] is False
    assert "wrote output=" in payload["message"]


def test_logged_phase_is_context_manager(caplog) -> None:
    caplog.set_level(logging.INFO, logger="rollup.pipeline")

    with _logged_phase("unit"):
        pass

    events = [
        record.event for record in caplog.records if record.name == "rollup.pipeline"
    ]
    assert events == ["phase_start", "phase_done"]
