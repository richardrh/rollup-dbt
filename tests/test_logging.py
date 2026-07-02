from __future__ import annotations

import json
import logging

from rollup.logging import JsonLineFormatter, make_formatter, normalize_log_format


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


def test_json_alias_normalizes_to_jsonl() -> None:
    assert normalize_log_format("json") == "jsonl"
    assert isinstance(make_formatter("json"), JsonLineFormatter)


def test_text_formatter_remains_default_format() -> None:
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

    assert "INFO rollup.example plain text" in formatter.format(record)
