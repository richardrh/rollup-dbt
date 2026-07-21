from __future__ import annotations

import inspect
import os
import subprocess
from pathlib import Path
from typing import assert_type, override

import polars as pl
import pytest

from rollup.model import PolarsModel


class _Model(PolarsModel[[pl.LazyFrame]]):
    called = False

    @override
    @classmethod
    def schema(cls) -> pl.Schema:
        return pl.Schema({"name": pl.String})

    @override
    @classmethod
    def _transform(cls, frame: pl.LazyFrame) -> pl.LazyFrame:
        cls.called = True
        return frame.select(pl.col("name").cast(pl.String))


class _InvalidModel(PolarsModel[[pl.LazyFrame]]):
    @override
    @classmethod
    def schema(cls) -> pl.Schema:
        return pl.Schema({"name": pl.String})

    @override
    @classmethod
    def _transform(cls, frame: pl.LazyFrame) -> pl.LazyFrame:
        return frame.select(pl.col("count"))


class _MissingTransform(PolarsModel[[pl.LazyFrame]]):
    @override
    @classmethod
    def schema(cls) -> pl.Schema:
        return pl.Schema({"name": pl.String})


def test_transform_invokes_private_hook_and_validates_its_schema() -> None:
    frame = _Model.transform(pl.DataFrame({"name": ["a"]}).lazy())
    assert_type(frame, pl.LazyFrame)

    assert _Model.called
    assert frame.collect_schema() == _Model.schema()


def test_transform_rejects_private_hook_schema_mismatch() -> None:
    with pytest.raises(ValueError, match="test_model.*output schema mismatch"):
        _InvalidModel.transform(pl.DataFrame({"count": [1]}).lazy())


def test_public_orchestration_methods_are_inherited_and_final() -> None:
    assert inspect.isabstract(_MissingTransform)
    assert getattr(_Model.transform, "__final__", False)
    assert getattr(_Model.validate, "__final__", False)


def test_mypy_rejects_a_model_override_without_override_decorator(
    tmp_path: Path,
) -> None:
    malformed_model = tmp_path / "malformed_model.py"
    malformed_model.write_text(
        """\
from typing import override

import polars as pl

from rollup.model import PolarsModel


class Model(PolarsModel[[pl.LazyFrame]]):
    @classmethod
    def schema(cls) -> pl.Schema:
        return pl.Schema({"name": pl.String})

    @override
    @classmethod
    def _transform(cls, frame: pl.LazyFrame) -> pl.LazyFrame:
        return frame.select("name")
""",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            "uv",
            "run",
            "mypy",
            "--strict",
            "--enable-error-code",
            "explicit-override",
            str(malformed_model),
        ],
        capture_output=True,
        check=False,
        cwd=Path(__file__).resolve().parents[1],
        env={**os.environ, "MYPYPATH": "src"},
        text=True,
    )

    assert result.returncode == 1
    assert "explicit-override" in result.stdout
