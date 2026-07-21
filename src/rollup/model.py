from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, ParamSpec, final

import polars as pl

from rollup.model_validation import validate_schema

P = ParamSpec("P")


class PolarsModel(ABC, Generic[P]):
    @classmethod
    @abstractmethod
    def schema(cls) -> pl.Schema: ...

    @classmethod
    @final
    def validate(cls, frame: pl.LazyFrame) -> None:
        validate_schema(cls.__module__.rsplit(".", 1)[-1], cls.schema(), frame)

    @classmethod
    @final
    def transform(cls, *args: P.args, **kwargs: P.kwargs) -> pl.LazyFrame:
        frame = cls._transform(*args, **kwargs)
        cls.validate(frame)
        return frame

    @classmethod
    @abstractmethod
    def _transform(cls, *args: P.args, **kwargs: P.kwargs) -> pl.LazyFrame: ...
