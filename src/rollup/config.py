from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import tomllib

from rollup.output_contract import DUCKDB_FILE


DEFAULT_VENDOR_YEARS = {"verisk": 10000, "risklink": 100000}


@dataclass(frozen=True)
class BlendingTargetPoint:
    ep_type: str
    return_period: int

    def __post_init__(self) -> None:
        if self.ep_type not in {"AAL", "OEP"}:
            raise ValueError("blending target ep_type must be 'AAL' or 'OEP'")
        if not isinstance(self.return_period, int) or isinstance(self.return_period, bool):
            raise ValueError("blending target return_period must be an integer")
        if self.ep_type == "AAL" and self.return_period != 0:
            raise ValueError("AAL blending target return_period must be 0")
        if self.ep_type == "OEP" and self.return_period <= 0:
            raise ValueError("OEP blending target return_period must be positive")


@dataclass(frozen=True)
class AnalysisConfig:
    simulation_counts: dict[str, int] = field(
        default_factory=lambda: dict(DEFAULT_VENDOR_YEARS)
    )
    return_periods: tuple[int, ...] = (30, 200, 1000)


@dataclass(frozen=True)
class BlendingConfig:
    vendor_years: dict[str, int] = field(
        default_factory=lambda: dict(DEFAULT_VENDOR_YEARS)
    )
    target_points: tuple[BlendingTargetPoint, ...] = field(
        default_factory=lambda: (
            BlendingTargetPoint("AAL", 0),
            BlendingTargetPoint("OEP", 200),
            BlendingTargetPoint("OEP", 1000),
        )
    )
    uplift_factor_min: float = 0.1
    uplift_factor_max: float = 10.0


@dataclass(frozen=True)
class OutputConfig:
    write_duckdb: bool = True
    minimum_event_loss_threshold: float = 1000.0
    duckdb_file: str | None = None

    def duckdb_path(self, output_root: Path) -> Path:
        path = (
            Path(self.duckdb_file)
            if self.duckdb_file is not None
            else Path(DUCKDB_FILE)
        )
        if path.is_absolute():
            return path
        return output_root / path


@dataclass(frozen=True)
class LoggingConfig:
    format: str = "jsonl"

    def __post_init__(self) -> None:
        if self.format not in {"text", "jsonl"}:
            raise ValueError("logging format must be 'text' or 'jsonl'")


@dataclass(frozen=True)
class RollupConfig:
    analysis: AnalysisConfig = field(default_factory=AnalysisConfig)
    blending: BlendingConfig = field(default_factory=BlendingConfig)
    outputs: OutputConfig = field(default_factory=OutputConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


def read_config(config_path: str | Path | None = None) -> RollupConfig:
    """Read rollup TOML config, falling back to tracked defaults."""
    path = Path(config_path) if config_path is not None else Path("config.toml")
    if not path.exists():
        return RollupConfig()

    with path.open("rb") as handle:
        raw = tomllib.load(handle)

    _reject_unknown_config(raw)

    analysis_raw = raw.get("analysis", {})
    blending_raw = raw.get("blending", {})
    outputs_raw = raw.get("outputs", {})
    logging_raw = raw.get("logging", {})
    vendor_years = _configured_vendor_years(raw.get("vendor_years", {}))
    analysis_return_periods = _analysis_return_periods(analysis_raw)
    target_points = _blending_target_points(blending_raw)
    uplift_factor_min = _number(blending_raw.get("uplift_factor_min", 0.1), "[blending].uplift_factor_min")
    uplift_factor_max = _number(blending_raw.get("uplift_factor_max", 10.0), "[blending].uplift_factor_max")
    write_duckdb = _bool(outputs_raw.get("write_duckdb", True), "[outputs].write_duckdb")
    minimum_event_loss_threshold = _number(
        outputs_raw.get("minimum_event_loss_threshold", 1000.0),
        "[outputs].minimum_event_loss_threshold",
    )
    duckdb_file = _optional_string(outputs_raw.get("duckdb_file"), "[outputs].duckdb_file")

    return RollupConfig(
        analysis=AnalysisConfig(
            simulation_counts=dict(vendor_years or DEFAULT_VENDOR_YEARS),
            return_periods=analysis_return_periods,
        ),
        blending=BlendingConfig(
            vendor_years=dict(vendor_years or DEFAULT_VENDOR_YEARS),
            target_points=target_points,
            uplift_factor_min=uplift_factor_min,
            uplift_factor_max=uplift_factor_max,
        ),
        outputs=OutputConfig(
            write_duckdb=write_duckdb,
            minimum_event_loss_threshold=minimum_event_loss_threshold,
            duckdb_file=duckdb_file,
        ),
        logging=LoggingConfig(format=logging_raw.get("format", "jsonl")),
    )


def _reject_unknown_config(raw: dict[str, Any]) -> None:
    allowed_sections = {"analysis", "blending", "logging", "outputs", "vendor_years"}
    unknown_sections = sorted(set(raw) - allowed_sections)
    if unknown_sections:
        raise ValueError(f"unknown config sections: {unknown_sections}")

    allowed_keys = {
        "analysis": {"return_periods"},
        "blending": {"target_points", "uplift_factor_min", "uplift_factor_max"},
        "logging": {"format"},
        "outputs": {"duckdb_file", "minimum_event_loss_threshold", "write_duckdb"},
        "vendor_years": set(DEFAULT_VENDOR_YEARS),
    }
    for section, keys in allowed_keys.items():
        values = raw.get(section, {})
        if not isinstance(values, dict):
            raise ValueError(f"config section [{section}] must be a table")
        unknown_keys = sorted(set(values) - keys)
        if unknown_keys:
            raise ValueError(f"unknown config keys in [{section}]: {unknown_keys}")


def _configured_vendor_years(values: dict[str, Any]) -> dict[str, int] | None:
    if not values:
        return None
    if set(values) != set(DEFAULT_VENDOR_YEARS):
        raise ValueError("[vendor_years] must contain exactly verisk and risklink")
    for key, value in values.items():
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"[vendor_years].{key} must be an integer")
    return dict(values)


def _analysis_return_periods(values: dict[str, Any]) -> tuple[int, ...]:
    raw_periods = values.get("return_periods", (30, 200, 1000))
    if not isinstance(raw_periods, list | tuple):
        raise ValueError("[analysis].return_periods must be an integer sequence")
    for value in raw_periods:
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError("[analysis].return_periods must contain only integers")
    return tuple(raw_periods)


def _blending_target_points(values: dict[str, Any]) -> tuple[BlendingTargetPoint, ...]:
    raw_points = values.get("target_points")
    if raw_points is None:
        return BlendingConfig().target_points
    if not isinstance(raw_points, list):
        raise ValueError("[blending].target_points must be an array of tables")
    points = []
    for index, point in enumerate(raw_points):
        if not isinstance(point, dict):
            raise ValueError(f"[blending].target_points[{index}] must be a table")
        if set(point) != {"ep_type", "return_period"}:
            raise ValueError(
                f"[blending].target_points[{index}] must contain ep_type and return_period"
            )
        points.append(BlendingTargetPoint(point["ep_type"], point["return_period"]))
    return tuple(points)


def _number(value: Any, name: str) -> float:
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise ValueError(f"{name} must be numeric")
    return float(value)


def _bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be boolean")
    return value


def _optional_string(value: Any, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return value
