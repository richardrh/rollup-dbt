from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import tomllib


DEFAULT_VENDOR_YEARS = {"verisk": 10000, "risklink": 100000}
DEFAULT_FANOUT_PREFIXES = {"verisk": "HiscoAIR", "risklink": "HiscoRMS"}


@dataclass(frozen=True)
class BlendingTargetPoint:
    ep_type: str
    return_period: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "ep_type", str(self.ep_type).upper())
        object.__setattr__(self, "return_period", int(self.return_period))


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
    write_stage_outputs: bool = True
    write_duckdb: bool = True
    minimum_event_loss_threshold: float = 1000.0
    stage_output_dir: str = "stages"
    staging_dir: str = "staging"
    intermediate_dir: str = "intermediate"
    marts_dir: str = "marts"
    analysis_dir: str = "analysis"
    combined_file: str = "mts_tbl_ylt_combined_all_factors.parquet"
    wide_file: str = "mts_tbl_ylt_combined_all_factors_wide.parquet"
    dialsup_file: str = "mts_tbl_ylt_dialsup.parquet"
    ep_report_file: str = "ep_report.csv"
    duckdb_file: str | None = None
    fanout_prefixes: dict[str, str] = field(
        default_factory=lambda: dict(DEFAULT_FANOUT_PREFIXES)
    )

    def staging_path(self, output_root: Path) -> Path:
        return output_root / self.stage_output_dir / self.staging_dir

    def intermediate_path(self, output_root: Path) -> Path:
        return output_root / self.stage_output_dir / self.intermediate_dir

    def marts_path(self, output_root: Path) -> Path:
        return output_root / self.marts_dir

    def analysis_path(self, output_root: Path) -> Path:
        return output_root / self.analysis_dir

    def duckdb_path(self, output_root: Path) -> Path:
        path = Path(self.duckdb_file) if self.duckdb_file is not None else Path("rollup.duckdb")
        if path.is_absolute():
            return path
        return output_root / path


@dataclass(frozen=True)
class InputConfig:
    verisk_events_file: str = "seeds/validation/verisk_events.parquet"
    risklink_events_file: str = "seeds/validation/risklink_flood22_model_events.parquet"

    def verisk_events_path(self, data_root: Path | str) -> Path:
        return self._resolve(data_root, self.verisk_events_file)

    def risklink_events_path(self, data_root: Path | str) -> Path:
        return self._resolve(data_root, self.risklink_events_file)

    @staticmethod
    def _resolve(data_root: Path | str, configured_path: str) -> Path:
        path = Path(configured_path)
        if path.is_absolute():
            return path
        return Path(data_root) / path


@dataclass(frozen=True)
class FXConfig:
    target_currency: str = "GBP"

    def __post_init__(self) -> None:
        object.__setattr__(self, "target_currency", str(self.target_currency).upper())


@dataclass(frozen=True)
class LoggingConfig:
    format: str = "jsonl"

    def __post_init__(self) -> None:
        log_format = str(self.format).lower()
        if log_format == "json":
            log_format = "jsonl"
        if log_format not in {"text", "jsonl"}:
            raise ValueError("logging format must be 'text' or 'jsonl'")
        object.__setattr__(self, "format", log_format)


@dataclass(frozen=True)
class RollupConfig:
    analysis: AnalysisConfig = field(default_factory=AnalysisConfig)
    blending: BlendingConfig = field(default_factory=BlendingConfig)
    inputs: InputConfig = field(default_factory=InputConfig)
    outputs: OutputConfig = field(default_factory=OutputConfig)
    fx: FXConfig = field(default_factory=FXConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


def load_config(config_path: str | Path | None = None) -> RollupConfig:
    """Load rollup TOML config, falling back to tracked defaults."""
    path = Path(config_path) if config_path is not None else Path("config.toml")
    if not path.exists():
        return RollupConfig()

    with path.open("rb") as handle:
        raw = tomllib.load(handle)

    analysis_raw = _normalise_keys(raw.get("analysis", {}))
    blending_raw = _normalise_keys(raw.get("blending", {}))
    inputs_raw = _normalise_keys(raw.get("inputs", {}))
    outputs_raw = _normalise_keys(raw.get("outputs", {}))
    fx_raw = _normalise_keys(raw.get("fx", {}))
    logging_raw = _normalise_keys(raw.get("logging", {}))
    vendor_years = _configured_vendor_years(
        _normalise_keys(raw.get("vendor_years", {}))
    )

    return RollupConfig(
        analysis=AnalysisConfig(
            simulation_counts=_simulation_counts(analysis_raw, vendor_years),
            return_periods=tuple(
                int(value) for value in analysis_raw.get("return_periods", (30, 200, 1000))
            ),
        ),
        blending=BlendingConfig(
            vendor_years=_blending_vendor_years(blending_raw, vendor_years),
            target_points=_blending_target_points(blending_raw),
            uplift_factor_min=float(blending_raw.get("uplift_factor_min", 0.1)),
            uplift_factor_max=float(blending_raw.get("uplift_factor_max", 10.0)),
        ),
        inputs=InputConfig(**_input_values(inputs_raw)),
        outputs=OutputConfig(**_output_values(outputs_raw)),
        fx=FXConfig(**_fx_values(fx_raw)),
        logging=LoggingConfig(**_logging_values(logging_raw)),
    )


def _normalise_keys(values: dict[str, Any]) -> dict[str, Any]:
    return {key.lower(): value for key, value in values.items()}


def _configured_vendor_years(values: dict[str, Any]) -> dict[str, int] | None:
    if not values:
        return None
    return {str(key).lower(): int(value) for key, value in values.items()}


def _simulation_counts(
    values: dict[str, Any],
    vendor_years: dict[str, int] | None,
) -> dict[str, int]:
    if vendor_years is not None:
        return dict(vendor_years)
    for nested_key in ("vendor_years", "simulation_counts"):
        nested = values.get(nested_key)
        if isinstance(nested, dict):
            return {str(key).lower(): int(value) for key, value in nested.items()}

    default = AnalysisConfig().simulation_counts
    return {
        "verisk": int(values.get("num_sims_verisk", default["verisk"])),
        "risklink": int(values.get("num_sims_risklink", default["risklink"])),
    }


def _blending_vendor_years(
    values: dict[str, Any],
    vendor_years: dict[str, int] | None,
) -> dict[str, int]:
    if vendor_years is not None:
        return dict(vendor_years)
    nested = values.get("vendor_years")
    if isinstance(nested, dict):
        return {str(key).lower(): int(value) for key, value in nested.items()}
    return dict(BlendingConfig().vendor_years)


def _blending_target_points(values: dict[str, Any]) -> tuple[BlendingTargetPoint, ...]:
    raw_points = values.get("target_points")
    if raw_points is None:
        return BlendingConfig().target_points
    return tuple(
        BlendingTargetPoint(point["ep_type"], point["return_period"])
        for point in raw_points
    )


def _output_values(values: dict[str, Any]) -> dict[str, Any]:
    allowed = OutputConfig.__dataclass_fields__.keys()
    output = {key: value for key, value in values.items() if key in allowed}
    if "minimum_event_loss_threshold" in output:
        output["minimum_event_loss_threshold"] = float(
            output["minimum_event_loss_threshold"]
        )
    fanout_prefixes = output.get("fanout_prefixes")
    if isinstance(fanout_prefixes, dict):
        output["fanout_prefixes"] = {
            str(base_model).lower(): str(prefix)
            for base_model, prefix in fanout_prefixes.items()
        }
    return output


def _input_values(values: dict[str, Any]) -> dict[str, Any]:
    allowed = InputConfig.__dataclass_fields__.keys()
    return {key: value for key, value in values.items() if key in allowed}


def _fx_values(values: dict[str, Any]) -> dict[str, Any]:
    allowed = FXConfig.__dataclass_fields__.keys()
    return {key: value for key, value in values.items() if key in allowed}


def _logging_values(values: dict[str, Any]) -> dict[str, Any]:
    allowed = LoggingConfig.__dataclass_fields__.keys()
    return {key: value for key, value in values.items() if key in allowed}
