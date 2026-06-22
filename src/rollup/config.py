from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import tomllib


@dataclass(frozen=True)
class BlendingTargetPoint:
    ep_type: str
    return_period: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "ep_type", str(self.ep_type).upper())
        object.__setattr__(self, "return_period", int(self.return_period))


@dataclass(frozen=True)
class AnalysisConfig:
    simulation_counts: dict[str, int]
    return_periods: tuple[int, ...]


@dataclass(frozen=True)
class BlendingConfig:
    vendor_years: dict[str, int]
    target_points: tuple[BlendingTargetPoint, ...]
    uplift_factor_min: float
    uplift_factor_max: float
    subregion_selection: dict[int, str]


@dataclass(frozen=True)
class OutputConfig:
    write_stage_outputs: bool
    write_duckdb: bool
    duckdb_file: str
    stage_output_dir: str
    staging_dir: str
    intermediate_dir: str
    marts_dir: str
    analysis_dir: str
    combined_file: str
    wide_file: str
    dialsup_file: str
    ep_report_file: str
    fanout_prefixes: dict[str, str]

    def staging_path(self, output_root: Path) -> Path:
        return output_root / self.stage_output_dir / self.staging_dir

    def intermediate_path(self, output_root: Path) -> Path:
        return output_root / self.stage_output_dir / self.intermediate_dir

    def marts_path(self, output_root: Path) -> Path:
        return output_root / self.marts_dir

    def analysis_path(self, output_root: Path) -> Path:
        return output_root / self.analysis_dir

    def duckdb_path(self, output_root: Path) -> Path:
        path = Path(self.duckdb_file)
        if path.is_absolute():
            return path
        return output_root / path


@dataclass(frozen=True)
class FXConfig:
    target_currency: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "target_currency", str(self.target_currency).upper())


@dataclass(frozen=True)
class RollupConfig:
    fx: FXConfig
    outputs: OutputConfig
    analysis: AnalysisConfig
    blending: BlendingConfig


REQUIRED_SECTIONS = ("fx", "outputs", "analysis", "vendor_years", "blending")


def load_config(config_path: str | Path | None = None) -> RollupConfig:
    """Load rollup TOML config strictly; no defaults are applied."""
    path = Path(config_path) if config_path is not None else Path("config.toml")
    if not path.exists():
        raise FileNotFoundError(f"rollup config not found: {path}")

    with path.open("rb") as handle:
        raw = tomllib.load(handle)

    missing = [section for section in REQUIRED_SECTIONS if section not in raw]
    if missing:
        raise ValueError(f"missing required config sections: {', '.join(missing)}")

    outputs_raw = _normalise_keys(raw["outputs"])
    fx_raw = _normalise_keys(raw["fx"])
    analysis_raw = _normalise_keys(raw["analysis"])
    blending_raw = _normalise_keys(raw["blending"])
    vendor_years = {
        str(key).lower(): int(value)
        for key, value in raw["vendor_years"].items()
    }

    return RollupConfig(
        fx=FXConfig(**_fx_values(fx_raw)),
        outputs=OutputConfig(**_output_values(outputs_raw)),
        analysis=AnalysisConfig(
            simulation_counts=dict(vendor_years),
            return_periods=tuple(int(value) for value in analysis_raw["return_periods"]),
        ),
        blending=BlendingConfig(
            vendor_years=dict(vendor_years),
            target_points=_blending_target_points(blending_raw),
            uplift_factor_min=float(blending_raw["uplift_factor_min"]),
            uplift_factor_max=float(blending_raw["uplift_factor_max"]),
            subregion_selection=_subregion_selection(blending_raw),
        ),
    )


def _normalise_keys(values: dict[str, Any]) -> dict[str, Any]:
    return {key.lower(): value for key, value in values.items()}


def _blending_target_points(values: dict[str, Any]) -> tuple[BlendingTargetPoint, ...]:
    return tuple(
        BlendingTargetPoint(point["ep_type"], point["return_period"])
        for point in values["target_points"]
    )


def _subregion_selection(values: dict[str, Any]) -> dict[int, str]:
    raw_selection = values.get("subregion_selection", {})
    return {
        int(region_peril_id): str(sub_region_peril_id)
        for region_peril_id, sub_region_peril_id in raw_selection.items()
    }


def _output_values(values: dict[str, Any]) -> dict[str, Any]:
    allowed = OutputConfig.__dataclass_fields__.keys()
    output = {key: value for key, value in values.items() if key in allowed}
    fanout_prefixes = output.get("fanout_prefixes")
    if isinstance(fanout_prefixes, dict):
        output["fanout_prefixes"] = {
            str(base_model).lower(): str(prefix)
            for base_model, prefix in fanout_prefixes.items()
        }
    return output


def _fx_values(values: dict[str, Any]) -> dict[str, Any]:
    allowed = FXConfig.__dataclass_fields__.keys()
    return {key: value for key, value in values.items() if key in allowed}
