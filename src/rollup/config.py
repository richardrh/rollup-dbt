from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import tomllib


@dataclass(frozen=True)
class AnalysisConfig:
    simulation_counts: dict[str, int] = field(
        default_factory=lambda: {"verisk": 10000, "risklink": 100000}
    )
    return_periods: tuple[int, ...] = (30, 200, 1000)


@dataclass(frozen=True)
class OutputConfig:
    write_stage_outputs: bool = True
    write_duckdb: bool = False
    stage_output_dir: str = "stages"
    staging_dir: str = "staging"
    intermediate_dir: str = "intermediate"
    marts_dir: str = "marts"
    analysis_dir: str = "analysis"
    combined_file: str = "mts_tbl_ylt_combined_all_factors.parquet"
    wide_file: str = "mts_tbl_ylt_combined_all_factors_wide.parquet"
    dialsup_file: str = "mts_tbl_ylt_dialsup.parquet"
    event_validation_file: str = "mts_event_validation.parquet"
    ep_report_file: str = "ep_report.csv"
    duckdb_file: str | None = None

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
class FXConfig:
    target_currency: str = "GBP"

    def __post_init__(self) -> None:
        object.__setattr__(self, "target_currency", str(self.target_currency).upper())


@dataclass(frozen=True)
class RollupConfig:
    analysis: AnalysisConfig = field(default_factory=AnalysisConfig)
    outputs: OutputConfig = field(default_factory=OutputConfig)
    fx: FXConfig = field(default_factory=FXConfig)


def load_config(config_path: str | Path | None = None) -> RollupConfig:
    """Load rollup TOML config, falling back to Dataiku-friendly defaults."""
    path = Path(config_path) if config_path is not None else Path("rollup.local.toml")
    if not path.exists():
        return RollupConfig()

    with path.open("rb") as handle:
        raw = tomllib.load(handle)

    analysis_raw = _normalise_keys(raw.get("analysis", {}))
    outputs_raw = _normalise_keys(raw.get("outputs", {}))
    fx_raw = _normalise_keys(raw.get("fx", {}))

    return RollupConfig(
        analysis=AnalysisConfig(
            simulation_counts=_simulation_counts(analysis_raw),
            return_periods=tuple(int(value) for value in analysis_raw.get("return_periods", (30, 200, 1000))),
        ),
        outputs=OutputConfig(**_output_values(outputs_raw)),
        fx=FXConfig(**_fx_values(fx_raw)),
    )


def _normalise_keys(values: dict[str, Any]) -> dict[str, Any]:
    return {key.lower(): value for key, value in values.items()}


def _simulation_counts(values: dict[str, Any]) -> dict[str, int]:
    nested = values.get("simulation_counts")
    if isinstance(nested, dict):
        return {str(key).lower(): int(value) for key, value in nested.items()}

    default = AnalysisConfig().simulation_counts
    return {
        "verisk": int(values.get("num_sims_verisk", default["verisk"])),
        "risklink": int(values.get("num_sims_risklink", default["risklink"])),
    }


def _output_values(values: dict[str, Any]) -> dict[str, Any]:
    allowed = OutputConfig.__dataclass_fields__.keys()
    return {key: value for key, value in values.items() if key in allowed}


def _fx_values(values: dict[str, Any]) -> dict[str, Any]:
    allowed = FXConfig.__dataclass_fields__.keys()
    return {key: value for key, value in values.items() if key in allowed}
