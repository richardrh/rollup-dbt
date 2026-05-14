"""Pipeline configuration: vendors, paths, and environment overrides.

This module owns runtime configuration only. Pre-run checking and rendering
live in ``rollup.plan`` and ``rollup.plan_render``; their APIs are re-exported
at the bottom for existing callers that import them from here.
"""

from __future__ import annotations

import importlib.util
import logging
import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


POLARS_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = POLARS_ROOT.parent


class VendorName(StrEnum):
    """Closed set of vendor identifiers used in pipeline data."""
    VERISK = "verisk"
    RISKLINK = "risklink"


class CurrencyCode(StrEnum):
    """Closed set of currency codes emitted by in-code derivation rules."""
    GBP = "GBP"
    EUR = "EUR"


# Any peril whose family is "FL" gets RiskLink as base model.
FLOOD_FAMILY: str = "FL"


class EnvVar(StrEnum):
    """Every ``ROLLUP_*`` environment variable the pipeline reads."""
    LOG = "ROLLUP_LOG"
    DATA_DIR = "ROLLUP_DATA_DIR"
    SEEDS_DIR = "ROLLUP_SEEDS_DIR"
    OUTPUT_DIR = "ROLLUP_OUTPUT_DIR"
    YLT_VERISK_DIR = "ROLLUP_YLT_VERISK_DIR"
    YLT_VERISK_GLOB = "ROLLUP_YLT_VERISK_GLOB"
    YLT_RISKLINK_DIR = "ROLLUP_YLT_RISKLINK_DIR"
    YLT_RISKLINK_GLOB = "ROLLUP_YLT_RISKLINK_GLOB"
    EP_VERISK_DIR = "ROLLUP_EP_VERISK_DIR"
    EP_RISKLINK_DIR = "ROLLUP_EP_RISKLINK_DIR"
    MSSQL_CONN_STR = "ROLLUP_MSSQL_CONN_STR"
    MIN_LOSS = "ROLLUP_MIN_LOSS"


def setup_logging(level: str | None = None) -> None:
    """Initialise the ``rollup`` logger. Silent by default (WARNING)."""
    resolved = level or os.getenv(EnvVar.LOG, "WARNING")
    logging.basicConfig(
        level=resolved.upper(),
        format="%(asctime)s  %(levelname)-5s  %(name)-22s  %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )


class Flavor(StrEnum):
    """Hisco output flavours produced by fanout."""
    MAIN = "main"
    DIALSUP = "dialsup"


_DEFAULT_FLAVORS: tuple[Flavor, ...] = (Flavor.MAIN, Flavor.DIALSUP)


@dataclass(frozen=True)
class Vendor:
    """Everything that varies by vendor, in one place."""
    name: VendorName
    hisco_label: str
    n_simulations: int
    ylt_dir: Path
    ylt_glob: str
    ep_summary_dir: Path
    ep_summary_glob: str = "*"
    flavors: tuple[Flavor, ...] = _DEFAULT_FLAVORS


def _env_path(var: EnvVar, default: Path) -> Path:
    raw = os.getenv(var)
    return Path(raw).expanduser().resolve() if raw else default


def _verisk(data_root: Path) -> Vendor:
    return Vendor(
        name=VendorName.VERISK,
        hisco_label="AIR",
        n_simulations=10_000,
        ylt_dir=_env_path(EnvVar.YLT_VERISK_DIR, data_root / "ylt" / VendorName.VERISK),
        ylt_glob=os.getenv(EnvVar.YLT_VERISK_GLOB, "air_ylt_*.parquet"),
        ep_summary_dir=_env_path(EnvVar.EP_VERISK_DIR, data_root / "ep_summaries" / VendorName.VERISK),
    )


def _risklink(data_root: Path) -> Vendor:
    return Vendor(
        name=VendorName.RISKLINK,
        hisco_label="RMS",
        n_simulations=100_000,
        ylt_dir=_env_path(EnvVar.YLT_RISKLINK_DIR, data_root / "ylt" / VendorName.RISKLINK),
        ylt_glob=os.getenv(EnvVar.YLT_RISKLINK_GLOB, "risklink_ylt*.parquet"),
        ep_summary_dir=_env_path(EnvVar.EP_RISKLINK_DIR, data_root / "ep_summaries" / VendorName.RISKLINK),
    )


@dataclass(frozen=True)
class Config:
    seeds_dir: Path
    output_dir: Path
    vendors: tuple[Vendor, ...]
    mssql_conn_str: str | None = None
    min_loss: float = 1000.0

    def vendor(self, name: VendorName) -> Vendor:
        for vendor in self.vendors:
            if vendor.name == name:
                return vendor
        raise KeyError(f"unknown vendor: {name!r}")


def _load_local_config():
    """Import ``config.py`` from the repo root if it exists, else return None."""
    path = REPO_ROOT / "config.py"
    if not path.exists():
        return None
    spec = importlib.util.spec_from_file_location("_rollup_local_config", path)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception as e:
        raise SystemExit(
            f"error: config.py has a problem: {type(e).__name__}: {e}\n"
            "fix config.py and retry."
        )
    return mod


def resolve() -> Config:
    """Build a ``Config`` from ``config.py`` or environment variables."""
    local_config = _load_local_config()

    def _getval(var: EnvVar, attr: str) -> str | None:
        return os.getenv(var) or (getattr(local_config, attr, None) if local_config else None)

    data_root = _env_path(EnvVar.DATA_DIR, REPO_ROOT / "data")
    raw_min_loss = _getval(EnvVar.MIN_LOSS, "MIN_LOSS")
    min_loss = float(raw_min_loss) if raw_min_loss is not None else 1000.0
    return Config(
        seeds_dir=_env_path(EnvVar.SEEDS_DIR, data_root / "seeds"),
        output_dir=_env_path(EnvVar.OUTPUT_DIR, data_root / "output"),
        vendors=(_verisk(data_root), _risklink(data_root)),
        mssql_conn_str=_getval(EnvVar.MSSQL_CONN_STR, "MSSQL_CONN_STR"),
        min_loss=min_loss,
    )


def redact_conn_str(conn_str: str) -> str:
    """Hide ``user:pass@`` in URL-style connection strings."""
    if "://" not in conn_str:
        return conn_str
    scheme, rest = conn_str.split("://", 1)
    if "@" in rest and not rest.startswith("@"):
        rest = f"...@{rest.split('@', 1)[1]}"
    return f"{scheme}://{rest}"


# Re-export plan APIs for existing callers that import them from config.
from rollup.plan import Check, Plan, Section, build_plan  # noqa: E402
from rollup.plan_render import (  # noqa: E402
    _section_icon,
    _status_pill,
    confirm,
    format_plan,
    print_plan,
)
