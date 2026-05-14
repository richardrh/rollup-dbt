"""Pipeline configuration: vendors, paths, and environment overrides.

This module owns runtime configuration only. Pre-run checking and rendering
live in ``rollup.plan`` and ``rollup.plan_render``; their APIs are re-exported
at the bottom for existing callers that import them from here.
"""

from __future__ import annotations

import importlib.util
import logging
import os
import tomllib
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any


POLARS_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = POLARS_ROOT.parent
LOCAL_TOML_CONFIG = REPO_ROOT / "rollup.local.toml"
LEGACY_PY_CONFIG = REPO_ROOT / "config.py"


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
    toml_config = _load_toml_config()
    local_config = _load_local_config()
    resolved = (
        level
        or os.getenv(EnvVar.LOG)
        or _string_or_none(_toml_value(toml_config, ("logging", "level"), ("log",)))
        or (getattr(local_config, "LOG", None) if local_config else None)
        or "WARNING"
    )
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


def _verisk(
    data_root: Path,
    get_path: Callable[..., Path] | None = None,
    get_str: Callable[[EnvVar, str, str, tuple[str, ...], tuple[str, ...]], str] | None = None,
) -> Vendor:
    if get_path is None:
        get_path = lambda var, _attr, default, *_paths: _env_path(var, default)
    if get_str is None:
        get_str = lambda var, _attr, default, *_paths: os.getenv(var, default)
    return Vendor(
        name=VendorName.VERISK,
        hisco_label="AIR",
        n_simulations=10_000,
        ylt_dir=get_path(
            EnvVar.YLT_VERISK_DIR,
            "YLT_VERISK_DIR",
            data_root / "ylt" / VendorName.VERISK,
            ("vendors", "verisk", "ylt_dir"),
            ("ylt_verisk_dir",),
        ),
        ylt_glob=get_str(
            EnvVar.YLT_VERISK_GLOB,
            "YLT_VERISK_GLOB",
            "air_ylt_*.parquet",
            ("vendors", "verisk", "ylt_glob"),
            ("ylt_verisk_glob",),
        ),
        ep_summary_dir=get_path(
            EnvVar.EP_VERISK_DIR,
            "EP_VERISK_DIR",
            data_root / "ep_summaries" / VendorName.VERISK,
            ("vendors", "verisk", "ep_summary_dir"),
            ("ep_verisk_dir",),
        ),
    )


def _risklink(
    data_root: Path,
    get_path: Callable[..., Path] | None = None,
    get_str: Callable[[EnvVar, str, str, tuple[str, ...], tuple[str, ...]], str] | None = None,
) -> Vendor:
    if get_path is None:
        get_path = lambda var, _attr, default, *_paths: _env_path(var, default)
    if get_str is None:
        get_str = lambda var, _attr, default, *_paths: os.getenv(var, default)
    return Vendor(
        name=VendorName.RISKLINK,
        hisco_label="RMS",
        n_simulations=100_000,
        ylt_dir=get_path(
            EnvVar.YLT_RISKLINK_DIR,
            "YLT_RISKLINK_DIR",
            data_root / "ylt" / VendorName.RISKLINK,
            ("vendors", "risklink", "ylt_dir"),
            ("ylt_risklink_dir",),
        ),
        ylt_glob=get_str(
            EnvVar.YLT_RISKLINK_GLOB,
            "YLT_RISKLINK_GLOB",
            "risklink_ylt_*.parquet",
            ("vendors", "risklink", "ylt_glob"),
            ("ylt_risklink_glob",),
        ),
        ep_summary_dir=get_path(
            EnvVar.EP_RISKLINK_DIR,
            "EP_RISKLINK_DIR",
            data_root / "ep_summaries" / VendorName.RISKLINK,
            ("vendors", "risklink", "ep_summary_dir"),
            ("ep_risklink_dir",),
        ),
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
    path = LEGACY_PY_CONFIG
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


def _load_toml_config(path: Path | None = None) -> dict[str, Any]:
    """Read ``rollup.local.toml`` from the repo root if it exists."""
    path = path or LOCAL_TOML_CONFIG
    if not path.exists():
        return {}
    try:
        with path.open("rb") as fh:
            return tomllib.load(fh)
    except Exception as e:
        raise SystemExit(
            f"error: {path.name} has a problem: {type(e).__name__}: {e}\n"
            f"fix {path.name} and retry."
        )


def _toml_value(config: dict[str, Any], *paths: tuple[str, ...]) -> Any | None:
    """Return the first configured TOML value found at any nested key path."""
    for path in paths:
        node: Any = config
        for key in path:
            if not isinstance(node, dict) or key not in node:
                break
            node = node[key]
        else:
            return node
    return None


def _string_or_none(value: Any | None) -> str | None:
    return None if value is None else str(value)


def _path_or_default(value: Any | None, default: Path) -> Path:
    if value is None:
        return default
    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path.resolve()


def resolve() -> Config:
    """Build a ``Config`` from TOML, legacy ``config.py``, and env vars."""
    toml_config = _load_toml_config()
    local_config = _load_local_config()

    def _getval(var: EnvVar, attr: str, *toml_paths: tuple[str, ...]) -> str | None:
        return (
            os.getenv(var)
            or _string_or_none(_toml_value(toml_config, *toml_paths))
            or (getattr(local_config, attr, None) if local_config else None)
        )

    def _getpath(var: EnvVar, attr: str, default: Path, *toml_paths: tuple[str, ...]) -> Path:
        env_value = os.getenv(var)
        if env_value:
            return Path(env_value).expanduser().resolve()
        toml_raw = _toml_value(toml_config, *toml_paths)
        if toml_raw is not None:
            return _path_or_default(toml_raw, default)
        legacy_raw = getattr(local_config, attr, None) if local_config else None
        return _path_or_default(legacy_raw, default)

    def _getstr(var: EnvVar, attr: str, default: str, *toml_paths: tuple[str, ...]) -> str:
        return _getval(var, attr, *toml_paths) or default

    data_root = _getpath(
        EnvVar.DATA_DIR,
        "DATA_DIR",
        REPO_ROOT / "data",
        ("paths", "data_dir"),
        ("data_dir",),
    )
    raw_min_loss = _getval(EnvVar.MIN_LOSS, "MIN_LOSS", ("run", "min_loss"), ("min_loss",))
    min_loss = float(raw_min_loss) if raw_min_loss is not None else 1000.0
    return Config(
        seeds_dir=_getpath(
            EnvVar.SEEDS_DIR,
            "SEEDS_DIR",
            data_root / "seeds",
            ("paths", "seeds_dir"),
            ("seeds_dir",),
        ),
        output_dir=_getpath(
            EnvVar.OUTPUT_DIR,
            "OUTPUT_DIR",
            data_root / "output",
            ("paths", "output_dir"),
            ("output_dir",),
        ),
        vendors=(_verisk(data_root, _getpath, _getstr), _risklink(data_root, _getpath, _getstr)),
        mssql_conn_str=_getval(
            EnvVar.MSSQL_CONN_STR,
            "MSSQL_CONN_STR",
            ("sql", "mssql_conn_str"),
            ("mssql_conn_str",),
        ),
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
