"""Simple configuration loader using plain dictionaries."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import tomllib
import tomli_w

# Config file is in config directory
CONFIG_PATH = Path(__file__).parent / "config.toml"


def load_config() -> dict[str, Any]:
    """Load the main configuration file."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

    with open(CONFIG_PATH, "rb") as f:
        return tomllib.load(f)


def save_config(config: dict[str, Any]) -> None:
    """Save configuration to the config file."""
    with open(CONFIG_PATH, "wb") as f:
        tomli_w.dump(config, f)


def get_source_paths(config: dict[str, Any] | None = None) -> dict[str, str]:
    """Get CSV source file paths."""
    if config is None:
        config = load_config()
    return {
        "risklink_elt": config["sources"]["risklink_elt_csv"],
        "verisk_ylt": config["sources"]["verisk_ylt_csv"],
    }


def get_database_config(config: dict[str, Any] | None = None) -> dict[str, str]:
    """Get database connection settings."""
    if config is None:
        config = load_config()
    return config["database"]


def get_staging_tables(config: dict[str, Any] | None = None) -> dict[str, str]:
    """Get staging table names."""
    if config is None:
        config = load_config()
    return {
        "risklink_elts": config["staging"]["risklink_elts_table"],
        "verisk_ylts": config["staging"]["verisk_ylts_table"],
        "risklink_ylts": config["staging"]["risklink_ylts_table"],
    }


def get_simulation_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Get simulation settings."""
    if config is None:
        config = load_config()
    return config["simulation"]


def get_project_info(config: dict[str, Any] | None = None) -> dict[str, str]:
    """Get project metadata."""
    if config is None:
        config = load_config()
    return config["project"]


def get_config_summary() -> dict[str, Any]:
    """Get a summary for CLI display."""
    config = load_config()

    return {
        "project": get_project_info(config),
        "sources": get_source_paths(config),
        "staging": get_staging_tables(config),
        "database": get_database_config(config),
        "simulation": get_simulation_config(config),
    }


# Backward compatibility aliases
load_rollup_config = load_config
load_all_configs = load_config
