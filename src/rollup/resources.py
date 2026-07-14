from __future__ import annotations

from pathlib import Path


def resource_root() -> Path:
    """Return the source checkout root containing project resources."""

    return Path(__file__).resolve().parents[2]


def resource_path(relative_path: str | Path) -> Path:
    """Resolve a project resource from the source checkout root."""

    return resource_root() / relative_path


def docs_dir() -> Path:
    """Resolve the source documentation directory."""

    return resource_path("docs")


def zensical_config_path() -> Path:
    """Resolve the source Zensical configuration file."""

    return resource_path("zensical.toml")
