from __future__ import annotations

from pathlib import Path
import sys


def is_frozen() -> bool:
    """Return whether the process is running from a PyInstaller bundle."""

    return bool(getattr(sys, "frozen", False))


def resource_root() -> Path:
    """Return the root containing bundled project resources.

    In source mode this is the repository root. In a PyInstaller build this is
    the extraction/internal directory where data files such as ``docs/`` and
    ``zensical.toml`` are bundled.
    """

    if is_frozen():
        return Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent)).resolve()
    return Path(__file__).resolve().parents[2]


def resource_path(relative_path: str | Path) -> Path:
    """Resolve a project resource in source and PyInstaller-frozen modes."""

    return resource_root() / relative_path


def docs_dir() -> Path:
    """Resolve the bundled/source documentation directory."""

    return resource_path("docs")


def zensical_config_path() -> Path:
    """Resolve the bundled/source Zensical configuration file."""

    return resource_path("zensical.toml")
