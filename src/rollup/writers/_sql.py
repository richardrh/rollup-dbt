from __future__ import annotations

from pathlib import Path


def identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"
