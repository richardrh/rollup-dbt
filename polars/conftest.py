"""Pytest bootstrap: put this folder on sys.path so `import rollup` resolves.

The on-disk folder is named `polars/` to match the project's mental model,
but the importable package inside is named `rollup/` to avoid shadowing the
polars library. See polars/README.md.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
