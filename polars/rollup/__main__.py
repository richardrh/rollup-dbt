"""Allow `python -m rollup` to invoke the CLI."""
from rollup.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
