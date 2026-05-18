# Polars package

The active rollup implementation lives in `polars/rollup/pipeline.py` with CLI
entrypoint code in `polars/rollup/cli.py`.

Use the repository-level `README.md` for input layout, commands, stages,
calculations, and outputs.

Common commands from the repository root:

```bash
uv run rollup validate
uv run rollup run
uv run rollup run --debug
```

Run tests with:

```bash
uv run pytest -q
```
