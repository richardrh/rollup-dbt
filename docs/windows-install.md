# Windows install

Windows users can run the same local CLI commands from PowerShell after the
project environment is available.

## Smoke run

```powershell
uv run python -m rollup run --data-root data --output-root output --target-currency GBP
```

Faster run without stage outputs or analysis:

```powershell
uv run python -m rollup run --data-root data --output-root output --target-currency GBP --no-stage-outputs --no-analysis
```

Outputs are written under `output\`. Logs default to `output\rollup.log` unless
`--log-file` is supplied.

For the exact input and output layouts, see [Runtime guide](runtime.md).
