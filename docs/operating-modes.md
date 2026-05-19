# Operating modes

## With and without uv

From a checkout, use `uv run`:

```bash
uv run rollup validate
uv run rollup run
```

After installing the project and activating the virtual environment, `uv` is not
required:

```bash
rollup validate
rollup run
```

## Normal run

```bash
uv run rollup run
```

Writes mart fanouts to `output/marts/` and wide/report parquets to `output/`.

## Debug run

```bash
uv run rollup run --debug
```

Also writes stage frames to `output/debug/` with prefixes:

- `seed_*`
- `stg_*`
- `int_*`
- `mts_*`

## Analyze

```bash
uv run rollup analyze
```

Generates `output/analysis/ep_report.csv` from pipeline outputs.

## Serve docs

```bash
uv run rollup docs
uv run rollup docs --host 127.0.0.1 --port 8000
```

The command serves Zensical docs and prints a URL. Direct Zensical use is also
available:

```bash
uv run zensical serve --config-file zensical.toml --dev-addr 127.0.0.1:8000
```
