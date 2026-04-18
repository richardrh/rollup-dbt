"""Vendors, config resolution, plan enumeration, interactive confirm."""

from __future__ import annotations

import io
from pathlib import Path

import pytest

from rollup import config
from rollup.seeds import SEEDS


# -----------------------------------------------------------------------------
# Vendor + Config resolution
# -----------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    for var in (
        "ROLLUP_DATA_DIR",
        "ROLLUP_SEEDS_DIR",
        "ROLLUP_OUTPUT_DIR",
        "ROLLUP_YLT_VERISK_DIR", "ROLLUP_YLT_VERISK_GLOB",
        "ROLLUP_YLT_RISKLINK_DIR", "ROLLUP_YLT_RISKLINK_GLOB",
        "ROLLUP_EP_VERISK_DIR",
        "ROLLUP_EP_RISKLINK_DIR",
    ):
        monkeypatch.delenv(var, raising=False)


def test_resolve_has_two_vendors():
    cfg = config.resolve()
    assert tuple(v.name for v in cfg.vendors) == ("verisk", "risklink")


def test_verisk_n_simulations_is_10k():
    v = config.resolve().vendor("verisk")
    assert v.n_simulations == 10_000
    assert v.hisco_label == "AIR"
    assert v.ylt_glob == "air_ylt_*.parquet"


def test_risklink_n_simulations_is_100k():
    v = config.resolve().vendor("risklink")
    assert v.n_simulations == 100_000
    assert v.hisco_label == "RMS"
    assert v.ylt_glob == "risklink_ylt_*.parquet"


def test_paths_default_under_repo_root():
    cfg = config.resolve()
    assert cfg.seeds_dir == config.POLARS_ROOT / "seeds"
    assert cfg.output_dir == config.REPO_ROOT / "data" / "output"
    assert cfg.vendor("verisk").ylt_dir == config.REPO_ROOT / "data" / "ylt" / "verisk"
    assert cfg.vendor("risklink").ylt_dir == config.REPO_ROOT / "data" / "ylt" / "risklink"


def test_env_overrides_win(monkeypatch, tmp_path):
    monkeypatch.setenv("ROLLUP_SEEDS_DIR", str(tmp_path / "s"))
    monkeypatch.setenv("ROLLUP_YLT_VERISK_DIR", str(tmp_path / "v"))
    cfg = config.resolve()
    assert cfg.seeds_dir == (tmp_path / "s").resolve()
    assert cfg.vendor("verisk").ylt_dir == (tmp_path / "v").resolve()


def test_unknown_vendor_raises():
    cfg = config.resolve()
    with pytest.raises(KeyError):
        cfg.vendor("katrisk")


# -----------------------------------------------------------------------------
# Plan enumeration
# -----------------------------------------------------------------------------

def _cfg_with_seeds(tmp_path: Path, populate: bool = True) -> config.Config:
    """Synthesize a config whose seeds_dir has real CSVs (so validation succeeds).

    We copy from the real seeds folder so schema validation passes.
    """
    import shutil
    seeds_dir = tmp_path / "seeds"
    seeds_dir.mkdir()
    if populate:
        real_seeds = config.POLARS_ROOT / "seeds"
        for spec in SEEDS:
            shutil.copy(real_seeds / spec.filename, seeds_dir / spec.filename)
    return config.Config(
        seeds_dir=seeds_dir,
        output_dir=tmp_path / "out",
        vendors=(
            config.Vendor("verisk",   "AIR", 10_000,  tmp_path / "ylt" / "verisk",
                          "air_ylt_*.parquet",       tmp_path / "ep" / "verisk"),
            config.Vendor("risklink", "RMS", 100_000, tmp_path / "ylt" / "risklink",
                          "risklink_ylt_*.parquet",  tmp_path / "ep" / "risklink"),
        ),
    )


def test_build_plan_validates_every_seed_schema(tmp_path):
    plan = config.build_plan(_cfg_with_seeds(tmp_path))
    seeds = plan.seeds_section
    assert len(seeds.checks) == len(SEEDS)
    assert all(c.ok for c in seeds.checks), [
        (c.label, c.note) for c in seeds.checks if not c.ok
    ]
    assert plan.all_seeds_ok


def test_missing_seed_flagged(tmp_path):
    cfg = _cfg_with_seeds(tmp_path)
    (cfg.seeds_dir / "lobs.csv").unlink()
    plan = config.build_plan(cfg)
    lobs_check = next(c for c in plan.seeds_section.checks if c.label == "lobs")
    assert not lobs_check.ok
    assert "missing" in lobs_check.note
    assert not plan.all_seeds_ok


def test_seed_schema_drift_flagged(tmp_path):
    """Rename a column in a seed CSV — plan reports 'unexpected=[...]' or 'missing=[...]'."""
    cfg = _cfg_with_seeds(tmp_path)
    bad_file = cfg.seeds_dir / "lobs.csv"
    content = bad_file.read_text().splitlines()
    content[0] = content[0].replace("lob_id", "renamed_col")
    bad_file.write_text("\n".join(content))
    plan = config.build_plan(cfg)
    lobs_check = next(c for c in plan.seeds_section.checks if c.label == "lobs")
    assert not lobs_check.ok
    assert "missing=" in lobs_check.note or "unexpected=" in lobs_check.note


def test_ylt_section_lists_globbed_files(tmp_path):
    cfg = _cfg_with_seeds(tmp_path)
    v = cfg.vendor("verisk")
    v.ylt_dir.mkdir(parents=True)
    (v.ylt_dir / "air_ylt_c1.parquet").write_bytes(b"x" * 1024)
    (v.ylt_dir / "air_ylt_c2.parquet").write_bytes(b"x" * 2048)
    (v.ylt_dir / "ignore.txt").write_text("nope")
    plan = config.build_plan(cfg)
    sec = next(s for s in plan.sections if s.title == "ylt verisk")
    labels = [c.label for c in sec.checks]
    assert "air_ylt_c1.parquet" in labels
    assert "air_ylt_c2.parquet" in labels
    assert "ignore.txt" not in labels
    assert "n_simulations=10,000" in sec.header


def test_missing_ylt_directory_flagged(tmp_path):
    plan = config.build_plan(_cfg_with_seeds(tmp_path))
    sec = next(s for s in plan.sections if s.title == "ylt verisk")
    assert all(not c.ok for c in sec.checks)


def test_format_plan_contains_every_section(tmp_path):
    plan = config.build_plan(_cfg_with_seeds(tmp_path))
    text = config.format_plan(plan)
    for title in ("seeds", "ylt verisk", "ylt risklink",
                  "ep_summaries verisk", "ep_summaries risklink", "output"):
        assert f"[{title}]" in text
    assert "Seeds:" in text
    assert "YLTs:" in text


# -----------------------------------------------------------------------------
# Interactive confirm
# -----------------------------------------------------------------------------

def test_confirm_with_assume_yes_skips_prompt(tmp_path, monkeypatch):
    plan = config.build_plan(_cfg_with_seeds(tmp_path))
    monkeypatch.setattr("builtins.input", lambda _: pytest.fail("input() called"))
    assert config.confirm(plan, assume_yes=True, stream=io.StringIO()) is True


def test_confirm_non_interactive_returns_true(tmp_path, monkeypatch):
    plan = config.build_plan(_cfg_with_seeds(tmp_path))
    monkeypatch.setattr("sys.stdin", io.StringIO(""))  # stdin.isatty() → False
    monkeypatch.setattr("builtins.input", lambda _: pytest.fail("input() called"))
    assert config.confirm(plan, assume_yes=False, stream=io.StringIO()) is True
