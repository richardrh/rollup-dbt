"""Unit tests for rollup.wizard run orchestration."""

from __future__ import annotations

from dataclasses import dataclass

from rollup.wizard import run_wizard


@dataclass
class Args:
    min_loss: float | None = None
    dry_run: bool = False
    yes: bool = True
    dump_interim: bool = True
    derive_blending: bool = False


def test_run_wizard_dry_run_prints_plan(monkeypatch, capsys):
    from rollup import config
    from rollup.plan import Plan, Section, Check

    cfg = config.resolve()
    plan = Plan(config=cfg, sections=[
        Section("seeds", "seed-dir", [Check("seed", cfg.seeds_dir, True)]),
        Section("ylt verisk", "ylt-dir", [Check("ylt", cfg.output_dir, True)]),
        Section("ylt risklink", "ylt-dir", [Check("ylt", cfg.output_dir, True)]),
        Section("ep_summaries verisk", "ep-dir", [Check("ep", cfg.output_dir, True)]),
        Section("ep_summaries risklink", "ep-dir", [Check("ep", cfg.output_dir, True)]),
    ])
    monkeypatch.setattr(config, "resolve", lambda: cfg)
    monkeypatch.setattr(config, "build_plan", lambda _, **_kwargs: plan)

    assert run_wizard(Args(dry_run=True)) == 0
    assert "Pipeline plan" in capsys.readouterr().out


def test_run_wizard_invokes_pipeline_with_prepared_inputs(monkeypatch):
    from rollup import config
    from rollup.plan import Plan, Section, Check
    from rollup.run_inputs import BlendingInput

    cfg = config.resolve()
    plan = Plan(config=cfg, sections=[
        Section("seeds", "seed-dir", [Check("seed", cfg.seeds_dir, True)]),
        Section("ylt verisk", "ylt-dir", [Check("ylt", cfg.output_dir, True)]),
        Section("ylt risklink", "ylt-dir", [Check("ylt", cfg.output_dir, True)]),
        Section("ep_summaries verisk", "ep-dir", [Check("ep", cfg.output_dir, True)]),
        Section("ep_summaries risklink", "ep-dir", [Check("ep", cfg.output_dir, True)]),
    ])
    calls: list[dict] = []

    monkeypatch.setattr(config, "resolve", lambda: cfg)
    monkeypatch.setattr(config, "build_plan", lambda _, **_kwargs: plan)
    monkeypatch.setattr(config, "confirm", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        "rollup.wizard.derive_blending_for_run",
        lambda _cfg: BlendingInput(weights="weights", message="derived"),
    )
    monkeypatch.setattr(
        "rollup.pipeline.run",
        lambda cfg_arg, **kwargs: calls.append({"cfg": cfg_arg, **kwargs}),
    )

    assert run_wizard(Args(derive_blending=True, dump_interim=True)) == 0
    assert calls == [{"cfg": cfg, "dump_interim": True, "blending_weights": "weights"}]


def test_run_wizard_requires_ep_summaries_for_default_derivation(monkeypatch, capsys):
    from rollup import config
    from rollup.plan import Plan, Section, Check

    cfg = config.resolve()
    plan = Plan(config=cfg, sections=[
        Section("seeds", "seed-dir", [Check("seed", cfg.seeds_dir, True)]),
        Section("ylt verisk", "ylt-dir", [Check("ylt", cfg.output_dir, True)]),
        Section("ylt risklink", "ylt-dir", [Check("ylt", cfg.output_dir, True)]),
        Section("ep_summaries verisk", "ep-dir", [Check("*.long.csv", cfg.output_dir, False)]),
        Section("ep_summaries risklink", "ep-dir", [Check("*.long.csv", cfg.output_dir, True)]),
    ])
    calls: list[dict] = []

    monkeypatch.setattr(config, "resolve", lambda: cfg)
    monkeypatch.setattr(config, "build_plan", lambda _, **_kwargs: plan)
    monkeypatch.setattr("rollup.pipeline.run", lambda *_args, **_kwargs: calls.append({}))

    assert run_wizard(Args(derive_blending=True)) == 2
    assert calls == []
    assert "fix the failing checks" in capsys.readouterr().err
