"""Unit tests for rollup.wizard run orchestration."""

from __future__ import annotations

from dataclasses import dataclass
import io

from rollup.wizard import _interactive_review, run_wizard


@dataclass
class Args:
    min_loss: float | None = None
    dry_run: bool = False
    yes: bool = True
    dump_interim: bool = True


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
        "rollup.pipeline.run",
        lambda cfg_arg, **kwargs: calls.append({"cfg": cfg_arg, **kwargs}),
    )

    assert run_wizard(Args(dump_interim=True)) == 0
    assert calls == [{"cfg": cfg, "dump_interim": True}]


def test_run_wizard_uses_seed_by_default_without_requiring_ep_summaries(monkeypatch):
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
    monkeypatch.setattr(config, "confirm", lambda *_args, **_kwargs: True)
    monkeypatch.setattr("rollup.pipeline.run", lambda _cfg, **kwargs: calls.append(kwargs))

    assert run_wizard(Args()) == 0
    assert calls == [{"dump_interim": True}]


def test_run_wizard_renders_rich_plan_for_tty_failures(monkeypatch):
    from rollup import config
    from rollup.plan import Plan, Section, Check

    class TtyBuffer(io.StringIO):
        def isatty(self) -> bool:
            return True

    cfg = config.resolve()
    plan = Plan(config=cfg, sections=[
        Section("seeds", "seed-dir", [Check("seed", cfg.seeds_dir, True)]),
        Section("ylt verisk", "ylt-dir", [Check("ylt", cfg.output_dir, False)]),
        Section("ylt risklink", "ylt-dir", [Check("ylt", cfg.output_dir, True)]),
        Section("ep_summaries verisk", "ep-dir", [Check("*.long.csv", cfg.output_dir, False)]),
        Section("ep_summaries risklink", "ep-dir", [Check("*.long.csv", cfg.output_dir, True)]),
    ])
    stderr = TtyBuffer()
    rendered: list[object] = []

    def fake_print_plan(_plan, *, console):
        rendered.append(console.file)
        console.print("RICH PLAN")

    monkeypatch.setattr(config, "resolve", lambda: cfg)
    monkeypatch.setattr(config, "build_plan", lambda _, **_kwargs: plan)
    monkeypatch.setattr(config, "print_plan", fake_print_plan)
    monkeypatch.setattr("sys.stderr", stderr)

    assert run_wizard(Args()) == 2
    assert rendered == [stderr]
    assert "RICH PLAN" in stderr.getvalue()


def test_run_wizard_dry_run_fails_on_lob_peril_validation(monkeypatch, capsys):
    from rollup import config
    from rollup.plan import Plan, Section, Check

    cfg = config.resolve()
    plan = Plan(config=cfg, sections=[
        Section("seeds", "seed-dir", [Check("seed", cfg.seeds_dir, True)]),
        Section("ylt verisk", "ylt-dir", [Check("ylt", cfg.output_dir, True)]),
        Section("ylt risklink", "ylt-dir", [Check("ylt", cfg.output_dir, True)]),
        Section("ep_summaries verisk", "ep-dir", [Check("ep", cfg.output_dir, True)]),
        Section("ep_summaries risklink", "ep-dir", [Check("ep", cfg.output_dir, True)]),
        Section("lob_peril_validation", "valid", [
            Check("one peril per rollup_lob", cfg.seeds_dir, False, note="one peril per rollup_lob validation failed"),
        ]),
    ])

    monkeypatch.setattr(config, "resolve", lambda: cfg)
    monkeypatch.setattr(config, "build_plan", lambda _, **_kwargs: plan)

    assert run_wizard(Args(dry_run=True)) == 2
    assert "one peril per rollup_lob validation failed" in capsys.readouterr().out


def test_run_wizard_interactive_fails_on_lob_peril_validation(monkeypatch, capsys):
    from rollup import config
    from rollup.plan import Plan, Section, Check

    cfg = config.resolve()
    plan = Plan(config=cfg, sections=[
        Section("seeds", "seed-dir", [Check("seed", cfg.seeds_dir, True)]),
        Section("ylt verisk", "ylt-dir", [Check("ylt", cfg.output_dir, True)]),
        Section("ylt risklink", "ylt-dir", [Check("ylt", cfg.output_dir, True)]),
        Section("ep_summaries verisk", "ep-dir", [Check("ep", cfg.output_dir, True)]),
        Section("ep_summaries risklink", "ep-dir", [Check("ep", cfg.output_dir, True)]),
        Section("lob_peril_validation", "valid", [
            Check("one peril per rollup_lob", cfg.seeds_dir, False, note="one peril per rollup_lob validation failed"),
        ]),
    ])
    calls: list[dict] = []

    monkeypatch.setattr(config, "resolve", lambda: cfg)
    monkeypatch.setattr(config, "build_plan", lambda _, **_kwargs: plan)
    monkeypatch.setattr("rollup.pipeline.run", lambda *_args, **_kwargs: calls.append({}))

    assert run_wizard(Args(yes=False)) == 2
    assert calls == []
    captured = capsys.readouterr()
    assert "one peril per rollup_lob validation failed" in captured.err
    assert "fix the failing checks" in captured.err


def test_interactive_review_collects_operator_choices(monkeypatch):
    from rollup import config
    from rollup.plan import Plan, Section, Check

    cfg = config.resolve()
    plan = Plan(config=cfg, sections=[
        Section("seeds", "seed-dir", [Check("seed", cfg.seeds_dir, True)]),
        Section("forecast_factors", "forecast", [Check("forecast dates", cfg.seeds_dir, True, note="2026-01-01")]),
    ])
    args = Args(dump_interim=True)
    answers = iter(["", "", "0", "n", "y"])

    monkeypatch.setattr(config, "print_plan", lambda _plan: None)
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(answers))

    reviewed = _interactive_review(cfg, plan, args)

    assert reviewed is not None
    assert reviewed.config.min_loss == 0
    assert reviewed.dump_interim is False
    # caller's args must not be mutated by the wizard
    assert args.dump_interim is True
