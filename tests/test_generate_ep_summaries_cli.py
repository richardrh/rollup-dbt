from __future__ import annotations

import builtins
from collections.abc import Iterator
from pathlib import Path

import polars as pl

from rollup import cli


def _write_minimal_wide_csv(path: Path, *, aal: float = 12.5) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            {
                "id": "TEST_ANALYSIS",
                "modelled_lob": "TEST_LOB",
                "modelled_peril": "TEST_PERIL",
                "AAL_0": aal,
                "AEP_100": 1000.0,
            },
        ]
    ).write_csv(path)


def _mock_input(monkeypatch, responses: list[str]) -> None:
    response_iter: Iterator[str] = iter(responses)

    def input_response(prompt: str = "") -> str:
        try:
            return next(response_iter)
        except StopIteration:
            raise AssertionError(f"unexpected prompt: {prompt}") from None

    monkeypatch.setattr(builtins, "input", input_response)


def test_generate_ep_summaries_interactive_selects_file_and_overwrites_output(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    data_root = tmp_path / "data"
    csv_path = data_root / "ep_summaries" / "verisk" / "selected.csv"
    output_path = data_root / "ep_summaries" / "verisk" / "verisk_ep_summary.long.csv"
    _write_minimal_wide_csv(csv_path)
    output_path.write_text("old contents\n", encoding="utf-8")
    _mock_input(monkeypatch, ["1", "1", "y"])

    exit_code = cli.main(["--data-root", str(data_root), "generate-ep-summaries"])

    assert exit_code == 0
    output = pl.read_csv(output_path)
    assert output.columns == [
        "vendor",
        "analysis_id",
        "modelled_lob",
        "modelled_peril",
        "ep_type",
        "return_period",
        "loss",
    ]
    assert output.to_dicts() == [
        {
            "vendor": "verisk",
            "analysis_id": "TEST_ANALYSIS",
            "modelled_lob": "TEST_LOB",
            "modelled_peril": "TEST_PERIL",
            "ep_type": "AAL",
            "return_period": 0,
            "loss": 12.5,
        },
        {
            "vendor": "verisk",
            "analysis_id": "TEST_ANALYSIS",
            "modelled_lob": "TEST_LOB",
            "modelled_peril": "TEST_PERIL",
            "ep_type": "AEP",
            "return_period": 100,
            "loss": 1000.0,
        },
    ]
    captured = capsys.readouterr().out
    assert "Select EP summary vendor:" in captured
    assert "Select source wide CSV:" in captured
    assert "EP summary written to" in captured


def test_generate_ep_summaries_interactive_writes_new_output_without_confirmation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "data"
    csv_path = data_root / "ep_summaries" / "verisk" / "selected.csv"
    output_path = data_root / "ep_summaries" / "verisk" / "verisk_ep_summary.long.csv"
    _write_minimal_wide_csv(csv_path, aal=37.5)
    _mock_input(monkeypatch, ["1", "1"])

    exit_code = cli.main(["--data-root", str(data_root), "generate-ep-summaries"])

    assert exit_code == 0
    assert pl.read_csv(output_path).item(0, "loss") == 37.5


def test_generate_ep_summaries_non_interactive_accepts_csv_filename(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    data_root = tmp_path / "data"
    csv_path = data_root / "ep_summaries" / "verisk" / "selected.csv"
    output_path = data_root / "ep_summaries" / "verisk" / "verisk_ep_summary.long.csv"
    _write_minimal_wide_csv(csv_path, aal=25.0)
    monkeypatch.setattr(
        builtins,
        "input",
        lambda prompt="": (_ for _ in ()).throw(AssertionError("input should not be called")),
    )

    exit_code = cli.main(
        [
            "--data-root",
            str(data_root),
            "generate-ep-summaries",
            "--vendor",
            "verisk",
            "--csv",
            "selected.csv",
            "--yes",
        ]
    )

    assert exit_code == 0
    assert pl.read_csv(output_path).item(0, "loss") == 25.0
    captured = capsys.readouterr().out
    assert "EP summary written to" in captured
    assert "EP summary generation:" in captured
    assert "Vendor: verisk" in captured
    assert f"CSV: {csv_path}" in captured
    assert f"Output: {output_path}" in captured
    assert "Reading CSV..." in captured
    assert "Writing canonical long CSV..." in captured
    assert "Done in " in captured
    assert "EP summary overview:" in captured
    assert "Rows: 2" in captured
    assert "Columns (7): vendor, analysis_id, modelled_lob, modelled_peril, ep_type, return_period, loss" in captured
    assert "Vendors: verisk" in captured
    assert "EP type counts: AAL=1, AEP=1" in captured
    assert "Modelled LOB/peril pairs: 1" in captured
    assert "Return period range: 0-100" in captured


def test_generate_ep_summaries_explicit_args_without_yes_skip_prompt_for_new_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "data"
    csv_path = data_root / "ep_summaries" / "verisk" / "selected.csv"
    output_path = data_root / "ep_summaries" / "verisk" / "verisk_ep_summary.long.csv"
    _write_minimal_wide_csv(csv_path, aal=50.0)
    monkeypatch.setattr(
        builtins,
        "input",
        lambda prompt="": (_ for _ in ()).throw(AssertionError("input should not be called")),
    )

    exit_code = cli.main(
        [
            "--data-root",
            str(data_root),
            "generate-ep-summaries",
            "--vendor",
            "verisk",
            "--csv",
            "selected.csv",
        ]
    )

    assert exit_code == 0
    assert pl.read_csv(output_path).item(0, "loss") == 50.0


def test_generate_ep_summaries_returns_nonzero_when_input_ends(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    data_root = tmp_path / "data"
    csv_path = data_root / "ep_summaries" / "verisk" / "selected.csv"
    _write_minimal_wide_csv(csv_path)
    monkeypatch.setattr(
        builtins,
        "input",
        lambda prompt="": (_ for _ in ()).throw(EOFError),
    )

    exit_code = cli.main(["--data-root", str(data_root), "generate-ep-summaries"])

    assert exit_code == 1
    assert "Input ended before EP summary generation could continue." in capsys.readouterr().err


def test_generate_ep_summaries_ctrl_c_returns_clean_cancel_status(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    data_root = tmp_path / "data"
    csv_path = data_root / "ep_summaries" / "verisk" / "selected.csv"
    _write_minimal_wide_csv(csv_path)
    monkeypatch.setattr(
        builtins,
        "input",
        lambda prompt="": (_ for _ in ()).throw(KeyboardInterrupt),
    )

    exit_code = cli.main(["--data-root", str(data_root), "generate-ep-summaries"])

    captured = capsys.readouterr()
    assert exit_code == 130
    assert "EP summary generation cancelled; no files overwritten." in captured.out
    assert "Traceback" not in captured.out
    assert "Traceback" not in captured.err


def test_generate_ep_summaries_parsing_error_is_user_friendly(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    data_root = tmp_path / "data"
    csv_path = data_root / "ep_summaries" / "verisk" / "selected.csv"
    _write_minimal_wide_csv(csv_path)

    def fail_generation(
        data_root: Path,
        vendor: str,
        csv_path: Path,
        *,
        status_callback=None,
    ) -> Path:
        raise ValueError("missing required EP summary columns: modelled_lob")

    monkeypatch.setattr(cli, "generate_vendor_ep_summary", fail_generation)

    exit_code = cli.main(
        [
            "--data-root",
            str(data_root),
            "generate-ep-summaries",
            "--vendor",
            "verisk",
            "--csv",
            "selected.csv",
            "--yes",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "EP summary generation failed: missing required EP summary columns: modelled_lob" in captured.err
    assert "Traceback" not in captured.err


def test_generate_ep_summaries_confirmation_defaults_to_no_without_overwrite(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "data"
    csv_path = data_root / "ep_summaries" / "verisk" / "selected.csv"
    output_path = data_root / "ep_summaries" / "verisk" / "verisk_ep_summary.long.csv"
    _write_minimal_wide_csv(csv_path)
    output_path.write_text("old contents\n", encoding="utf-8")
    _mock_input(monkeypatch, ["1", "1", ""])

    exit_code = cli.main(["--data-root", str(data_root), "generate-ep-summaries"])

    assert exit_code == 0
    assert output_path.read_text(encoding="utf-8") == "old contents\n"


def test_generate_ep_summaries_reports_missing_csv(tmp_path: Path, capsys) -> None:
    data_root = tmp_path / "data"

    exit_code = cli.main(
        [
            "--data-root",
            str(data_root),
            "generate-ep-summaries",
            "--vendor",
            "verisk",
            "--csv",
            "missing.csv",
        ]
    )

    assert exit_code == 1
    assert "CSV file not found: missing.csv" in capsys.readouterr().err
