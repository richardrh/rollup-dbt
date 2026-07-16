from __future__ import annotations

from pathlib import Path

from rollup import cli


def _write(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("test", encoding="utf-8")
    return path


def test_cleanup_dry_run_does_not_delete_known_files(tmp_path: Path) -> None:
    output_root = tmp_path / "output"
    known_file = _write(output_root / "mts_tbl_ylt_dialsup.parquet")
    mart_file = _write(output_root / "marts" / "mart.parquet")

    exit_code = cli.main(["--output-root", str(output_root), "cleanup"])

    assert exit_code == 0
    assert known_file.is_file()
    assert mart_file.is_file()


def test_cleanup_yes_deletes_root_generated_parquets_and_mart_parquets(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "output"
    generated_files = [
        _write(output_root / "mts_tbl_ylt_combined_all_factors.parquet"),
        _write(output_root / "mts_tbl_ylt_dialsup.parquet"),
        _write(output_root / "mts_event_validation.parquet"),
        _write(output_root / "marts" / "one.parquet"),
        _write(output_root / "marts" / "two.parquet"),
    ]

    exit_code = cli.main(["--output-root", str(output_root), "cleanup", "--yes"])

    assert exit_code == 0
    assert all(not path.exists() for path in generated_files)


def test_cleanup_yes_preserves_unrelated_analysis_and_debug_files(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "output"
    preserved_files = [
        _write(output_root / "keep.parquet"),
        _write(output_root / "marts" / "keep.csv"),
        _write(output_root / "analysis" / "report.csv"),
        _write(output_root / "debug" / "frame.parquet"),
    ]
    generated_file = _write(output_root / "mts_event_validation.parquet")

    exit_code = cli.main(["--output-root", str(output_root), "cleanup", "--yes"])

    assert exit_code == 0
    assert not generated_file.exists()
    assert all(path.is_file() for path in preserved_files)
