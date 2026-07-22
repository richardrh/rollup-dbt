from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from types import ModuleType


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_summarizer() -> ModuleType:
    path = REPO_ROOT / "pipelines" / "summarize_pytest.py"
    spec = importlib.util.spec_from_file_location("summarize_pytest", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_parse_testsuites_root_sums_counts_and_calculates_passed(
    tmp_path: Path,
) -> None:
    summarizer = _load_summarizer()
    junit = tmp_path / "pytest.xml"
    junit.write_text(
        """<?xml version='1.0' encoding='utf-8'?>
<testsuites>
  <testsuite name='a' tests='3' failures='1' errors='0' skipped='1' />
  <testsuite name='b' tests='2' failures='0' errors='1' skipped='0' />
</testsuites>
""",
        encoding="utf-8",
    )

    summary = summarizer.parse_junit("normal", junit)

    assert summary.total == 5
    assert summary.passed == 2
    assert summary.failed == 1
    assert summary.errors == 1
    assert summary.skipped == 1
    assert summary.status == "FAILED"


def test_parse_testsuite_root_and_render_markdown(tmp_path: Path) -> None:
    summarizer = _load_summarizer()
    junit = tmp_path / "pytest.xml"
    junit.write_text(
        "<testsuite tests='4' failures='0' errors='0' skipped='1' />",
        encoding="utf-8",
    )

    markdown = summarizer.render_markdown([summarizer.parse_junit("fuzz", junit)])

    assert "| fuzz | PASSED | 4 | 3 | 0 | 0 | 1 |" in markdown
    assert "| Total | PASSED | 4 | 3 | 0 | 0 | 1 |" in markdown


def test_missing_file_is_reported_instead_of_raising(tmp_path: Path) -> None:
    summarizer = _load_summarizer()
    missing = tmp_path / "missing.xml"

    markdown = summarizer.render_markdown(
        [summarizer.parse_junit("integration", missing)]
    )

    assert "| integration | MISSING | 0 | 0 | 0 | 0 | 0 |" in markdown
    assert "| Total | MISSING | 0 | 0 | 0 | 0 | 0 |" in markdown
