from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import sys
import xml.etree.ElementTree as ET


@dataclass(frozen=True)
class SuiteSummary:
    name: str
    path: Path
    total: int = 0
    passed: int = 0
    failed: int = 0
    errors: int = 0
    skipped: int = 0
    missing: bool = False

    @property
    def status(self) -> str:
        if self.missing:
            return "MISSING"
        if self.failed or self.errors:
            return "FAILED"
        return "PASSED"


def _count(root: ET.Element, attribute: str) -> int:
    value = root.attrib.get(attribute, "0")
    try:
        return int(value)
    except ValueError:
        return 0


def _sum_counts(elements: list[ET.Element], attribute: str) -> int:
    return sum(_count(element, attribute) for element in elements)


def parse_junit(name: str, path: Path) -> SuiteSummary:
    if not path.exists():
        return SuiteSummary(name=name, path=path, missing=True)

    root = ET.parse(path).getroot()
    if root.tag == "testsuites":
        suites = list(root.findall("testsuite"))
        if suites:
            total = _sum_counts(suites, "tests")
            failed = _sum_counts(suites, "failures")
            errors = _sum_counts(suites, "errors")
            skipped = _sum_counts(suites, "skipped")
        else:
            total = _count(root, "tests")
            failed = _count(root, "failures")
            errors = _count(root, "errors")
            skipped = _count(root, "skipped")
    elif root.tag == "testsuite":
        total = _count(root, "tests")
        failed = _count(root, "failures")
        errors = _count(root, "errors")
        skipped = _count(root, "skipped")
    else:
        total = failed = errors = skipped = 0

    passed = max(total - failed - errors - skipped, 0)
    return SuiteSummary(
        name=name,
        path=path,
        total=total,
        passed=passed,
        failed=failed,
        errors=errors,
        skipped=skipped,
    )


def totals(summaries: list[SuiteSummary]) -> SuiteSummary:
    present = [summary for summary in summaries if not summary.missing]
    return SuiteSummary(
        name="Total",
        path=Path(""),
        total=sum(summary.total for summary in present),
        passed=sum(summary.passed for summary in present),
        failed=sum(summary.failed for summary in present),
        errors=sum(summary.errors for summary in present),
        skipped=sum(summary.skipped for summary in present),
        missing=any(summary.missing for summary in summaries),
    )


def render_markdown(summaries: list[SuiteSummary]) -> str:
    rows = [*summaries, totals(summaries)]
    lines = [
        "# Pytest results summary",
        "",
        "| Suite | Status | Total | Passed | Failed | Errors | Skipped |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for summary in rows:
        lines.append(
            "| "
            f"{summary.name} | {summary.status} | {summary.total} | "
            f"{summary.passed} | {summary.failed} | {summary.errors} | "
            f"{summary.skipped} |"
        )
    lines.append("")
    return "\n".join(lines)


def _suite(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("suite must use NAME=PATH")
    name, path = value.split("=", 1)
    if not name or not path:
        raise argparse.ArgumentTypeError("suite must use NAME=PATH")
    return name, Path(path)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize pytest JUnit XML files.")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--suite", action="append", required=True, type=_suite)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    summaries = [parse_junit(name, path) for name, path in args.suite]
    markdown = render_markdown(summaries)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(markdown, encoding="utf-8")
    print(markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
