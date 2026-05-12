"""Plain and Rich renderers for pre-run plans."""

from __future__ import annotations

import sys

from rich.console import Console
from rich.padding import Padding
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from rollup.config import redact_conn_str
from rollup.plan import Check, Plan, Section


_BRAND = "bold #B22234"
_RULE = "#5A1A28"
_OK = "bold #6CC04A"
_WARN = "bold #E5A53B"
_FAIL = "bold #D14B4B"
_BODY = "white"
_DIM = "grey50"
_LABEL = "bold #E5B36B"
_NUM = "#E5B36B"
_GLYPH_OK = "✓"
_GLYPH_FAIL = "✘"
_GLYPH_WARN = "•"

_SECTION_ICONS: dict[str, str] = {
    "seeds": "▣",
    "ylt": "▶",
    "ep_summaries": "◆",
    "forecast_factors": "◇",
    "output": "◯",
}


def format_plan(plan: Plan) -> str:
    lines = ["Pipeline plan", "=" * 13, ""]
    for section in plan.sections:
        lines.append(f"[{section.title}]  {section.header}")
        for check in section.checks:
            row = f"  {check.mark} {check.label:<30}"
            if check.rows:
                row += f"  {check.rows:>8,} rows"
            else:
                row += "  " + " " * 12
            if check.note:
                row += f"   {check.note}"
            lines.append(row.rstrip())
        lines.append("")

    seed_ok = sum(1 for c in plan.seeds_section.checks if c.ok)
    seed_total = len(plan.seeds_section.checks)
    ylt_ready = sum(
        1 for v in plan.config.vendors
        if any(c.ok for s in plan.sections if s.title == f"ylt {v.name}" for c in s.checks)
    )
    ep_ready = sum(
        1 for v in plan.config.vendors
        if any(c.ok for s in plan.sections if s.title == f"ep_summaries {v.name}" for c in s.checks)
    )
    lines.append(f"Seeds: {seed_ok}/{seed_total} valid.")
    lines.append(f"YLTs:  {ylt_ready}/{len(plan.config.vendors)} vendors have data.")
    lines.append(f"EP summaries: {ep_ready}/{len(plan.config.vendors)} vendors have data.")
    if plan.config.mssql_conn_str:
        lines.append(f"SQL Server: {redact_conn_str(plan.config.mssql_conn_str)}")
    else:
        lines.append("SQL Server: not configured (parquet-only run)")
    lines.append("")
    return "\n".join(lines)


def _section_icon(title: str) -> str:
    for key, icon in _SECTION_ICONS.items():
        if title.startswith(key):
            return icon
    return "·"


def _status_pill(ok: int, total: int) -> Text:
    if total == 0:
        return Text(f"{_GLYPH_FAIL} empty", style=_FAIL)
    if ok == total:
        return Text(f"{ok}/{total} {_GLYPH_OK}", style=_OK)
    if ok == 0:
        return Text(f"{ok}/{total} {_GLYPH_FAIL}", style=_FAIL)
    return Text(f"{ok}/{total} {_GLYPH_WARN}", style=_WARN)


def _render_section_header(section: Section, console_width: int) -> Table:
    icon = _section_icon(section.title)
    ok = sum(1 for c in section.checks if c.ok)
    total = len(section.checks)

    head = Table(show_header=False, box=None, expand=True, pad_edge=False, padding=(0, 0))
    head.add_column(no_wrap=True, ratio=1, overflow="ellipsis")
    head.add_column(no_wrap=True, justify="right", min_width=10)

    left = Text.assemble(
        (icon, _LABEL),
        ("  ", ""),
        (section.title, _LABEL),
        ("    ", ""),
        (section.header, _DIM),
        ("  ", ""),
    )
    head.add_row(left, _status_pill(ok, total))
    return head


def _render_check_table(checks: list[Check]) -> Table:
    has_rows = any(c.rows for c in checks)

    table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1), expand=False)
    table.add_column(width=1, no_wrap=True)
    table.add_column(min_width=24, max_width=44, overflow="fold")
    if has_rows:
        table.add_column(justify="right", min_width=10, no_wrap=True)
    table.add_column(overflow="fold", no_wrap=False)

    for check in checks:
        glyph = _GLYPH_OK if check.ok else _GLYPH_FAIL
        glyph_style = _OK if check.ok else _FAIL
        label_style = _BODY if check.ok else _FAIL
        note_style = _DIM if check.ok else _FAIL

        cells = [
            Text(glyph, style=glyph_style),
            Text(check.label, style=label_style),
        ]
        if has_rows:
            rows_text = f"{check.rows:>7,} rows" if check.rows else ""
            cells.append(Text(rows_text, style=_NUM))
        cells.append(Text(check.note, style=note_style))
        table.add_row(*cells)
    return table


def _final_summary_line(plan: Plan) -> Text:
    seed_ok = sum(1 for c in plan.seeds_section.checks if c.ok)
    seed_total = len(plan.seeds_section.checks)
    n_vendors = len(plan.config.vendors)
    ylt_ready = sum(
        1 for v in plan.config.vendors
        if any(c.ok for s in plan.sections if s.title == f"ylt {v.name}" for c in s.checks)
    )
    ep_ready = sum(
        1 for v in plan.config.vendors
        if any(c.ok for s in plan.sections if s.title == f"ep_summaries {v.name}" for c in s.checks)
    )

    parts: list[Text] = []

    def _add(label: str, pill: Text) -> None:
        if parts:
            parts.append(Text("  │  ", style=_DIM))
        parts.append(Text.assemble((label, _DIM), ("  ", ""), pill))

    _add("seeds", _status_pill(seed_ok, seed_total))
    _add("ylt", _status_pill(ylt_ready, n_vendors))
    _add("ep", _status_pill(ep_ready, n_vendors))

    if plan.config.mssql_conn_str:
        _add("sql", Text(redact_conn_str(plan.config.mssql_conn_str), style=_BODY))
    else:
        _add("sql", Text(f"{_GLYPH_FAIL} not configured", style=_DIM))

    out = Text()
    for part in parts:
        out.append_text(part)
    return out


def print_plan(plan: Plan, console: Console | None = None) -> None:
    """Render the plan with Rich."""
    if console is None:
        console = Console()

    width = console.width or 100
    title = Text.assemble(
        ("  polars rollup pipeline  ", _BRAND),
        ("·  pre-flight plan  ", _DIM),
    )
    console.print()
    console.print(Rule(title, style=_RULE))
    console.print()

    for index, section in enumerate(plan.sections):
        console.print(_render_section_header(section, width))
        if section.checks:
            console.print(Padding(_render_check_table(section.checks), (0, 0, 0, 4)))
        if index < len(plan.sections) - 1:
            console.print()

    console.print()
    console.print(Rule(style=_RULE))
    console.print(Padding(_final_summary_line(plan), (0, 2)))
    console.print()


def confirm(plan: Plan, *, assume_yes: bool = False, stream=sys.stdout) -> bool:
    """Print the plan, ask y/N. Returns True if the user accepts."""
    use_rich = getattr(stream, "isatty", lambda: False)() and stream is sys.stdout
    if use_rich:
        print_plan(plan, console=Console(file=stream))
    else:
        print(format_plan(plan), file=stream)

    if not plan.all_seeds_ok:
        if use_rich:
            Console(file=stream).print(Text("! seeds have errors — fix before running.", style=_FAIL))
        else:
            print("! seeds have errors — fix before running.", file=stream)
    if assume_yes:
        if use_rich:
            Console(file=stream).print(Text("(--yes) proceeding", style=_OK))
        else:
            print("(--yes) proceeding", file=stream)
        return True
    if not sys.stdin.isatty():
        print("(non-interactive stdin) refusing to run without --yes", file=stream)
        return False
    try:
        prompt = "Proceed? [y/N]: "
        if use_rich:
            console = Console(file=stream)
            console.print(Text(prompt, style=_BRAND), end="")
            reply = input().strip().lower()
        else:
            reply = input(prompt).strip().lower()
    except EOFError:
        return False
    return reply in {"y", "yes"}
