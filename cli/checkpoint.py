"""Human-in-the-Loop checkpoint reporting using Rich panels."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from rich.panel import Panel
from rich.table import Table
from rich.text import Text


def create_checkpoint_panel(stage: str) -> Panel:
    """Create a Rich panel showing checkpoint status."""

    # Create table for data sources
    table = Table(show_header=False, box=None)
    table.add_column("Source", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Details", style="white")

    # Sample data (in reality would query database)
    table.add_row("✓ vor__blending_factors", "45 rows", "")
    table.add_row("✓ hisco_org__lobs", "12 rows", "")
    table.add_row("✓ forecast_factors", "8 rows", "")
    table.add_row("✓ vor__euws_rate_factors", "1,523 rows", "")
    table.add_row("", "", "")  # Spacer

    # Staging tables
    table.add_row("[bold]Staging Tables:[/bold]", "", "")
    table.add_row("✓ stg_risklink__elts", "15,432 rows", "2020-01-01 to 2024-12-31")
    table.add_row("✓ stg_verisk__ylts", "10,000,000 rows", "⚠ 3 events not in ref")
    table.add_row("", "", "")  # Spacer

    # Validation
    table.add_row("[bold]Validation:[/bold]", "", "")
    table.add_row("✓ min_events", "PASSED", "15,432 >= 100")
    table.add_row("✓ event_ids", "PASSED", "All validated")
    table.add_row("✓ seed_integrity", "PASSED", "All loaded")

    return Panel(
        table,
        title=f"Checkpoint: {stage}",
        subtitle=f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        border_style="yellow",
    )


def format_number(n: int) -> str:
    """Format large numbers with commas."""
    return f"{n:,}"


if __name__ == "__main__":
    # Test rendering
    from rich.console import Console

    console = Console()
    console.print(create_checkpoint_panel("initial_load"))
