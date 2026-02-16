"""Interactive CLI for Laiter with Rich UI and Dagu integration."""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import click
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
)
from rich.table import Table

from config.loader import load_config, save_config, get_config_summary

console = Console()


def show_spinner(message: str, duration: float = 2.0) -> None:
    """Show a spinner with a message for a duration."""
    with console.status(f"[bold green]{message}", spinner="dots"):
        time.sleep(duration)


def render_config_panel(config: dict[str, Any]) -> Panel:
    """Render configuration in a Rich panel."""
    table = Table(show_header=False, box=None)
    table.add_column("Key", style="cyan")
    table.add_column("Value", style="white")

    # Project info
    project = config.get("project", {})
    table.add_row("Project", project.get("name", "N/A"))
    table.add_row("Environment", project.get("environment", "N/A"))
    table.add_row("Version", project.get("version", "N/A"))
    table.add_row("", "")  # Spacer

    # Sources
    sources = config.get("sources", {})
    table.add_row("[bold]Sources:[/bold]", "")

    risklink_path = sources.get("risklink_elt_csv", "N/A")
    if len(risklink_path) > 45:
        risklink_path = risklink_path[:42] + "..."
    table.add_row("  Risklink ELT", risklink_path)

    verisk_path = sources.get("verisk_ylt_csv", "N/A")
    if len(verisk_path) > 45:
        verisk_path = verisk_path[:42] + "..."
    table.add_row("  Verisk YLT", verisk_path)
    table.add_row("", "")  # Spacer

    # Database
    db = config.get("database", {})
    schemas = config.get("schemas", {})
    table.add_row("[bold]Destination:[/bold]", "")
    table.add_row("  Host", db.get("host", "N/A"))
    table.add_row("  Database", db.get("database", "N/A"))
    table.add_row("  Raw Schema", schemas.get("raw_schema", "N/A"))
    table.add_row("  Analytics Schema", schemas.get("analytics_schema", "N/A"))
    table.add_row("", "")  # Spacer

    # Simulation
    sim = config.get("simulation", {})
    table.add_row("[bold]Simulation:[/bold]", "")
    table.add_row("  Simulations", f"{sim.get('n_simulations', 0):,}")
    analysis_ids = sim.get("analysis_ids", [])
    ids_str = str(analysis_ids) if analysis_ids else "Not configured"
    if len(ids_str) > 45:
        ids_str = ids_str[:42] + "..."
    table.add_row("  Analysis IDs", ids_str)

    return Panel(table, title="Configuration", border_style="blue")


@click.group()
def cli() -> None:
    """Laiter Rollup - Interactive CLI with Dagu orchestration."""
    pass


@click.command()
def verify_installed_packages() -> None:
    """
    This command we want to check uv is synced and all dbt deps
    are installed and report it to user
    """
    console.print(Panel.fit("Laiter Package Install Status"))
    config = load_config()

    table = Table(show_header=False, box=None)
    table.add_column("Source System", style="cyan")
    table.add_column("Installed", style="white")

    console.print(table)
    console.print()

    # Check uv is synced or not
    show_spinner("Checking uv packages...", 0.5)
    result_uv_synced = subprocess.run(["uv", "sync"], capture_output=True, text=True)
    if result_uv_synced.returncode == 0:
        is_uv_synced = True
    else:
        is_uv_synced = False

    # check dbt deps are installed
    # TODO: Fix the dbt call so it runs inside ./dbt
    show_spinner("Checking / installed dbt deps...", 0.5)
    result_dbt_deps = subprocess.run(
        ["dbt", "deps"], capture_output=True, text=True, cwd="./dbt"
    )

    # TODO:  Verify dagu is installed here (not in stats below)


@cli.command()
def status() -> None:
    """Show current workflow status."""
    console.print(Panel.fit("[bold blue]Laiter Rollup Status[/bold blue]"))

    config = load_config()
    project = config.get("project", {})

    table = Table(show_header=False, box=None)
    table.add_column("Item", style="cyan")
    table.add_column("Value", style="white")
    table.add_row("Project", project.get("name", "N/A"))
    table.add_row("Environment", project.get("environment", "N/A"))
    table.add_row("Version", project.get("version", "N/A"))
    console.print(table)
    console.print()

    # Check Dagu status
    show_spinner("Checking Dagu status...", 0.5)

    try:
        result = subprocess.run(
            ["dagu", "version"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            console.print("[green]✓ Dagu is installed[/green]")
        else:
            console.print("[yellow]⚠ Dagu status unknown[/yellow]")
    except FileNotFoundError:
        console.print("[red]✗ Dagu is not installed or not in PATH[/red]")


@cli.command()
@click.option("--risklink-elt", help="Override Risklink ELT CSV path")
@click.option("--verisk-ylt", help="Override Verisk YLT CSV path")
@click.option("--analysis-ids", help="Comma-separated list of Risklink analysis IDs")
@click.option("--n-simulations", type=int, help="Number of Risklink simulations")
@click.option("--skip-confirm", is_flag=True, help="Skip confirmation prompts")
def run(
    risklink_elt: str | None,
    verisk_ylt: str | None,
    analysis_ids: str | None,
    n_simulations: int | None,
    skip_confirm: bool,
) -> None:
    """Run the complete Laiter rollup workflow via Dagu."""

    console.print(Panel.fit("[bold green]🚀 Laiter Rollup Workflow[/bold green]"))
    console.print()

    # Load configuration
    show_spinner("Loading configuration...", 0.5)
    config = load_config()

    # Apply CLI overrides
    if risklink_elt:
        config["sources"]["risklink_elt_csv"] = risklink_elt
    if verisk_ylt:
        config["sources"]["verisk_ylt_csv"] = verisk_ylt
    if analysis_ids:
        config["simulation"]["analysis_ids"] = [
            int(x.strip()) for x in analysis_ids.split(",")
        ]
    if n_simulations:
        config["simulation"]["n_simulations"] = n_simulations

    # Display configuration
    console.print(render_config_panel(config))
    console.print()

    # Check for required configuration
    missing = []
    if not config["simulation"].get("analysis_ids"):
        missing.append("Risklink analysis_ids")

    if missing:
        console.print("[red]✗ Missing required configuration:[/red]")
        for item in missing:
            console.print(f"  - {item}")
        console.print()
        console.print("[yellow]Use 'laiter configure' or pass CLI options.[/yellow]")
        sys.exit(1)

    # Confirm with user
    if not skip_confirm:
        if not click.confirm("Start workflow with current configuration?"):
            console.print("[yellow]Workflow cancelled.[/yellow]")
            sys.exit(0)

    console.print()
    console.print("[green]✓ Configuration confirmed. Starting Dagu workflow...[/green]")
    console.print()

    # Trigger Dagu workflow
    trigger_dagu_workflow("laiter_rollup")


def trigger_dagu_workflow(workflow_name: str) -> None:
    """Trigger a Dagu workflow and show progress."""

    console.print(
        Panel(f"[bold cyan]🔄 Running Dagu Workflow: {workflow_name}[/bold cyan]")
    )
    console.print()

    # Step 1: Check if workflow exists
    show_spinner("Checking workflow...", 0.5)

    # Step 2: Start workflow
    console.print("[yellow]▶ Starting workflow...[/yellow]")

    try:
        # Workflow steps
        workflow_steps = [
            ("load_seeds", "Loading seed files", 5),
            ("load_risklink_elt", "Loading Risklink ELT from CSV", 10),
            ("load_verisk_ylt", "Loading Verisk YLT from CSV", 10),
            ("checkpoint_initial", "⏸ Human checkpoint: Review initial load", 0),
            ("run_risklink_sim", "Running Risklink simulation", 30),
            ("run_dbt_models", "Building dbt models", 20),
            ("checkpoint_blending", "⏸ Human checkpoint: Review blending", 0),
            ("generate_exports", "Generating exports", 10),
            ("checkpoint_export", "⏸ Human checkpoint: Pre-export review", 0),
            ("export_to_cds", "Exporting to CDS Staging", 10),
        ]

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(complete_style="green", finished_style="green"),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            overall = progress.add_task(
                "[cyan]Overall Progress", total=len(workflow_steps)
            )

            for step_id, step_name, duration in workflow_steps:
                task = progress.add_task(
                    f"[white]{step_name}[/white]", total=duration or 1
                )

                if "checkpoint" in step_id:
                    # Human checkpoint
                    progress.stop()
                    console.print()
                    console.print(
                        Panel(
                            f"[bold yellow]⏸ Checkpoint: {step_name}[/bold yellow]\n\n"
                            "The workflow is paused for human review.\n"
                            "Run: [cyan]laiter checkpoint {step_id}[/cyan] to continue",
                            title="Human-in-the-Loop",
                            border_style="yellow",
                        )
                    )
                    # In reality, Dagu would pause here and wait for signal
                    input("Press Enter to continue...")
                    progress.start()
                else:
                    # Automated step
                    for i in range(duration):
                        time.sleep(0.1)
                        progress.update(task, advance=1)

                progress.update(task, completed=True)
                progress.update(overall, advance=1)

        console.print()
        console.print(
            Panel.fit("[bold green]✅ Workflow completed successfully![/bold green]")
        )

    except subprocess.CalledProcessError as e:
        console.print(f"[red]✗ Workflow failed: {e}[/red]")
        sys.exit(1)


@cli.command()
def configure() -> None:
    """Interactively configure the Laiter rollup."""
    console.print(Panel.fit("[bold blue]⚙️  Configuration[/bold blue]"))
    console.print()

    # Load current config
    config = load_config()

    console.print("Current configuration values shown in [brackets].")
    console.print("Press Enter to keep current value.")
    console.print()

    # Project settings
    console.print("[bold cyan]Project Settings:[/bold cyan]")
    project_name = click.prompt(
        "  Project name",
        default=config["project"]["name"],
    )

    console.print()
    console.print("[bold cyan]Risklink Settings:[/bold cyan]")
    elt_path = click.prompt(
        "  ELT CSV path",
        default=config["sources"]["risklink_elt_csv"],
    )

    current_ids = ",".join(str(x) for x in config["simulation"]["analysis_ids"])
    analysis_ids_str = click.prompt(
        "  Analysis IDs (comma-separated)",
        default=current_ids if current_ids else "",
    )

    n_simulations = click.prompt(
        "  Number of simulations",
        type=int,
        default=config["simulation"]["n_simulations"],
    )

    console.print()
    console.print("[bold cyan]Verisk Settings:[/bold cyan]")
    ylt_path = click.prompt(
        "  YLT CSV path",
        default=config["sources"]["verisk_ylt_csv"],
    )

    console.print()
    console.print("[bold cyan]Destination Settings:[/bold cyan]")
    host = click.prompt(
        "  SQL Server host",
        default=config["database"]["host"],
    )
    database = click.prompt(
        "  Database name",
        default=config["database"]["database"],
    )
    raw_schema = click.prompt(
        "  Raw schema (DLT loads)",
        default=config["schemas"]["raw_schema"],
    )
    analytics_schema = click.prompt(
        "  Analytics schema (dbt models)",
        default=config["schemas"]["analytics_schema"],
    )

    # Summary
    console.print()
    console.print("[bold]Configuration Summary:[/bold]")
    table = Table(show_header=False)
    table.add_column("Setting", style="cyan")
    table.add_column("Value")
    table.add_row("Project", project_name)
    table.add_row("Risklink ELT", elt_path)
    table.add_row("Risklink Analyses", analysis_ids_str or "None")
    table.add_row("Verisk YLT", ylt_path)
    table.add_row("Destination", f"{host}/{database}")
    table.add_row("Raw Schema", raw_schema)
    table.add_row("Analytics Schema", analytics_schema)
    console.print(table)

    if click.confirm("Save configuration?"):
        # Update config dict with new values
        config["project"]["name"] = project_name
        config["sources"]["risklink_elt_csv"] = elt_path
        config["sources"]["verisk_ylt_csv"] = ylt_path
        config["simulation"]["n_simulations"] = n_simulations
        config["database"]["host"] = host
        config["database"]["database"] = database
        config["schemas"]["raw_schema"] = raw_schema
        config["schemas"]["analytics_schema"] = analytics_schema

        # Parse analysis IDs
        if analysis_ids_str:
            config["simulation"]["analysis_ids"] = [
                int(x.strip()) for x in analysis_ids_str.split(",") if x.strip()
            ]
        else:
            config["simulation"]["analysis_ids"] = []

        # Save to file
        save_config(config)
        console.print("[green]✓ Configuration saved to config/config.toml[/green]")
    else:
        console.print("[yellow]✗ Configuration discarded.[/yellow]")


@cli.command()
@click.argument(
    "stage",
    type=click.Choice(
        ["initial_load", "post_simulation", "post_blending", "pre_export"]
    ),
)
def checkpoint(stage: str) -> None:
    """Resume workflow from a checkpoint."""
    from app.checkpoint import create_checkpoint_panel

    console.print(Panel.fit(f"[bold yellow]⏸ Checkpoint: {stage}[/bold yellow]"))

    # Create checkpoint report panel
    panel = create_checkpoint_panel(stage)
    console.print(panel)

    if click.confirm("\nContinue to next stage?", default=True):
        # Signal Dagu to continue
        console.print("[green]✓ Resuming workflow...[/green]")
    else:
        console.print("[yellow]⏸ Workflow remains paused.[/yellow]")


if __name__ == "__main__":
    cli()
