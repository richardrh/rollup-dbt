from __future__ import annotations

from __future__ import annotations

from typing import Any

import click

from config.source_config import add_source, list_sources, load_connectors
from app.dlt_config import generate_dlt_config


def format_source_table(sources: list[dict[str, Any]]) -> str:
    if not sources:
        return "No sources configured yet."
    header = "\t".join(["Name", "Type", "Connector", "Details"])
    rows = [header]
    for source in sources:
        connector = source.get("connector", "")
        rows.append(
            "\t".join(
                [
                    source["name"],
                    source["type"],
                    connector,
                    str(source.get("params", {})),
                ]
            )
        )
    return "\n".join(rows)


def _prompt_field(field: dict[str, Any]) -> str:
    label = field.get("label", field["name"])
    default = field.get("default")
    required = field.get("required", False)
    while True:
        if default is not None:
            value = click.prompt(label, default=default, show_default=True)
        else:
            value = click.prompt(label)
        if value or not required:
            return value
        click.echo("This field is required.")


def _assign_param(params: dict[str, Any], field: dict[str, Any], value: str) -> None:
    section = field.get("section")
    if section:
        section_dict = params.setdefault(section, {})
        section_dict[field["name"]] = value
    else:
        params[field["name"]] = value


def prompt_for_params(connector_meta: dict[str, Any]) -> dict[str, Any]:
    params: dict[str, Any] = {}
    for field in connector_meta.get("fields", []):
        value = _prompt_field(field)
        _assign_param(params, field, value)
    return params


def choose_connector(
    connectors: dict[str, dict[str, Any]],
) -> tuple[str, dict[str, Any]]:
    choices = sorted(connectors.keys())
    if not choices:
        raise click.ClickException("No connectors defined in config/sources.toml")
    selection = click.prompt("Connector", type=click.Choice(choices))
    return selection, connectors[selection]


@click.command()
def configure_sources() -> None:
    """Interactive CLI to register data sources for DLT/dbt."""

    connectors = load_connectors()
    if not connectors:
        click.echo("Define connector metadata inside config/sources.toml first.")
        return

    click.echo("Current sources:\n" + format_source_table(list_sources()))

    while True:
        click.echo("\nEnter source information:")
        name = click.prompt("Logical source name (e.g. risklink_ylt)")
        connector_name, connector_meta = choose_connector(connectors)
        click.echo(connector_meta.get("description", connector_meta.get("label", "")))
        params = prompt_for_params(connector_meta)
        source = {
            "name": name,
            "type": connector_meta.get("type", "custom"),
            "connector": connector_name,
            "params": params,
        }
        add_source(source)
        click.echo("Source registered:")
        click.echo(format_source_table(list_sources()))
        click.echo("Overrides stored in config/sources.toml (source controlled).")

        generate_dlt_config()
        click.echo("DLT config regenerated at .dlt/config.toml")

        if not click.confirm("Add another source?"):
            break


if __name__ == "__main__":
    configure_sources()
