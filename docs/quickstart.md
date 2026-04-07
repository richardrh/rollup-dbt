# Quick start


## Config
The ./config folder contains config.toml.
This config file specifies all the required config for Laiter in one file.

Users can edit this config file directly and then run the workflows manually.
The recommended approach is to use the CLI tool.

## CLI
Laiter ships with a cli tool that prompts users to confirm configuration.

run this with

```bash

uv run python -m cli

```

User will be prompted to confirm or otherwise the config settings such as:
- Database paths
- Source paths for ELTs/YLTs
- Output file name for CDS Staging

The CLI tool writes this config to ./config/config.toml and executes the workflows.

### Manually running workflows
Laiter uses Dagu as a lightweight orchestrator to execute the end to end
workflow.

Dagu ships with it's own web gui and allows individual steps to be executed
or the entire workflow to be run in one step manually.


