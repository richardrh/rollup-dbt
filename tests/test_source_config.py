from __future__ import annotations

import tomli_w
import pytest

from config import source_config


def test_save_and_load_sources(tmp_path, monkeypatch):
    config_path = tmp_path / "sources.toml"
    monkeypatch.setattr(source_config, "CONFIG_PATH", config_path)

    assert source_config.list_sources() == []
    assert "sql_database" in source_config.load_connectors()

    definition = {
        "name": "example",
        "type": "sql",
        "connector": "sql_database",
        "params": {"host": "localhost", "database": "master"},
    }
    source_config.add_source(definition)

    loaded = source_config.list_sources()
    assert len(loaded) == 1
    assert loaded[0]["name"] == "example"
    assert loaded[0]["params"]["host"] == "localhost"

    updated = {**definition, "params": {"host": "prod"}}
    source_config.add_source(updated)
    assert source_config.list_sources()[0]["params"]["host"] == "prod"


def test_load_connectors_reads_metadata(tmp_path, monkeypatch):
    config_path = tmp_path / "sources.toml"
    raw = {
        "connectors": {
            "custom": {"type": "sql", "fields": [{"name": "host", "label": "Host"}]}
        },
        "sources": [],
    }
    config_path.write_text(tomli_w.dumps(raw))
    monkeypatch.setattr(source_config, "CONFIG_PATH", config_path)

    connectors = source_config.load_connectors()
    assert "custom" in connectors
    assert connectors["custom"]["fields"][0]["name"] == "host"

    with pytest.raises(KeyError):
        source_config.get_connector("missing")
