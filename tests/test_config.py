from __future__ import annotations

from pathlib import Path

import pytest

from rollup.config import BlendingTargetPoint, load_config


def test_load_config_defaults_to_config_toml_in_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[fx]
target_currency = "USD"

[outputs]
write_stage_outputs = false
write_duckdb = false
duckdb_file = "rollup.duckdb"
stage_output_dir = "stages"
staging_dir = "staging"
intermediate_dir = "intermediate"
marts_dir = "marts"
analysis_dir = "analysis"
combined_file = "combined.parquet"
wide_file = "wide.parquet"
dialsup_file = "dialsup.parquet"
ep_report_file = "ep_report.csv"

[outputs.fanout_prefixes]
verisk = "HiscoAIR"
risklink = "HiscoRMS"

[analysis]
return_periods = [50, 500]

[vendor_years]
verisk = 123
risklink = 456

[blending]
uplift_factor_min = 0.5
uplift_factor_max = 5.0

[[blending.target_points]]
ep_type = "OEP"
return_period = 50

[[blending.target_points]]
ep_type = "OEP"
return_period = 500
""".strip(),
        encoding="utf-8",
    )

    config = load_config()

    assert config.fx.target_currency == "USD"
    assert config.analysis.simulation_counts == {"verisk": 123, "risklink": 456}
    assert config.analysis.return_periods == (50, 500)
    assert config.blending.vendor_years == {"verisk": 123, "risklink": 456}
    assert config.blending.target_points == (
        BlendingTargetPoint("OEP", 50),
        BlendingTargetPoint("OEP", 500),
    )
    assert config.blending.uplift_factor_min == 0.5
    assert config.blending.uplift_factor_max == 5.0
    assert config.outputs.write_stage_outputs is False


def test_load_config_raises_file_not_found_for_missing_path() -> None:
    with pytest.raises(FileNotFoundError, match="rollup config not found"):
        load_config("missing-config.toml")


def test_load_config_raises_value_error_for_missing_sections(tmp_path: Path) -> None:
    config_path = tmp_path / "rollup.toml"
    config_path.write_text(
        """
[fx]
target_currency = "GBP"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing required config sections"):
        load_config(config_path)


def test_load_config_populates_subregion_selection(tmp_path: Path) -> None:
    config_path = tmp_path / "rollup.toml"
    config_path.write_text(
        """
[fx]
target_currency = "GBP"

[outputs]
write_stage_outputs = true
write_duckdb = false
duckdb_file = "rollup.duckdb"
stage_output_dir = "stages"
staging_dir = "staging"
intermediate_dir = "intermediate"
marts_dir = "marts"
analysis_dir = "analysis"
combined_file = "combined.parquet"
wide_file = "wide.parquet"
dialsup_file = "dialsup.parquet"
ep_report_file = "ep_report.csv"

[outputs.fanout_prefixes]
verisk = "HiscoAIR"
risklink = "HiscoRMS"

[analysis]
return_periods = [1000]

[vendor_years]
verisk = 10000
risklink = 100000

[blending]
uplift_factor_min = 0.1
uplift_factor_max = 10.0

[[blending.target_points]]
ep_type = "OEP"
return_period = 1000

[blending.subregion_selection]
"999" = "999z"
"205" = "205a"
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.blending.subregion_selection == {999: "999z", 205: "205a"}


def test_duckdb_path_resolves_relative_to_output_root(tmp_path: Path) -> None:
    config_path = tmp_path / "rollup.toml"
    config_path.write_text(
        """
[fx]
target_currency = "GBP"

[outputs]
write_stage_outputs = true
write_duckdb = true
duckdb_file = "custom.duckdb"
stage_output_dir = "stages"
staging_dir = "staging"
intermediate_dir = "intermediate"
marts_dir = "marts"
analysis_dir = "analysis"
combined_file = "combined.parquet"
wide_file = "wide.parquet"
dialsup_file = "dialsup.parquet"
ep_report_file = "ep_report.csv"

[outputs.fanout_prefixes]
verisk = "HiscoAIR"
risklink = "HiscoRMS"

[analysis]
return_periods = [1000]

[vendor_years]
verisk = 10000
risklink = 100000

[blending]
uplift_factor_min = 0.1
uplift_factor_max = 10.0

[[blending.target_points]]
ep_type = "OEP"
return_period = 1000
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.outputs.duckdb_path(tmp_path / "output") == tmp_path / "output" / "custom.duckdb"
