import pytest
import typer
from typer.testing import CliRunner

from incognita.config import config_cli

app = typer.Typer()
app.command()(config_cli)

runner = CliRunner()


@pytest.mark.parametrize(
    "flag,expected_output",
    [
        ("--project-name", "incognita"),
        ("--project-version", "0.1.0"),
        ("--data-api-port", "5003"),
        ("--dashboard-port", "5004"),
        ("--flights-map", "static/flights_pydeck.html"),
        ("--gps-map", "static/gps_pydeck.html"),
        ("--visited-map", "static/visited_pydeck.html"),
    ],
)
def test_config_returns_single_value(flag: str, expected_output: str):
    result = runner.invoke(app, [flag])
    assert result.exit_code == 0
    assert result.stdout.strip() == expected_output


def test_config_all_returns_all_values():
    result = runner.invoke(app, ["--all"])
    assert result.exit_code == 0
    assert "project_name=incognita" in result.stdout
    assert "data_api_port=5003" in result.stdout
    assert "dashboard_port=5004" in result.stdout
    assert "flights_map_filename=" in result.stdout


def test_config_without_flag_fails():
    result = runner.invoke(app, [])
    assert result.exit_code == 1
    assert "Error: No config key specified" in result.output
