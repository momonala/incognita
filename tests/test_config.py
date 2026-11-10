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
        ("--data-api-port", "5003"),
        ("--dashboard-port", "5004"),
    ],
)
def test_config_returns_single_value(flag: str, expected_output: str):
    result = runner.invoke(app, [flag])
    assert result.exit_code == 0
    assert result.stdout.strip() == expected_output


def test_config_without_flag_fails():
    result = runner.invoke(app, [])
    assert result.exit_code == 1
    assert "Error: No config key specified" in result.output
