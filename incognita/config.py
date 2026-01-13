import tomllib
from pathlib import Path

import typer

_config_file = Path(__file__).parent.parent / "pyproject.toml"
with _config_file.open("rb") as f:
    _config = tomllib.load(f)

_project_config = _config["project"]
_tool_config = _config["tool"]["config"]

DATA_API_PORT = _tool_config["data_api_port"]
DASHBOARD_PORT = _tool_config["dashboard_port"]
FLIGHTS_MAP_FILENAME = _tool_config["flights_map_filename"]
GPS_MAP_FILENAME = _tool_config["gps_map_filename"]
VISITED_MAP_FILENAME = _tool_config["visited_map_filename"]
DEFAULT_LOCATION = _tool_config["default_location"]


# fmt: off
def config_cli(
    all: bool = typer.Option(False, "--all", help="Show all configuration values"),
    project_name: bool = typer.Option(False, "--project-name", help=_project_config['name']),
    project_version: bool = typer.Option(False, "--project-version", help=_project_config['version']),
    data_api_port: bool = typer.Option(False, "--data-api-port", help=str(DATA_API_PORT)),
    dashboard_port: bool = typer.Option(False, "--dashboard-port", help=str(DASHBOARD_PORT)),
    flights_map: bool = typer.Option(False, "--flights-map", help=FLIGHTS_MAP_FILENAME),
    gps_map: bool = typer.Option(False, "--gps-map", help=GPS_MAP_FILENAME),
    visited_map: bool = typer.Option(False, "--visited-map", help=VISITED_MAP_FILENAME),
    default_location: bool = typer.Option(False, "--default-location", help=str(DEFAULT_LOCATION)),
) -> None:
# fmt: on
    if all:
        typer.echo(f"project_name={_project_config['name']}")
        typer.echo(f"project_version={_project_config['version']}")
        typer.echo(f"data_api_port={DATA_API_PORT}")
        typer.echo(f"dashboard_port={DASHBOARD_PORT}")
        typer.echo(f"flights_map_filename={FLIGHTS_MAP_FILENAME}")
        typer.echo(f"gps_map_filename={GPS_MAP_FILENAME}")
        typer.echo(f"visited_map_filename={VISITED_MAP_FILENAME}")
        typer.echo(f"default_location={DEFAULT_LOCATION}")
        return

    param_map = {
        project_name: _project_config["name"],
        project_version: _project_config["version"],
        data_api_port: DATA_API_PORT,
        dashboard_port: DASHBOARD_PORT,
        flights_map: FLIGHTS_MAP_FILENAME,
        gps_map: GPS_MAP_FILENAME,
        visited_map: VISITED_MAP_FILENAME,
        default_location: DEFAULT_LOCATION,
    }

    for is_set, value in param_map.items():
        if is_set:
            typer.echo(value)
            return

    typer.secho("Error: No config key specified. Use --help to see available options.", fg=typer.colors.RED, err=True)
    raise typer.Exit(1)


def main():
    typer.run(config_cli)


if __name__ == "__main__":
    main()
