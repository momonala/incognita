"""Print days where step_count has more than one hardware version recording data."""

import argparse
import sqlite3
from pathlib import Path

import pandas as pd
from rich.console import Console
from rich.table import Table

from incognita.health_database import HEALTH_DB_FILE

console = Console()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Find days with step data from multiple hardware versions.")
    parser.add_argument(
        "--db", default=HEALTH_DB_FILE, help=f"Path to health_data.db (default: {HEALTH_DB_FILE})"
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql(
            "SELECT date(start) AS day, device_hardware_version, SUM(value) AS steps, COUNT(*) AS samples "
            "FROM step_count "
            "GROUP BY day, device_hardware_version "
            "ORDER BY day, device_hardware_version",
            conn,
        )

    if df.empty:
        console.print("[yellow]No step_count data found.[/yellow]")
        return

    devices_per_day = df.groupby("day")["device_hardware_version"].nunique()
    duplicate_days = devices_per_day[devices_per_day > 1].index

    console.print(f"\nTotal days with step data: {devices_per_day.shape[0]:,}")
    console.print(f"Days with [bold]multiple hardware versions[/bold]: {len(duplicate_days):,}\n")

    if duplicate_days.empty:
        console.print("[green]No duplicate hardware days found.[/green]")
        return

    dupes = df[df["day"].isin(duplicate_days)].copy()
    dupes["steps"] = dupes["steps"].round(0).astype(int)

    table = Table("Day", "Hardware", "Steps", "Samples", box=None, show_header=True)
    table.columns[0].style = "bold"

    max_samples_by_day = dupes.groupby("day")["samples"].transform("max")
    dupes["is_max"] = dupes["samples"] == max_samples_by_day

    prev_day = None
    for row in dupes.itertuples(index=False):
        day = row.day if row.day != prev_day else ""
        style = "bold green" if row.is_max else ""
        table.add_row(
            day, row.device_hardware_version or "(none)", f"{row.steps:,}", str(row.samples), style=style
        )
        prev_day = row.day

    console.print(table)


if __name__ == "__main__":
    main()
