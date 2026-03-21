import argparse
import time
from dataclasses import dataclass

import requests
from rich.console import Console
from rich.table import Table

BYTES_PER_MB = 1024 * 1024
DEFAULT_BASE_URL = "http://127.0.0.1:5003"
LOOKBACK_WINDOWS = [
    ("3_months", 90),
    ("2_months", 60),
    ("1_month", 30),
    ("15_days", 15),
    ("7_days", 7),
]
ENDPOINTS = ["/coordinates", "/coordinates2"]

console = Console()


@dataclass(frozen=True)
class BenchmarkResult:
    endpoint: str
    label: str
    lookback_days: int
    status_code: int
    duration_seconds: float
    payload_size_bytes: int
    coordinate_count: int | None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare /coordinates and /coordinates2 HTTP performance.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Base URL for the data API server.")
    parser.add_argument("--timeout", type=float, default=300.0, help="Per-request timeout in seconds.")
    return parser.parse_args()


def _run_benchmark(
    base_url: str, endpoint: str, label: str, lookback_days: int, timeout: float
) -> BenchmarkResult:
    lookback_hours = lookback_days * 24
    url = f"{base_url.rstrip('/')}{endpoint}"
    started_at = time.perf_counter()
    response = requests.get(url, params={"lookback_hours": lookback_hours}, timeout=timeout)
    duration_seconds = time.perf_counter() - started_at
    payload_size_bytes = len(response.content)

    coordinate_count: int | None = None
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict):
        coordinate_count = payload.get("count")

    return BenchmarkResult(
        endpoint=endpoint,
        label=label,
        lookback_days=lookback_days,
        status_code=response.status_code,
        duration_seconds=duration_seconds,
        payload_size_bytes=payload_size_bytes,
        coordinate_count=coordinate_count,
    )


def _format_payload_size(payload_size_bytes: int) -> str:
    """Return payload size in MB for table display."""
    return f"{payload_size_bytes / BYTES_PER_MB:.2f} MB"


def _format_ratio(new_value: float, old_value: float) -> str:
    """Return a human-readable comparison ratio."""
    if old_value == 0:
        return "-"
    return f"{new_value / old_value:.2f}x"


def _render_results_table(results: list[BenchmarkResult]) -> None:
    """Render benchmark results in a side-by-side comparison table."""
    results_by_window = {
        result.label: {
            window_result.endpoint: window_result
            for window_result in results
            if window_result.label == result.label
        }
        for result in results
    }

    table = Table(title="Coordinate Endpoint Comparison")
    table.add_column("Lookback", style="cyan", no_wrap=True)
    table.add_column("/coordinates", justify="right")
    table.add_column("/coordinates2", justify="right")
    table.add_column("Time Ratio", justify="right")
    table.add_column("Payload Ratio", justify="right")
    table.add_column("Count Delta", justify="right")

    for label, lookback_days in LOOKBACK_WINDOWS:
        window_results = results_by_window[label]
        coordinates_result = window_results["/coordinates"]
        coordinates2_result = window_results["/coordinates2"]
        count_delta = (coordinates2_result.coordinate_count or 0) - (coordinates_result.coordinate_count or 0)
        table.add_row(
            f"{label} ({lookback_days}d)",
            (
                f"{coordinates_result.duration_seconds:.2f}s\n"
                f"{_format_payload_size(coordinates_result.payload_size_bytes)}\n"
                f"count={coordinates_result.coordinate_count}\n"
                f"status={coordinates_result.status_code}"
            ),
            (
                f"{coordinates2_result.duration_seconds:.2f}s\n"
                f"{_format_payload_size(coordinates2_result.payload_size_bytes)}\n"
                f"count={coordinates2_result.coordinate_count}\n"
                f"status={coordinates2_result.status_code}"
            ),
            _format_ratio(coordinates2_result.duration_seconds, coordinates_result.duration_seconds),
            _format_ratio(coordinates2_result.payload_size_bytes, coordinates_result.payload_size_bytes),
            str(count_delta),
        )

    console.print(table)


def main() -> None:
    args = _parse_args()
    console.print(f"[bold]Comparing coordinate endpoints[/bold] against [cyan]{args.base_url}[/cyan]")

    results: list[BenchmarkResult] = []
    for label, lookback_days in LOOKBACK_WINDOWS:
        for endpoint in ENDPOINTS:
            result = _run_benchmark(args.base_url, endpoint, label, lookback_days, args.timeout)
            results.append(result)
    _render_results_table(results)
    console.print(f"[green]Completed[/green] {len(results)} requests")


if __name__ == "__main__":
    main()
