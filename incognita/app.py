import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from flask import Flask, jsonify, render_template, request, send_from_directory

from incognita.config import DASHBOARD_PORT, GPS_MAP_FILENAME, VISITED_MAP_FILENAME
from incognita.countries import (
    get_countries_df,
    get_countries_visited,
    get_visited_stats,
    visited_df_to_deck_map,
)
from incognita.data_models import HealthKitExportType
from incognita.flights import (
    flights_df_to_graph,
    get_countries,
    get_flight_aggregations,
    get_flight_dist_space_stats,
    get_flights_df,
    get_flights_routes_for_map,
    get_flights_stats,
    get_flights_visited_countries_for_map,
)
from incognita.gps_trips_renderer import (
    get_latest_location_snapshot,
    get_trip_points_for_date_range,
    get_trips_for_date_range,
    render_trips_to_file,
)
from incognita.health_database import (
    HEALTH_DB_FILE,
    filter_dominant_hardware_rows,
    get_health_meta,
    load_metric_df,
)
from incognita.observability import configure_logging
from incognita.utils import BYTES_PER_MB, DEFAULT_MAP_BOX, google_sheets_document_url
from incognita.values import MAPBOX_API_KEY

GPS_DEFAULT_DAYS_BACK = 30
# Date ranges this short (inclusive) render as an animated comet-trace instead of a static map.
GPS_ANIMATE_MAX_DAYS = 7
DATE_FMT = "%Y-%m-%d"
FLIGHTS_TABLE_DATE_FMT = "%Y.%m.%d"
LIVE_STALENESS_GREEN_MINUTES = 5
LIVE_STALENESS_YELLOW_MINUTES = 15

_last_heartbeat_time: datetime | None = None

_base_dir = Path(__file__).parent.parent
app = Flask(
    __name__,
    static_folder=str(_base_dir / "static"),
    template_folder=str(_base_dir / "templates"),
)

logger = logging.getLogger(__name__)


def get_age_of_map_update(filename: str) -> str:
    """Return last-modified date of file as dd.mm.yyyy HH:MM, or 'N/A' if missing."""
    path = Path(filename)
    if not path.exists():
        logger.debug("%s not found", filename)
        return "N/A"
    return time.strftime("%d.%m.%Y %H:%M", time.localtime(path.stat().st_mtime))


@app.route("/favicon.ico")
def favicon():
    """Serve favicon."""
    return send_from_directory(app.static_folder, "favicon.ico", mimetype="image/vnd.microsoft.icon")


@app.route("/")
def index():
    """Serve home page."""
    return render_template("index.html")


def _flights_table_records(flights_df):
    """Return list of dicts for flights table: date formatted, columns normalized for template."""
    table_df = flights_df.copy()
    table_df["Date"] = table_df["Date"].dt.strftime(FLIGHTS_TABLE_DATE_FMT)
    table_df = table_df[
        [
            "Date",
            "Origin",
            "Destination",
            "Flight #",
            "departure_airport",
            "arrival_airport",
            "Distance km",
        ]
    ]
    table_df.columns = [c.lower().replace(" ", "_").replace("#", "") for c in table_df.columns]
    return table_df.to_dict(orient="records")


@app.route("/flights")
def flights():
    """Render flights page: stats, map, plots, and table."""
    flights_df = get_flights_df()
    flights_stats = get_flights_stats(flights_df)
    flight_dist_space_stats = get_flight_dist_space_stats(flights_df["Distance km"].sum())
    airport_countries_visited = get_countries(flights_df)
    flags = " ".join(c.flag for c in airport_countries_visited)

    aggregations = get_flight_aggregations(flights_df)
    graphs = {
        "year": flights_df_to_graph(flights_df, "year", aggregations),
        "month": flights_df_to_graph(flights_df, "month", aggregations),
        "dayofweek": flights_df_to_graph(flights_df, "dayofweek", aggregations),
    }

    return render_template(
        "flights.html",
        modified_date="Live",
        flights_stats=flights_stats,
        flight_dist_space_stats=flight_dist_space_stats,
        flags=flags,
        flights_routes=get_flights_routes_for_map(flights_df),
        flights_visited_countries=get_flights_visited_countries_for_map(flights_df),
        mapbox_api_key=MAPBOX_API_KEY,
        flights_per_year_graph=graphs["year"],
        flights_per_month_graph=graphs["month"],
        flights_per_dayofweek_graph=graphs["dayofweek"],
        flights_data=_flights_table_records(flights_df),
        gsheets_url=google_sheets_document_url(),
    )


@app.route("/gps", methods=["GET", "POST"])
def gps():
    """Render GPS map page for a configurable date range; defaults to last 30 days on first load."""
    default_location = DEFAULT_MAP_BOX
    now = datetime.now(timezone.utc)
    default_start = (now - timedelta(days=GPS_DEFAULT_DAYS_BACK)).strftime(DATE_FMT)
    default_end = now.strftime(DATE_FMT)

    start_date = request.form.get("start_date", default_start)
    end_date = request.form.get("end_date", default_end)
    start_dt = datetime.strptime(start_date, DATE_FMT).replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end_date, DATE_FMT).replace(
        hour=23, minute=59, second=59, microsecond=999999, tzinfo=timezone.utc
    )
    day_count = (datetime.strptime(end_date, DATE_FMT) - datetime.strptime(start_date, DATE_FMT)).days + 1

    # Short ranges animate a comet-trace in-page (deck.gl); longer ranges render a static map.
    animated = False
    day_paths: list[list[list[float]]] | None = None
    track_points = 0
    trips_count = 0
    file_size_mb = 0.0

    if day_count <= GPS_ANIMATE_MAX_DAYS:
        trip_points = get_trip_points_for_date_range(start_dt, end_dt)
        if trip_points:
            animated = True
            day_paths = trip_points
            track_points = sum(len(p) for p in trip_points)
            trips_count = len(trip_points)

    if not animated:
        paths, stats = get_trips_for_date_range(start_dt, end_dt)
        if paths is not None:
            render_trips_to_file(paths, Path(GPS_MAP_FILENAME), default_location)
        map_path = Path(GPS_MAP_FILENAME)
        file_size_mb = map_path.stat().st_size / BYTES_PER_MB if map_path.exists() else 0.0
        track_points = stats.track_points
        trips_count = stats.trips_count

    logger.debug("[/gps] done start_date=%s end_date=%s animated=%s", start_date, end_date, animated)
    return render_template(
        "gps.html",
        start_date=start_date,
        end_date=end_date,
        map_filename=GPS_MAP_FILENAME,
        animated=animated,
        day_paths=day_paths,
        mapbox_api_key=MAPBOX_API_KEY,
        day_count=day_count,
        track_points=track_points,
        trips_count=trips_count,
        file_size_mb=file_size_mb,
    )


HEALTH_API_URL = "http://localhost:5009/api/health-data"

# How each HealthKit metric is aggregated to a daily value for the chart.
_HEALTH_METRIC_AGG: dict[str, str] = {
    "step_count": "sum",
    "distance": "sum",
    "active_energy": "sum",
    "flights_climbed": "sum",
}

_HEALTH_METRIC_UNIT: dict[str, str] = {
    "step_count": "steps",
    "distance": "km",
    "active_energy": "kcal",
    "flights_climbed": "floors",
}


@app.route("/health")
def health():
    """Render HealthKit dashboard page."""
    return render_template("health.html")


@app.route("/health/meta")
def health_meta_api():
    """Return available devices and date range for filter controls."""
    return jsonify(get_health_meta(HEALTH_DB_FILE))


@app.route("/health/data")
def health_data_api():
    """Return daily-aggregated chart data and raw samples for the active filters."""
    metric = request.args.get("metric", "heart_rate")
    date_from = request.args.get("from") or None
    date_to = request.args.get("to") or None
    devices = request.args.getlist("device") or None

    valid_tables = {t.table_name for t in HealthKitExportType}
    if metric not in valid_tables:
        return jsonify({"error": "invalid metric"}), 400

    export_type = next(t for t in HealthKitExportType if t.table_name == metric)
    df = load_metric_df(export_type, HEALTH_DB_FILE, date_from, date_to, devices)

    unit = _HEALTH_METRIC_UNIT.get(metric, "")
    if df.empty:
        return jsonify({"chart": [], "stats": {}, "samples": [], "unit": unit})

    if metric == "distance":
        df["value"] = df["value"] / 1000  # meters → km

    # Deduplicate to dominant hardware per day — applied before chart, stats, and table
    # so all three views are always consistent with each other.
    df["day"] = pd.to_datetime(df["start"], utc=True).dt.date.astype(str)
    df = filter_dominant_hardware_rows(df)

    agg_fn = _HEALTH_METRIC_AGG.get(metric, "mean")

    daily = df.groupby("day")["value"].agg(agg_fn).reset_index()
    daily.columns = ["date", "value"]
    daily["value"] = daily["value"].round(2)

    # Stats are derived from daily aggregates so they reflect the same granularity
    # the user sees in the chart (e.g. "average daily steps", not "average HK interval").
    stats: dict = {
        "count": int(len(df)),
        "mean": round(float(daily["value"].mean()), 2),
        "min": round(float(daily["value"].min()), 2),
        "max": round(float(daily["value"].max()), 2),
        "unit": unit,
        "agg": agg_fn,
    }
    if agg_fn == "sum":
        stats["total"] = round(float(daily["value"].sum()), 2)

    daily_by_device = df.groupby(["day", "device_hardware_version"], as_index=False).agg(
        value=("value", agg_fn), samples=("value", "count")
    )
    daily_by_device["value"] = daily_by_device["value"].round(2)
    samples = daily_by_device.sort_values("day", ascending=False).to_dict(orient="records")

    return jsonify(
        {
            "chart": daily.to_dict(orient="records"),
            "stats": stats,
            "samples": samples,
            "unit": unit,
        }
    )


@app.route("/health/table")
def health_table_api():
    """Return one row per day with columns for every metric.

    Each metric independently applies the dominant-hardware filter before
    aggregating to a daily value, so each column is self-consistent.
    """
    date_from = request.args.get("from") or None
    date_to = request.args.get("to") or None

    frames: dict[str, pd.DataFrame] = {}
    for export_type in HealthKitExportType:
        metric = export_type.table_name
        df = load_metric_df(export_type, HEALTH_DB_FILE, date_from, date_to, devices=None)
        if df.empty:
            continue
        if metric == "distance":
            df["value"] = df["value"] / 1000
        df["day"] = pd.to_datetime(df["start"], utc=True).dt.date.astype(str)
        df = filter_dominant_hardware_rows(df)
        agg_fn = _HEALTH_METRIC_AGG[metric]
        daily = (
            df.groupby("day", as_index=False).agg(value=("value", agg_fn)).rename(columns={"value": metric})
        )
        daily[metric] = daily[metric].round(2)
        frames[metric] = daily

    if not frames:
        return jsonify([])

    result = next(iter(frames.values()))
    for frame in list(frames.values())[1:]:
        result = result.merge(frame, on="day", how="outer")

    result = result.sort_values("day", ascending=False)
    return jsonify(result.fillna("").to_dict(orient="records"))


@app.route("/health/summary")
def health_summary_api():
    """Return deduplicated totals/averages for all metrics in a date window.

    Used by the chart selection panel. Query params: from, to (ISO date strings).
    """
    date_from = request.args.get("from") or None
    date_to = request.args.get("to") or None

    result: dict[str, dict] = {}
    for export_type in HealthKitExportType:
        metric = export_type.table_name
        df = load_metric_df(export_type, HEALTH_DB_FILE, date_from, date_to, devices=None)
        if df.empty:
            result[metric] = None
            continue

        if metric == "distance":
            df["value"] = df["value"] / 1000

        df["day"] = pd.to_datetime(df["start"], utc=True).dt.date.astype(str)
        df = filter_dominant_hardware_rows(df)

        agg_fn = _HEALTH_METRIC_AGG.get(metric, "mean")
        unit = _HEALTH_METRIC_UNIT.get(metric, "")
        agg_value = float(df["value"].sum() if agg_fn == "sum" else df["value"].mean())
        result[metric] = {
            "value": round(agg_value, 1),
            "unit": unit,
            "agg": agg_fn,
            "days": int(df["day"].nunique()),
        }

    return jsonify({"from": date_from, "to": date_to, "metrics": result})


@app.route("/live/health")
def live_health():
    """Proxy today's health stats from the iOS health API."""
    try:
        r = requests.get(f"{HEALTH_API_URL}?date=today", timeout=3)
        return jsonify(r.json())
    except Exception:
        return jsonify({"data": []}), 200


@app.route("/internal/heartbeat", methods=["POST"])
def internal_heartbeat():
    """Receive heartbeat forwarded from data_api; updates in-memory last-seen time."""
    global _last_heartbeat_time
    _last_heartbeat_time = datetime.now(timezone.utc)
    return jsonify({"status": "ok"}), 200


def _staleness_color(timestamp: datetime) -> str:
    """Return CSS color class based on how old the GPS fix is."""
    age_minutes = (datetime.now(timezone.utc) - timestamp).total_seconds() / 60
    if age_minutes <= LIVE_STALENESS_GREEN_MINUTES:
        return "live-fresh"
    if age_minutes <= LIVE_STALENESS_YELLOW_MINUTES:
        return "live-stale"
    return "live-old"


@app.route("/live")
def live():
    """Render live location page showing the most recent GPS point on an animated map."""
    snapshot = get_latest_location_snapshot()
    if snapshot is None:
        return render_template("live.html", no_data=True, mapbox_api_key=MAPBOX_API_KEY)
    return render_template(
        "live.html",
        no_data=False,
        lat=snapshot.lat,
        lon=snapshot.lon,
        last_updated_iso=snapshot.timestamp.isoformat(),
        staleness_color=_staleness_color(snapshot.timestamp),
        staleness_green_ms=LIVE_STALENESS_GREEN_MINUTES * 60 * 1000,
        staleness_yellow_ms=LIVE_STALENESS_YELLOW_MINUTES * 60 * 1000,
        day_paths=snapshot.day_paths,
        mapbox_api_key=MAPBOX_API_KEY,
        last_heartbeat_iso=_last_heartbeat_time.isoformat() if _last_heartbeat_time else None,
    )


@app.route("/live/current")
def live_current():
    """Return current GPS location as JSON for polling updates."""
    snapshot = get_latest_location_snapshot()
    if snapshot is None:
        return jsonify({"no_data": True})
    return jsonify(
        {
            "lat": snapshot.lat,
            "lon": snapshot.lon,
            "last_updated_iso": snapshot.timestamp.isoformat(),
            "staleness_color": _staleness_color(snapshot.timestamp),
            "day_paths": snapshot.day_paths,
            "last_heartbeat_iso": _last_heartbeat_time.isoformat() if _last_heartbeat_time else None,
        }
    )


@app.route("/passport", methods=["GET"])
def passport():
    """Render passport page with visited countries map and coverage stats."""
    countries_df = get_countries_df()
    visited_df = get_countries_visited(countries_df)
    visited_stats = get_visited_stats(visited_df, countries_df)
    flags_data = visited_df.drop_duplicates(subset=["name"], keep="first")[["flag", "name"]].to_dict(
        "records"
    )
    visited_df_to_deck_map(visited_df)
    return render_template(
        "passport.html",
        modified_date=get_age_of_map_update(VISITED_MAP_FILENAME),
        visited_stats=visited_stats,
        flags_data=flags_data,
        map_filename=VISITED_MAP_FILENAME,
        gsheets_url=google_sheets_document_url(),
    )


def main():
    configure_logging()
    logger.info(f"http://localhost:{DASHBOARD_PORT}")
    app.run(host="0.0.0.0", port=DASHBOARD_PORT, debug=True)


if __name__ == "__main__":
    main()
