import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import Flask, render_template, request, send_from_directory

from incognita.config import FLIGHTS_MAP_FILENAME, GPS_MAP_FILENAME, VISITED_MAP_FILENAME
from incognita.countries import (
    get_countries_df,
    get_countries_visited,
    get_visited_stats,
    visited_df_to_deck_map,
)
from incognita.flights import (
    flights_df_to_deck_map,
    flights_df_to_graph,
    get_countries,
    get_flight_aggregations,
    get_flight_dist_space_stats,
    get_flights_df,
    get_flights_stats,
)
from incognita.gps_trips_renderer import get_trips_for_date_range, render_trips_to_file
from incognita.utils import BYTES_PER_MB, DEFAULT_MAP_BOX, google_sheets_view_url

GPS_DEFAULT_DAYS_BACK = 30
DATE_FMT = "%Y-%m-%d"

_base_dir = Path(__file__).parent.parent
app = Flask(
    __name__,
    static_folder=str(_base_dir / "static"),
    template_folder=str(_base_dir / "templates"),
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("werkzeug").setLevel(logging.WARNING)


def get_age_of_map_update(filename: str) -> str:
    """Return last-modified date of file as dd.mm.yyyy HH:MM, or 'N/A' if missing."""
    path = Path(filename)
    if not path.exists():
        logger.info("%s not found", filename)
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


@app.route("/flights")
def flights():
    """Render flights page: stats, map, plots, and table."""
    flights_df = get_flights_df()
    flights_stats = get_flights_stats(flights_df)
    flight_dist_space_stats = get_flight_dist_space_stats(flights_df["Distance km"].sum())
    airport_countries_visited = get_countries(flights_df)
    flags = " ".join(c.flag for c in airport_countries_visited)

    flights_df_to_deck_map(flights_df)
    map_update_date = get_age_of_map_update(FLIGHTS_MAP_FILENAME)
    logger.info("Updated flights map at: %s", map_update_date)

    aggregations = get_flight_aggregations(flights_df)
    graphs = {
        "year": flights_df_to_graph(flights_df, "year", aggregations),
        "month": flights_df_to_graph(flights_df, "month", aggregations),
        "dayofweek": flights_df_to_graph(flights_df, "dayofweek", aggregations),
    }
    table_df = flights_df.copy()
    table_df["Date"] = table_df["Date"].dt.strftime("%Y.%m.%d")
    table_df.columns = [c.lower().replace(" ", "_").replace("#", "") for c in table_df.columns]

    return render_template(
        "flights.html",
        modified_date=map_update_date,
        flights_stats=flights_stats,
        flight_dist_space_stats=flight_dist_space_stats,
        flags=flags,
        flights_map_filename=FLIGHTS_MAP_FILENAME,
        flights_per_year_graph=graphs["year"],
        flights_per_month_graph=graphs["month"],
        flights_per_dayofweek_graph=graphs["dayofweek"],
        flights_data=table_df.to_dict(orient="records"),
        gsheets_url=google_sheets_view_url(),
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
    paths, stats = get_trips_for_date_range(start_dt, end_dt)
    if paths is not None:
        render_trips_to_file(paths, Path(GPS_MAP_FILENAME), default_location)
    map_path = Path(GPS_MAP_FILENAME)
    file_size_mb = map_path.stat().st_size / BYTES_PER_MB if map_path.exists() else 0.0
    day_count = (datetime.strptime(end_date, DATE_FMT) - datetime.strptime(start_date, DATE_FMT)).days + 1
    logger.info(f"[/gps]: {'-'*10} done {start_date=} {end_date=} {'-'*10}")
    return render_template(
        "gps.html",
        start_date=start_date,
        end_date=end_date,
        map_filename=GPS_MAP_FILENAME,
        day_count=day_count,
        track_points=stats.track_points,
        trips_count=stats.trips_count,
        file_size_mb=file_size_mb,
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
        gsheets_url=google_sheets_view_url("countries"),
    )


def main():
    app.run(host="0.0.0.0", port=5004, debug=True)


if __name__ == "__main__":
    main()
