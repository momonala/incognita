import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask, render_template, request, send_from_directory

from incognita.countries import get_countries_visited, get_visited_stats, visited_df_to_deck_map
from incognita.flights import (
    flights_df_to_deck_map,
    flights_df_to_graph,
    get_countries,
    get_flight_dist_space_stats,
    get_flights_df,
    get_flights_stats,
)
from incognita.gps import get_deck_map_html
from incognita.utils import coordinates_from_place_name, google_sheets_url
from incognita.values import flights_map_filename, gps_map_filename, visited_map_filename

_base_dir = Path(__file__).parent.parent
app = Flask(
    __name__,
    static_folder=str(_base_dir / "static"),
    template_folder=str(_base_dir / "templates"),
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("werkzeug").setLevel(logging.WARNING)
DEFAULT_LOCATION = "Berlin, De"


def get_age_of_map_update(filename: str) -> str:
    if os.path.exists(filename):
        modified_timestamp = os.path.getmtime(filename)
        modified_date = time.strftime("%d.%m.%Y %H:%M", time.localtime(modified_timestamp))
        return modified_date
    logger.info(f"{filename=} not found!")
    return "N/A"


@app.route("/favicon.ico")
def favicon():
    return send_from_directory(
        os.path.join(app.root_path, "static"), "favicon.ico", mimetype="image/vnd.microsoft.icon"
    )


@app.route("/")
def index():
    return render_template(
        "index.html",  # noqa
    )


@app.route("/flights")
def flights():
    flights_df = get_flights_df()
    # flights_df = flights_df[flights_df['Flight #'].notna()]
    flights_per_year_graph = flights_df_to_graph(flights_df, "year")
    flights_per_month_graph = flights_df_to_graph(flights_df, "month")
    flights_per_dayofweek_graph = flights_df_to_graph(flights_df, "dayofweek")
    flights_stats = get_flights_stats(flights_df)
    flight_dist_space_stats = get_flight_dist_space_stats(flights_df["Distance km"].sum())
    airport_countries_visited = get_countries(flights_df)
    flags = " ".join([x.flag for x in airport_countries_visited])

    flights_df_to_deck_map(flights_df)
    map_update_date = get_age_of_map_update(flights_map_filename)
    logger.info(f"Updated flights map at: {map_update_date}")

    flights_df["Date"] = flights_df["Date"].dt.strftime("%Y.%m.%d")
    flights_df.columns = [col.lower().replace(" ", "_").replace("#", "") for col in flights_df.columns]
    return render_template(
        "flights.html",  # noqa
        modified_date=map_update_date,
        flights_stats=flights_stats,
        flight_dist_space_stats=flight_dist_space_stats,
        flags=flags,
        flights_map_filename=flights_map_filename,
        flights_per_year_graph=flights_per_year_graph,
        flights_per_month_graph=flights_per_month_graph,
        flights_per_dayofweek_graph=flights_per_dayofweek_graph,
        flights_data=flights_df.to_dict(orient="records"),
        gsheets_url=google_sheets_url().split("gviz")[0],
    )


@app.route("/gps", methods=["GET", "POST"])
def gps():
    default_location = coordinates_from_place_name(DEFAULT_LOCATION)
    now = datetime.now()
    three_weeks_ago = now - timedelta(weeks=3)

    start_date = request.form.get("start_date", three_weeks_ago.strftime("%Y-%m-%d"))
    end_date = request.form.get("end_date", now.strftime("%Y-%m-%d"))
    get_deck_map_html(start_date, end_date, default_location)
    return render_template(
        "gps.html",  # noqa
        start_date=start_date,
        end_date=end_date,
        map_filename=gps_map_filename,
    )


@app.route("/passport", methods=["GET"])
def passport():
    visited_df = get_countries_visited()
    visited_stats = get_visited_stats(visited_df)
    flags = visited_df.flag.value_counts()
    flags = " ".join(flags.index)
    visited_df_to_deck_map(visited_df)
    return render_template(
        "passport.html",  # noqa
        modified_date=get_age_of_map_update(visited_map_filename),
        visited_stats=visited_stats,
        flags=flags,
        map_filename=visited_map_filename,
        gsheets_url=google_sheets_url("countries").split("gviz")[0],
    )


def main():
    app.run(host="0.0.0.0", port=5004, debug=True)


if __name__ == "__main__":
    main()
