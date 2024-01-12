import logging
from datetime import datetime, timedelta

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import dcc, html
from joblib import Memory

from incognita.data_models import GeoBoundingBox
from incognita.database import get_gdf_from_db, get_start_end_date
from incognita.processing import get_stationary_groups
from incognita.processing import split_into_trips, add_speed_to_gdf
from incognita.utils import get_ip_address, coordinates_from_place_name, timed
from incognita.view import get_map_deck

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.propagate = False  # don't send  logs to root handler (causes duplicates)

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.YETI, 'https://codepen.io/chriddyp/pen/bWLwgP.css'],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
disk_memory = Memory("joblib_cache")

PORT = 8384
DEFAULT_LOCATION = "Berlin, De"


@app.callback(
    dash.dependencies.Output('folium_map', 'srcDoc'),
    [
        dash.dependencies.Input('date_range_picker', 'start_date'),
        dash.dependencies.Input('date_range_picker', 'end_date'),
        dash.dependencies.Input('checklist', 'value'),
    ],
)
def generate_map_callback(start_date, end_date, show_flights):
    """Filter df based on timestamps provided. Returns HTML repr of map for browser rendering."""
    bbox = coordinates_from_place_name(DEFAULT_LOCATION)
    return get_deck_map_html(start_date, end_date, bbox, bool(show_flights))


@timed
@disk_memory.cache
def get_deck_map_html(start_date: str, end_date: str, bbox: GeoBoundingBox, show_flights: bool) -> str:
    df, stationary_groups, trips_df = get_data_for_maps(start_date, end_date, show_flights)
    deck = get_map_deck(bbox, trips_df, stationary_groups, df)
    return deck.to_html(as_string=True)


@timed
def get_data_for_maps(start_date: str, end_date: str, show_flights: bool):
    start_date = pd.to_datetime(start_date, utc=True).replace(hour=0, minute=0)
    end_date = pd.to_datetime(end_date, utc=True).replace(hour=23, minute=59)

    gdf = add_speed_to_gdf(get_gdf_from_db())
    logger.info(f"{gdf.shape=}")
    logger.info(gdf.tail(1))

    gdf["timestamp"] = pd.to_datetime(gdf["timestamp"])
    gdf_filtered = gdf[(gdf["timestamp"] >= start_date) & (gdf["timestamp"] <= end_date)]
    max_dist = 100 if not show_flights else 400
    trips = split_into_trips(gdf_filtered, max_dist)
    stationary_points = get_stationary_groups(gdf_filtered)
    return gdf_filtered, stationary_points, trips


start_date_base, end_date_base = tuple(x.split("T")[0] for x in get_start_end_date())
# start_date_base, end_date_base = "2023-09-15" ,  "2023-10-14" # small range for debugging
date_fmt = '%Y-%m-%d'
lookback = timedelta(days=21)
start_date_base = (datetime.strptime(end_date_base, date_fmt) - lookback).strftime(date_fmt)  # noqa

default_location = coordinates_from_place_name(DEFAULT_LOCATION)
map_html = get_deck_map_html(start_date_base, end_date_base, default_location, False)

app.layout = html.Div(
    [
        html.Div(
            [
                html.H1("Incognita", style={'display': 'inline-block', "width": "15%"}),
                dcc.DatePickerRange(
                    id='date_range_picker',
                    minimum_nights=0,
                    start_date=start_date_base,
                    end_date=end_date_base,
                    display_format='DD.MM.YYYY',
                    style={"width": "20%"},
                ),
                dcc.Checklist(
                    options=[{'label': 'Show Flights', 'value': 'False'}],
                    value=[],
                    # labelStyle={'display': 'inline-block'},
                    id="checklist",
                ),
            ],
            style={"height": "50px", "margin-left": "10px", "margin-bottom": "10px"},
        ),
        html.Iframe(
            id="folium_map", srcDoc=map_html, width="100%", height="1000", style={"margin-top": "5px"}
        ),
    ],
)


if __name__ == '__main__':
    logger.info(f"Visit: http://{get_ip_address()}:{PORT}")
    app.run_server(debug=True, port=PORT, host='0.0.0.0')
