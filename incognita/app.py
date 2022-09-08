import logging
from functools import lru_cache

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import dcc
from dash import html
from joblib import Memory

from incognita.database import get_gdf_from_db, get_start_end_date
from incognita.processing import get_stationary_groups
from incognita.processing import split_into_trips, add_speed_to_gdf
from incognita.utils import get_ip_address
from incognita.view import generate_folium

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

PORT = 8384

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.YETI, 'https://codepen.io/chriddyp/pen/bWLwgP.css'],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
disk_memory = Memory("joblib_cache")


@lru_cache()
def get_raw_gdf_with_speed():
    return add_speed_to_gdf(get_gdf_from_db())


@app.callback(
    dash.dependencies.Output('folium_map', 'srcDoc'),
    [
        dash.dependencies.Input('date_range_picker', 'start_date'),
        dash.dependencies.Input('date_range_picker', 'end_date'),
        dash.dependencies.Input('checklist', 'value'),
    ],
)
def _generate_folium_map(start_date, end_date, show_flights):
    """Filter GDF based on timestamps provided. Returns HTML repr of Folium map for browser rendering."""
    folium_map_html = get_folium_map_html(start_date, end_date, bool(show_flights))
    return folium_map_html


# @disk_memory.cache
def get_folium_map_html(start_date: str, end_date: str, show_flights: bool) -> str:
    start_date = pd.to_datetime(start_date, utc=True).replace(hour=0, minute=0)
    end_date = pd.to_datetime(end_date, utc=True).replace(hour=23, minute=59)

    gdf = get_raw_gdf_with_speed()
    logger.info(f"{gdf.shape=}")
    logger.info(gdf.tail(1))

    gdf["timestamp"] = pd.to_datetime(gdf["timestamp"])
    gdf_filtered = gdf[(gdf["timestamp"] >= start_date) & (gdf["timestamp"] <= end_date)]

    max_dist = 100 if not show_flights else 400
    trips = split_into_trips(gdf_filtered, max_dist)
    stationary_points = get_stationary_groups(gdf_filtered)

    folium_map_html = generate_folium(trips, stationary_points)._repr_html_()
    logger.info("generated folium map")
    return folium_map_html


# start_date_base, end_date_base = tuple(x.split("T")[0] for x in get_start_end_date())
start_date_base, end_date_base = '2022-09-01', '2022-09-05'
map_html = get_folium_map_html(start_date_base, end_date_base, False)

app.layout = html.Div(
    [
        html.Div(
            [
                html.H1("Incognita"),
                dcc.DatePickerRange(
                    id='date_range_picker',
                    minimum_nights=0,
                    start_date=start_date_base,
                    end_date=end_date_base,
                    display_format='DD.MM.YYYY',
                ),
                dcc.Checklist(
                    options=[{'label': 'Show Flights', 'value': 'False'}],
                    value=[],
                    # labelStyle={'display': 'inline-block'},
                    id="checklist",
                ),
            ],
            style={"height": "50px", "margin-left": "10px"},
        ),
        html.Iframe(id="folium_map", srcDoc=map_html, width="100%", height="1000"),
    ],
)


if __name__ == '__main__':
    logger.info(f"Visit: http://{get_ip_address()}:{PORT}")
    app.run_server(debug=False, port=PORT, host='0.0.0.0')
