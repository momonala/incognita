import logging

import dash
import pandas as pd
from dash import dcc
from dash import html

from incognita.database import get_gdf_from_db, get_start_end_date
from incognita.processing import get_stationary_groups, convert_pd_to_gpd
from incognita.processing import split_into_trips, add_speed_to_gdf
from incognita.utils import get_ip_address
from incognita.view import generate_folium

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

PORT = 8384

app = dash.Dash(__name__, meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}])


@app.callback(
    dash.dependencies.Output('folium_map', 'srcDoc'),
    [
        dash.dependencies.Input('date_range_picker', 'start_date'),
        dash.dependencies.Input('date_range_picker', 'end_date'),
        dash.dependencies.Input('checklist', 'value'),
    ],
)
def generate_folium_map(start_date, end_date, checklist_values):
    """Filter GDF based on timestamps provided. Returns HTML repr of Folium map for browser rendering."""
    start_date = pd.to_datetime(start_date, utc=True).replace(hour=0, minute=0)
    end_date = pd.to_datetime(end_date, utc=True).replace(hour=23, minute=59)

    gdf = add_speed_to_gdf(convert_pd_to_gpd(get_gdf_from_db()))
    gdf["timestamp"] = pd.to_datetime(gdf["timestamp"])
    gdf_filtered = gdf[(gdf["timestamp"] >= start_date) & (gdf["timestamp"] <= end_date)]

    trips = split_into_trips(gdf_filtered)

    stationary_points = get_stationary_groups(gdf)
    all_points = gdf_filtered if checklist_values else None
    folium_map = generate_folium(trips, stationary_points, all_points)
    return folium_map._repr_html_()


gdf = add_speed_to_gdf(convert_pd_to_gpd(get_gdf_from_db()))
start_date_base, end_date_base = get_start_end_date()
app.layout = html.Div(
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
            options=[{'label': 'Show Points', 'value': 'True'}],
            value=[],
            labelStyle={'display': 'inline-block'},
            id="checklist",
        ),
        html.Iframe(
            id="folium_map",
            srcDoc=generate_folium(
                trips=split_into_trips(gdf), stationary_points=get_stationary_groups(gdf)
            )._repr_html_(),
            width="100%",
            height="1000",
        ),
    ],
)


if __name__ == '__main__':
    logger.info(f"Visit: http://{get_ip_address()}:{PORT}")
    app.run_server(debug=True, port=PORT, host='0.0.0.0')
