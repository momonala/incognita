import dash
from dash import dcc
from dash import html

from incognita.processing import split_into_trips, get_processed_gdf, get_raw_gdf
from incognita.view import generate_folium
import pandas as pd
app = dash.Dash(__name__)

gdf = get_processed_gdf(get_raw_gdf())


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
    gdf_filtered = gdf[(pd.to_datetime(gdf.timestamp) >= start_date) & (pd.to_datetime(gdf.timestamp) <= end_date)]

    trips = split_into_trips(gdf_filtered)

    points = gdf_filtered if checklist_values else None
    folium_map = generate_folium(trips, points)
    return folium_map._repr_html_()


app.layout = html.Div(
    [
        html.H1("Map"),
        dcc.DatePickerRange(
            id='date_range_picker',
            minimum_nights=0,
            start_date=gdf.timestamp.iloc[0],
            end_date=gdf.timestamp.iloc[-1],
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
            srcDoc=generate_folium(split_into_trips(gdf))._repr_html_(),
            width="100%",
            height="1000",
        ),
    ]
)


if __name__ == '__main__':
    app.run_server(debug=True)
