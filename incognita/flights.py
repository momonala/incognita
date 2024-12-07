import calendar

import airportsdata
import pandas as pd
import plotly.graph_objs as go
import pycountry
import pydeck
import pydeck as pdk

from incognita.processing import get_haversine_dist
from incognita.utils import coordinates_from_place_name, df_from_gsheets
from incognita.values import MAPBOX_API_KEY, GOOGLE_MAPS_API_KEY, flights_map_filename
from incognita.countries import get_countries_df

airport_db = airportsdata.load('IATA')


def _city_to_coord(city: str) -> tuple[float, float]:
    geo_box = coordinates_from_place_name(city)
    return round(geo_box.center.lon, 3), round(geo_box.center.lat, 3)


def _airport_to_coord(airport_iata_code: str) -> tuple[float, float]:
    airport = airport_db[airport_iata_code]
    return round(airport["lon"], 3), round(airport["lat"], 3)


def airport_iata_to_coords_departure(flight_row) -> tuple[float, float]:
    try:
        coords = _airport_to_coord(flight_row["departure_airport"])
    except KeyError:
        coords = _city_to_coord(flight_row["Origin"])
    return coords


def _airport_iata_to_coords_arrival(flight_row) -> tuple[float, float]:
    try:
        coords = _airport_to_coord(flight_row["arrival_airport"])
    except KeyError:
        coords = _city_to_coord(flight_row["Destination"])
    return coords


def _distance_between_airports_km(flight_row) -> float:
    lon_orig, lat_orig = flight_row["orig_coords"]
    lon_dest, lat_dest = flight_row["dest_coords"]
    dist_meters = get_haversine_dist(lat_orig, lon_orig, lat_dest, lon_dest)
    return round(dist_meters / 1000, 2)


def get_flights_df():
    flights_df = df_from_gsheets()
    flights_df = flights_df[~flights_df["Origin"].isnull()]
    flights_df = flights_df[
        [
            "Date",
            "Origin",
            "Destination",
            "Flight #",
            "departure_airport",
            "arrival_airport",
            "aircraft",
            "call_sign",
        ]
    ]
    flights_df["departure_airport"] = flights_df["departure_airport"].apply(lambda s: s.strip())
    flights_df["arrival_airport"] = flights_df["arrival_airport"].apply(lambda s: s.strip())
    flights_df["orig_coords"] = flights_df.apply(airport_iata_to_coords_departure, axis=1)
    flights_df["dest_coords"] = flights_df.apply(_airport_iata_to_coords_arrival, axis=1)
    flights_df["Distance km"] = flights_df.apply(_distance_between_airports_km, axis=1)
    flights_df["Date"] = pd.to_datetime(flights_df['Date'], format='mixed')
    flights_df.sort_values(by="Date", inplace=True)
    return flights_df


def get_countries(flights_df: pd.DataFrame) -> list:
    def country_from_airport(airport: str) -> str | None:
        if str(airport) == "nan" or not airport:
            return
        if airport == "SXF":
            return "DE"
        return airport_db[airport]["country"]

    def _country(country_code: str):
        return pycountry.countries.get(alpha_2=country_code)

    flights_df["country_arrival"] = flights_df["arrival_airport"].apply(country_from_airport)
    flights_df["country_departure"] = flights_df["departure_airport"].apply(country_from_airport)
    all_countries = pd.concat([flights_df["country_arrival"], flights_df["country_departure"]])
    country_value_counts = all_countries.value_counts()
    return list(map(lambda cc: _country(cc), country_value_counts.index))


def get_flights_stats(flights_df: pd.DataFrame) -> dict:
    def get_airline(flight_num: str | None) -> str | None:
        if not isinstance(flight_num, str):
            return
        return flight_num.split(" ")[0]

    airlines_value_counts = flights_df["Flight #"].apply(get_airline).value_counts()
    flights_df["route"] = flights_df["departure_airport"] + "-" + flights_df["arrival_airport"]
    routes_value_counts = flights_df["route"].value_counts()
    airport_value_counts = pd.concat(
        [flights_df['departure_airport'], flights_df['arrival_airport']]
    ).value_counts()
    flight_distance = round(flights_df["Distance km"].sum())
    countires = get_countries(flights_df)

    return {
        "Flights": flights_df.count()["Date"],
        "Flight Distance": flight_distance,
        "Airports": len(airport_value_counts),
        "Airlines": len(airlines_value_counts),
        "Routes": len(routes_value_counts),
        "Countries": len(countires),
    }


def get_flight_dist_space_stats(flight_distance: float) -> dict:
    distance_around_earth = 40_075
    distance_to_moon = 385_000
    distance_to_mars = 54_600_000

    dist_around_earth = round(flight_distance / distance_around_earth, 1)
    dist_to_moon = round(flight_distance / distance_to_moon, 1)
    dist_to_mars = round(flight_distance / distance_to_mars, 3)

    f"""
    {dist_around_earth}x around the earth
    {dist_to_moon}x to the moon
    {dist_to_mars}x to the mars
    """
    return {
        "earth": dist_around_earth,
        "moon": dist_to_moon,
        "mars": dist_to_mars,
    }


def flights_df_to_graph(flights_df: pd.DataFrame, agg_by: str):
    assert agg_by in ["year", "month", "dayofweek"], f"{agg_by=} not allowed"
    flights_per_year = flights_df.groupby(flights_df['Date'].dt.year).size()
    flights_per_month = flights_df.groupby(flights_df['Date'].dt.month).size()
    flights_per_dayofweek = flights_df.groupby(flights_df['Date'].dt.dayofweek).size()

    agg_by_mapping = {
        "year": {
            "data": flights_per_year,
            "title": "Flights per Year",
            "x_title": "Year",
            "x_labels": flights_per_year.index.astype(str),
        },
        "month": {
            "data": flights_per_month,
            "title": "Flights per Month",
            "x_title": "Month",
            "x_labels": list(calendar.month_name)[1:],
        },
        "dayofweek": {
            "data": flights_per_dayofweek,
            "title": "Flights per Day of Week",
            "x_title": "Day of Week",
            "x_labels": list(calendar.day_name),
        },
    }
    data = agg_by_mapping[agg_by]["data"]
    title = agg_by_mapping[agg_by]["title"]
    x_title = agg_by_mapping[agg_by]["x_title"]
    x_labels = agg_by_mapping[agg_by]["x_labels"]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=x_labels,  # Use x_labels instead of data.index
            y=data.values,
            mode='lines+markers',
            marker=dict(color='royalblue', size=5),
            line=dict(color='royalblue', width=2, shape='spline'),
        )
    )

    # Customize layout
    fig.update_layout(
        title=title,
        xaxis=dict(
            title=x_title,
            tickmode='array',
            tickvals=list(range(len(x_labels))),
            ticktext=x_labels,
            tickangle=-45,
        ),
        yaxis=dict(title="Number of Flights"),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='black'),
        margin=dict(l=40, r=40, t=80, b=40),
        hovermode='x',
        shapes=[
            {
                'type': 'line',
                'xref': 'paper',
                'yref': 'y',
                'x0': 0,
                'x1': 1,
                'y0': i,
                'y1': i,
                'line': dict(color='gray', width=1),
            }
            for i in range(5, max(data.values), 5)
        ],
    )

    return fig.to_html(full_html=False)


def _tooltip(row):
    return f"""
    <div>Date: {row["date_vis"]}</div>
    <div>Origin: {row["Origin"]}</div>
    <div>Destination: {row["Destination"]}</div>
    <div>Flight #: {row["Flight #"]}</div>
    <div>Distance km: {row["Distance km"]}</div>
    """


def flights_df_to_deck_map(flights_df: pd.DataFrame) -> pydeck.Deck:
    # Define layers to display on a map
    flights_df['date_vis'] = flights_df['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    flights_df['tooltip'] = flights_df.apply(_tooltip, axis=1)
    flights_layer = pdk.Layer(
        "GreatCircleLayer",
        flights_df,
        pickable=True,
        get_stroke_width=20,
        get_source_position="orig_coords",
        get_target_position="dest_coords",
        get_target_color=[255, 0, 255],
        get_source_color=[0, 255, 255],
        auto_highlight=True,
    )
    countrties_layer = get_airport_countries_visited_deck_layer()

    # Set the viewport location
    view_state = pdk.ViewState(latitude=30, longitude=0, zoom=1, bearing=0, pitch=0)

    # Render
    tooltip = {
        "html": """
    <div style='font-family: "Helvetica Neue",Helvetica,Arial,sans-serif; font-weight: 200;'>
        {tooltip}
    </div>
        """,
        "style": {
            "backgroundColor": "rgba(25, 25, 25, 0.9)",
            "color": "white",
            "border": "1px solid black",
        },
    }
    r = pdk.Deck(
        layers=[countrties_layer, flights_layer],
        initial_view_state=view_state,
        api_keys={"mapbox": MAPBOX_API_KEY, "google_maps": GOOGLE_MAPS_API_KEY},
        map_provider="mapbox",
        map_style="dark",  # ‘light’, ‘dark’, ‘road’, ‘satellite’,
        tooltip=tooltip,
    )
    r.picking_radius = 10
    r.to_html(flights_map_filename)
    return r


def get_airport_countries_visited_deck_layer():
    flights_df = get_flights_df()
    countries_df = get_countries_df()
    airport_countries_visited = get_countries(flights_df)
    countries_visited_alpha_3 = [x.alpha_3 for x in airport_countries_visited]
    airport_countries_visited_df = countries_df[countries_df.alpha_3.isin(countries_visited_alpha_3)]

    airport_countries_visited_df["tooltip"] = airport_countries_visited_df.apply(
        lambda x: f"<div>{x['flag']} {x['name']}</div>", axis=1
    )
    return pdk.Layer(
        "PolygonLayer",
        data=airport_countries_visited_df,
        get_polygon="geometry.coordinates",
        get_line_color=[0, 255, 0],
        get_fill_color=[0, 255, 0],
        lineWidthMinPixels=1,
        opacity=0.2,
        filled=True,
        pickable=True,
    )


if __name__ == "__main__":
    df = get_flights_df()
    flights_df_to_deck_map(df)
