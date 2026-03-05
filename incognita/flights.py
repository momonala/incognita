import calendar
from collections import defaultdict

import airportsdata
import pandas as pd
import plotly.graph_objs as go
import pycountry
import pydeck as pdk

from incognita.config import FLIGHTS_MAP_FILENAME
from incognita.countries import get_countries_df
from incognita.geo_distance import get_haversine_dist
from incognita.utils import (
    DEFAULT_MAP_BOX,
    google_sheets_export_csv_url,
    read_google_sheets_csv,
)
from incognita.values import GOOGLE_MAPS_API_KEY, MAPBOX_API_KEY

AIRPORT_DB = airportsdata.load("IATA")

COORD_DECIMALS = 3
SAMPLE_FLIGHTS_TOOLTIP_LIMIT = 6

# Airport IATA -> country alpha_2 when DB is wrong or missing
AIRPORT_COUNTRY_OVERRIDES: dict[str, str] = {"SXF": "DE", "REP": "KH"}

DISTANCE_EARTH_KM = 40_075
DISTANCE_MOON_KM = 385_000
DISTANCE_MARS_KM = 54_600_000
METERS_PER_KM = 1000
DISTANCE_KM_DECIMALS = 2

AGGREGATION_OPTIONS = ("year", "month", "dayofweek")

# Chart styling (Apple-inspired)
CHART_ACCENT_RGBA = "rgba(0, 113, 227, 0.9)"
CHART_FILL_RGBA = "rgba(0, 113, 227, 0.12)"
CHART_FONT_FAMILY = (
    "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif"
)
CHART_TITLE_COLOR = "#1d1d1f"
CHART_AXIS_COLOR = "#6e6e73"


def _city_to_coord(_city: str) -> tuple[float, float]:
    """(lon, lat) fallback when airport IATA is unknown; uses default map center (Berlin)."""
    return (
        round(DEFAULT_MAP_BOX.center.lon, COORD_DECIMALS),
        round(DEFAULT_MAP_BOX.center.lat, COORD_DECIMALS),
    )


def _airport_to_coord(airport_iata_code: str) -> tuple[float, float]:
    airport = AIRPORT_DB[airport_iata_code]
    return round(airport["lon"], COORD_DECIMALS), round(airport["lat"], COORD_DECIMALS)


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
    return round(dist_meters / METERS_PER_KM, DISTANCE_KM_DECIMALS)


def get_flights_df() -> pd.DataFrame:
    """Load flights from Google Sheets, add coords and distance, sort by date."""
    flights_df = read_google_sheets_csv(google_sheets_export_csv_url("raw"))
    print(flights_df.head())
    flights_df = flights_df[~flights_df["Origin"].isnull()]
    flights_df = flights_df[
        [
            "Date",
            "Origin",
            "Destination",
            "Flight #",
            "Departure Airport",
            "Arrival Airport",
            "aircraft",
        ]
    ]
    flights_df["departure_airport"] = flights_df["Departure Airport"].apply(lambda s: s.strip())
    flights_df["arrival_airport"] = flights_df["Arrival Airport"].apply(lambda s: s.strip())
    flights_df["orig_coords"] = flights_df.apply(airport_iata_to_coords_departure, axis=1)
    flights_df["dest_coords"] = flights_df.apply(_airport_iata_to_coords_arrival, axis=1)
    flights_df["Distance km"] = flights_df.apply(_distance_between_airports_km, axis=1)
    flights_df["Date"] = pd.to_datetime(flights_df["Date"], format="mixed")
    flights_df.sort_values(by="Date", inplace=True)
    return flights_df


def _airport_to_alpha_2(airport: str) -> str | None:
    """Country alpha_2 code for airport IATA, or None."""
    if pd.isna(airport) or not str(airport).strip():
        return None
    if airport in AIRPORT_COUNTRY_OVERRIDES:
        return AIRPORT_COUNTRY_OVERRIDES[airport]
    try:
        return AIRPORT_DB[airport]["country"]
    except KeyError:
        return None


def get_countries(flights_df: pd.DataFrame) -> list:
    """List of pycountry Country objects for countries visited by flight (arrival/departure), by frequency."""
    arr_codes = flights_df["arrival_airport"].apply(_airport_to_alpha_2)
    dep_codes = flights_df["departure_airport"].apply(_airport_to_alpha_2)
    by_frequency = pd.concat([arr_codes, dep_codes]).value_counts()
    return [pycountry.countries.get(alpha_2=code) for code in by_frequency.index]


def _build_alpha_3_to_flights(
    flights_df: pd.DataFrame, alpha_2_to_alpha_3: dict[str, str]
) -> dict[str, list[tuple[str, str]]]:
    """Map country alpha_3 to list of (date_str, route_str) for flights touching that country."""
    alpha_3_to_flights: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for _, row in flights_df.iterrows():
        dep_a2 = _airport_to_alpha_2(row["departure_airport"])
        arr_a2 = _airport_to_alpha_2(row["arrival_airport"])
        dep_a3 = alpha_2_to_alpha_3.get(dep_a2) if dep_a2 else None
        arr_a3 = alpha_2_to_alpha_3.get(arr_a2) if arr_a2 else None
        date_str = row["Date"].strftime("%Y-%m-%d")
        route_str = f"{row['departure_airport']} → {row['arrival_airport']}"
        for a3 in (dep_a3, arr_a3):
            if a3:
                alpha_3_to_flights[a3].append((date_str, route_str))
    return alpha_3_to_flights


def get_flights_visited_countries_for_map(flights_df: pd.DataFrame) -> list[dict]:
    """Countries visited by flight with geometry and tooltip: flight count and up to N sample dates/routes."""
    visited = get_countries(flights_df)
    alpha_3_list = [c.alpha_3 for c in visited]
    alpha_2_to_alpha_3 = {c.alpha_2: c.alpha_3 for c in visited}
    alpha_3_to_flights = _build_alpha_3_to_flights(flights_df, alpha_2_to_alpha_3)

    countries_df = get_countries_df()
    layer_df = countries_df[countries_df["alpha_3"].isin(alpha_3_list)].copy()
    result = []
    for _, row in layer_df.iterrows():
        a3 = row["alpha_3"]
        flights_list = alpha_3_to_flights.get(a3, [])
        sample = list(reversed(flights_list[-SAMPLE_FLIGHTS_TOOLTIP_LIMIT:]))
        result.append(
            {
                "name": row["name"],
                "flag": row["flag"],
                "alpha_3": a3,
                "geometry": row["geometry"],
                "flight_count": len(flights_list),
                "sample_flights": [f"{d} {r}" for d, r in sample],
            }
        )
    return result


def get_flights_stats(flights_df: pd.DataFrame) -> dict[str, int]:
    """Aggregate flight stats: count of flights, distance, airports, airlines, routes, countries."""

    def airline_from_flight_num(flight_num: str | None) -> str | None:
        if not isinstance(flight_num, str):
            return None
        return flight_num.split(" ")[0]

    airlines_count = flights_df["Flight #"].apply(airline_from_flight_num).value_counts()
    routes_count = (flights_df["departure_airport"] + "-" + flights_df["arrival_airport"]).value_counts()
    airports_count = pd.concat(
        [flights_df["departure_airport"], flights_df["arrival_airport"]]
    ).value_counts()
    return {
        "Flights": int(flights_df["Date"].count()),
        "Flight Distance": round(flights_df["Distance km"].sum()),
        "Airports": len(airports_count),
        "Airlines": len(airlines_count),
        "Routes": len(routes_count),
        "Countries": len(get_countries(flights_df)),
    }


def get_flight_dist_space_stats(flight_distance_km: float) -> dict[str, float]:
    """Compare total flight distance to Earth circumference, Moon distance, Mars distance (multiples)."""
    return {
        "earth": round(flight_distance_km / DISTANCE_EARTH_KM, 1),
        "moon": round(flight_distance_km / DISTANCE_MOON_KM, 1),
        "mars": round(flight_distance_km / DISTANCE_MARS_KM, 3),
    }


def get_flight_aggregations(flights_df: pd.DataFrame) -> dict[str, pd.Series]:
    """Compute year, month, dayofweek counts once for use by flights_df_to_graph."""
    return {
        "year": flights_df.groupby(flights_df["Date"].dt.year).size(),
        "month": flights_df.groupby(flights_df["Date"].dt.month).size(),
        "dayofweek": flights_df.groupby(flights_df["Date"].dt.dayofweek).size(),
    }


def flights_df_to_graph(
    flights_df: pd.DataFrame,
    agg_by: str,
    aggregations: dict[str, pd.Series] | None = None,
) -> str:
    """Plotly HTML for flights aggregated by year, month, or day of week."""
    if agg_by not in AGGREGATION_OPTIONS:
        raise ValueError(f"agg_by must be one of {AGGREGATION_OPTIONS}")
    if aggregations is None:
        aggregations = get_flight_aggregations(flights_df)
    flights_per_year = aggregations["year"]
    flights_per_month = aggregations["month"]
    flights_per_dayofweek = aggregations["dayofweek"]

    agg_config = {
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
    cfg = agg_config[agg_by]
    data = cfg["data"]
    x_labels = cfg["x_labels"]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=x_labels,
            y=data.values,
            mode="lines+markers",
            fill="tozeroy",
            marker=dict(
                color=CHART_ACCENT_RGBA,
                size=8,
                line=dict(width=0),
                symbol="circle",
            ),
            line=dict(color=CHART_ACCENT_RGBA, width=1.5, shape="spline"),
            fillcolor=CHART_FILL_RGBA,
            hoverinfo="x+y",
            hovertemplate="%{x}<br>%{y} flights<extra></extra>",
        )
    )

    fig.update_layout(
        title=dict(
            text=cfg["title"],
            font=dict(size=18, color=CHART_TITLE_COLOR, family=CHART_FONT_FAMILY),
            x=0,
            xanchor="left",
        ),
        xaxis=dict(
            title=dict(text=cfg["x_title"], font=dict(size=12, color=CHART_AXIS_COLOR)),
            tickmode="array",
            tickvals=list(range(len(x_labels))),
            ticktext=x_labels,
            tickangle=-45,
            showgrid=False,
            showline=False,
            zeroline=False,
            tickfont=dict(size=11, color=CHART_AXIS_COLOR, family=CHART_FONT_FAMILY),
        ),
        yaxis=dict(
            title=dict(text="Flights", font=dict(size=12, color=CHART_AXIS_COLOR)),
            showgrid=False,
            showline=False,
            zeroline=False,
            tickfont=dict(size=11, color=CHART_AXIS_COLOR, family=CHART_FONT_FAMILY),
        ),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family=CHART_FONT_FAMILY),
        margin=dict(l=44, r=24, t=52, b=64),
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor="rgba(255,255,255,0.95)",
            bordercolor="rgba(0,0,0,0.08)",
            font=dict(size=12, color=CHART_TITLE_COLOR, family=CHART_FONT_FAMILY),
        ),
        showlegend=False,
    )

    return fig.to_html(full_html=False)


def _flight_tooltip_row(row: pd.Series) -> str:
    """Build tooltip HTML for one flight row."""
    return f"""<div>Date: {row["date_vis"]}</div>
<div>Origin: {row["Origin"]}</div>
<div>Destination: {row["Destination"]}</div>
<div>Flight #: {row["Flight #"]}</div>
<div>Distance km: {row["Distance km"]}</div>"""


def get_flights_routes_for_map(flights_df: pd.DataFrame) -> list[dict]:
    """Return list of route dicts with orig_coords, dest_coords, and distance_km (for animation duration)."""
    return [
        {
            "orig_coords": list(row["orig_coords"]),
            "dest_coords": list(row["dest_coords"]),
            "distance_km": float(row["Distance km"]),
        }
        for _, row in flights_df.iterrows()
    ]


def flights_df_to_deck_map(flights_df: pd.DataFrame) -> pdk.Deck:
    """Render flights as great-circle layer and write map HTML."""
    flights_df = flights_df.copy()
    flights_df["date_vis"] = flights_df["Date"].dt.strftime("%Y-%m-%d")
    flights_df["tooltip"] = flights_df.apply(_flight_tooltip_row, axis=1)
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
    countries_layer = get_airport_countries_visited_deck_layer()

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
        layers=[countries_layer, flights_layer],
        initial_view_state=view_state,
        api_keys={"mapbox": MAPBOX_API_KEY, "google_maps": GOOGLE_MAPS_API_KEY},
        map_provider="mapbox",
        map_style="dark",  # ‘light’, ‘dark’, ‘road’, ‘satellite’,
        tooltip=tooltip,
    )
    r.picking_radius = 10
    r.to_html(FLIGHTS_MAP_FILENAME)
    return r


def get_airport_countries_visited_deck_layer() -> pdk.Layer:
    """Pydeck PolygonLayer for countries visited by flight (from airport locations)."""
    flights_df = get_flights_df()
    countries_df = get_countries_df()
    visited = get_countries(flights_df)
    alpha_3_codes = [c.alpha_3 for c in visited]
    layer_df = countries_df[countries_df["alpha_3"].isin(alpha_3_codes)].copy()
    layer_df["tooltip"] = layer_df.apply(lambda r: f"<div>{r['flag']} {r['name']}</div>", axis=1)
    return pdk.Layer(
        "PolygonLayer",
        data=layer_df,
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
