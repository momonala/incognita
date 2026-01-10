#!/usr/bin/env python3
"""Visualize recent GPS coordinates using pydeck."""
import pandas as pd
import pydeck as pdk
import requests
from joblib import Memory

from incognita.database import fetch_coordinates
from incognita.processing import add_speed_to_gdf
from incognita.values import GOOGLE_MAPS_API_KEY, MAPBOX_API_KEY

memory = Memory(".cache")

# Berlin center coordinates
BERLIN_CENTER = [52.5200, 13.4050]
BERLIN_ZOOM = 11

LOOKBACK_HOURS = 24 * 14
MAX_DISTANCE_M = 100
MIN_ACCURACY_M = 2000


def get_coordinates(
    lookback_hours: int = LOOKBACK_HOURS,
    min_accuracy: float = MIN_ACCURACY_M,
    max_distance: float = MAX_DISTANCE_M,
) -> list[tuple[str, float, float]]:
    # host = "http://localhost:5003"
    host = "https://trace.mnalavadi.org"
    response = requests.get(
        f"{host}/coordinates?lookback_hours={lookback_hours}&min_accuracy={min_accuracy}&max_distance={max_distance}"
    )
    if response.status_code != 200:
        raise Exception(f"Failed to fetch coordinates: {response.text}")

    data = response.json()
    if data["status"] != "success":
        raise Exception(f"API returned error: {data}")
    print(f"Fetched {len(data['coordinates'])} points")
    return data["coordinates"]


def create_deck_map(df: pd.DataFrame) -> pdk.Deck:
    """Create a pydeck visualization with multiple layers."""
    # Create hover text columns
    df["next_lat"] = df["lat"].shift(1)
    df["next_lon"] = df["lon"].shift(1)
    df["next_accuracy"] = df["accuracy"].shift(1)

    df["_ts"] = pd.to_datetime(df["timestamp"])
    df["hover_time"] = df["_ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["hover_next_time"] = df["hover_time"].shift(-1)
    df["hover_point1"] = df.apply(lambda x: f"({x['lat']:.6f}, {x['lon']:.6f})", axis=1)
    df["hover_point2"] = df.apply(
        lambda x: f"({x['next_lat']:.6f}, {x['next_lon']:.6f})" if pd.notna(x["next_lat"]) else "N/A", axis=1
    )
    df["hover_accuracy1"] = df["accuracy"].apply(lambda x: f"{x:.1f} meters" if pd.notna(x) else "N/A")
    df["hover_accuracy2"] = df["next_accuracy"].apply(lambda x: f"{x:.1f} meters" if pd.notna(x) else "N/A")
    df["hover_point_dist"] = df["meters"].apply(lambda x: f"{x:.1f} meters" if x > 0 else "N/A")
    df["hover_point_speed"] = df["speed_calc"].apply(lambda x: f"{x:.1f} km/h" if x > 0 else "N/A")

    normal_layer = pdk.Layer(
        "LineLayer",
        df[df["meters"] <= MAX_DISTANCE_M],
        get_source_position=["lon", "lat"],
        get_target_position=["next_lon", "next_lat"],
        get_color=[255, 165, 0, 255],  # orange
        get_width=4,
        pickable=True,
    )

    # Create a line layer for large distances
    jumps_layer = pdk.Layer(
        "LineLayer",
        df[df["meters"] > MAX_DISTANCE_M],
        get_source_position=["lon", "lat"],
        get_target_position=["next_lon", "next_lat"],
        get_color=[30, 144, 255, 35],  # blue
        get_width=4,
        pickable=True,
    )

    # Set the viewport location
    view_state = pdk.ViewState(
        latitude=BERLIN_CENTER[0],
        longitude=BERLIN_CENTER[1],
        zoom=BERLIN_ZOOM,
    )

    # Combine layers in a deck
    deck = pdk.Deck(
        # map_style="light",
        api_keys={"mapbox": MAPBOX_API_KEY, "google_maps": GOOGLE_MAPS_API_KEY},
        map_provider="mapbox",
        layers=[normal_layer, jumps_layer],  # normal lines first, then jumps
        initial_view_state=view_state,
        tooltip={
            "html": """
                <div style="font-family: Arial, sans-serif;">
                    <div style="margin-bottom: 12px;">
                        <p style="font-weight: bold; margin: 0 0 8px 0;">Point 1:</p>
                        <p style="margin: 0 0 4px 12px;">Time: {hover_time}</p>
                        <p style="margin: 0 0 4px 12px;">Coordinates: {hover_point1}</p>
                        <p style="margin: 0 0 4px 12px;">Accuracy: {hover_accuracy1}</p>
                    </div>
                    
                    <div style="margin-bottom: 12px;">
                        <p style="font-weight: bold; margin: 0 0 8px 0;">Point 2:</p>
                        <p style="margin: 0 0 4px 12px;">Time: {hover_next_time}</p>
                        <p style="margin: 0 0 4px 12px;">Coordinates: {hover_point2}</p>
                        <p style="margin: 0 0 4px 12px;">Accuracy: {hover_accuracy2}</p>
                    </div>
                    
                    <div style="border-top: 1px solid rgba(255,255,255,0.3); padding-top: 8px;">
                        <p style="margin: 0 0 4px 0;">Time Between: {time_diff}</p>
                        <p style="margin: 0 0 4px 0;">Distance: {hover_point_dist}</p>
                        <p style="margin: 0 0 0 0;">Speed: {hover_point_speed}</p>
                    </div>
                </div>
            """,
            "style": {
                "backgroundColor": "steelblue",
                "color": "white",
                "fontSize": "14px",
                "padding": "10px",
            },
        },
    )

    return deck


def main():
    """Fetch data and create visualization."""
    use_api = False
    if use_api:
        coordinates = get_coordinates(
            lookback_hours=LOOKBACK_HOURS, min_accuracy=MIN_ACCURACY_M, max_distance=MAX_DISTANCE_M
        )
        coordinates = pd.DataFrame(coordinates, columns=["timestamp", "lat", "lon", "accuracy"])

    else:
        coordinates = fetch_coordinates(lookback_hours=LOOKBACK_HOURS, min_accuracy=MIN_ACCURACY_M)
        coordinates = pd.DataFrame(coordinates, columns=["timestamp", "lat", "lon", "accuracy"])
    coordinates = add_speed_to_gdf(coordinates)
    # Convert timestamp to datetime and filter for June 14, 2025
    coordinates["timestamp"] = pd.to_datetime(coordinates["timestamp"])
    coordinates = coordinates[coordinates["timestamp"].dt.date == pd.to_datetime("2025-06-11").date()]

    # Display the filtered data
    deck = create_deck_map(coordinates)
    output_file = "recent_locations.html"
    deck.to_html(output_file, open_browser=True)
    print(f"Map saved to {output_file}")


if __name__ == "__main__":
    main()
