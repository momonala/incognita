#!/usr/bin/env python3
"""Visualize recent GPS coordinates using pydeck."""
import pandas as pd
import pydeck as pdk
import requests
from geopy.distance import geodesic
from joblib import Memory

memory = Memory(".cache")

# Berlin center coordinates
BERLIN_CENTER = [52.5200, 13.4050]
BERLIN_ZOOM = 11


def calculate_point_metrics(row: pd.Series) -> tuple[float, float, float]:
    """Calculate distance (km), time (hours), and speed (km/h) between this point and next point."""
    # Check if we have valid coordinates for both points
    if pd.isna(row["next_lat"]) or pd.isna(row["next_lon"]):
        return 0.0, 0.0, 0.0

    point1 = (row["lat"], row["lon"])
    point2 = (row["next_lat"], row["next_lon"])

    # Validate coordinates
    if not all(isinstance(x, (int, float)) and pd.notna(x) for x in point1 + point2):
        return 0.0, 0.0, 0.0

    # Calculate distance in kilometers between consecutive points
    try:
        distance = geodesic(point1, point2).kilometers
    except ValueError:
        return 0.0, 0.0, 0.0

    # Calculate time difference in hours between consecutive points
    time_diff = (row["next_timestamp"] - row["timestamp"]).total_seconds() / 3600

    # Avoid division by zero and validate time difference
    if time_diff <= 0:
        return distance, 0.0, 0.0

    speed = distance / time_diff

    return distance, time_diff, speed


@memory.cache
def get_coordinates(lookback_hours: int = 24) -> pd.DataFrame:
    response = requests.get(
        f"https://full-primarily-weevil.ngrok-free.app/incognita/coordinates?lookback_hours={lookback_hours}"
    )
    if response.status_code != 200:
        raise Exception(f"Failed to fetch coordinates: {response.text}")

    data = response.json()
    if data["status"] != "success":
        raise Exception(f"API returned error: {data}")
    print(f"Fetched {len(data['coordinates'])} points")
    return data


def fetch_coordinates(lookback_hours: int = 24) -> pd.DataFrame:
    """Fetch coordinates from the local API endpoint."""
    data = get_coordinates(lookback_hours)

    # Convert to DataFrame and parse timestamps
    coords = pd.DataFrame(data["coordinates"], columns=["timestamp", "lat", "lon", "accuracy"])
    coords["timestamp"] = pd.to_datetime(coords["timestamp"])

    # Ensure numeric types for coordinates and accuracy
    coords["lat"] = pd.to_numeric(coords["lat"], errors="coerce")
    coords["lon"] = pd.to_numeric(coords["lon"], errors="coerce")
    coords["accuracy"] = pd.to_numeric(coords["accuracy"], errors="coerce")

    # Drop any rows with invalid coordinates
    coords = coords.dropna(subset=["lat", "lon"])

    # Calculate next point coordinates, timestamp and accuracy
    coords["next_lon"] = coords["lon"].shift(-1)
    coords["next_lat"] = coords["lat"].shift(-1)
    coords["next_timestamp"] = coords["timestamp"].shift(-1)
    coords["next_accuracy"] = coords["accuracy"].shift(-1)

    # Calculate metrics between consecutive points
    point_metrics = coords.apply(calculate_point_metrics, axis=1)
    coords["point_distance"] = point_metrics.apply(lambda x: x[0])
    coords["point_time"] = point_metrics.apply(lambda x: x[1])
    coords["point_speed"] = point_metrics.apply(lambda x: x[2])

    # Format data for hover display
    coords["hover_time"] = coords["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    coords["hover_next_time"] = coords["next_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    coords["hover_point1"] = coords.apply(lambda x: f"({x['lat']:.6f}, {x['lon']:.6f})", axis=1)
    coords["hover_point2"] = coords.apply(
        lambda x: f"({x['next_lat']:.6f}, {x['next_lon']:.6f})" if pd.notna(x["next_lat"]) else "N/A", axis=1
    )
    coords["hover_accuracy1"] = coords["accuracy"].apply(
        lambda x: f"{x:.1f} meters" if pd.notna(x) else "N/A"
    )
    coords["hover_accuracy2"] = coords["next_accuracy"].apply(
        lambda x: f"{x:.1f} meters" if pd.notna(x) else "N/A"
    )
    coords["hover_point_dist"] = coords["point_distance"].apply(
        lambda x: f"{x*1000:.1f} meters" if x > 0 else "N/A"
    )
    coords["hover_point_speed"] = coords["point_speed"].apply(lambda x: f"{x:.1f} km/h" if x > 0 else "N/A")
    coords["hover_time_diff"] = coords.apply(
        lambda x: f"{x['point_time']*60:.0f} minutes" if x["point_time"] > 0 else "N/A", axis=1
    )

    # Drop rows where we don't have next point data
    coords = coords.dropna(subset=["next_lon", "next_lat"])

    print(f"Processing {len(coords)} points from {coords.timestamp.min()} to {coords.timestamp.max()}")
    return coords


def create_deck_map(df: pd.DataFrame) -> pdk.Deck:
    """Create a pydeck visualization with multiple layers."""
    # Create a line layer for normal distances (<=100m)
    normal_lines = df[df["point_distance"] <= 0.1]  # 100m = 0.1km
    normal_layer = pdk.Layer(
        "LineLayer",
        normal_lines,
        get_source_position=["lon", "lat"],
        get_target_position=["next_lon", "next_lat"],
        get_color=[255, 165, 0, 255],  # orange
        get_width=4,
        pickable=True,
    )

    # Create a line layer for large distances (>100m)
    large_jumps = df[df["point_distance"] > 0.1]  # 100m = 0.1km
    jumps_layer = pdk.Layer(
        "LineLayer",
        large_jumps,
        get_source_position=["lon", "lat"],
        get_target_position=["next_lon", "next_lat"],
        get_color=[30, 144, 255, 10],  # blue
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
        map_style="light",
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
                        <p style="margin: 0 0 4px 0;">Time Between: {hover_time_diff}</p>
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
    # Fetch the coordinates
    df = fetch_coordinates(lookback_hours=24 * 14)

    # Create and show the map
    deck = create_deck_map(df)

    # Save to HTML file
    output_file = f"recent_locations.html"
    deck.to_html(output_file, open_browser=True)
    print(f"Map saved to {output_file}")


if __name__ == "__main__":
    main()
