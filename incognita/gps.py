import logging
import os

import pandas as pd
import pydeck as pdk

from incognita.config import GPS_MAP_FILENAME
from incognita.data_models import GeoBoundingBox
from incognita.database import get_gdf_for_map
from incognita.gps_point_series import add_speed_to_gdf
from incognita.observability import timed
from incognita.utils import BYTES_PER_MB
from incognita.values import GOOGLE_MAPS_API_KEY, MAPBOX_API_KEY

logger = logging.getLogger(__name__)

# Align with /coordinates API: min_accuracy and max_distance in meters
GPS_MIN_ACCURACY_M = 100.0
GPS_MAX_DISTANCE_M = 100.0
GPS_POINT_RADIUS_PX = 8
GPS_POINT_OPACITY = 180


def get_filtered_gps_df(
    date_min: str,
    date_max: str,
    min_accuracy_m: float = GPS_MIN_ACCURACY_M,
    max_distance_m: float = GPS_MAX_DISTANCE_M,
) -> pd.DataFrame:
    """Single code path: load GPS points (speed>0, accuracy), add speed, filter by segment distance.

    Used by the DB-backed GET /coordinates endpoint.
    """
    gdf = get_gdf_for_map(date_min, date_max, min_accuracy=min_accuracy_m)
    if gdf.empty:
        return gdf
    gdf["timestamp"] = pd.to_datetime(gdf["timestamp"])
    gdf = add_speed_to_gdf(gdf)
    gdf["meters"] = gdf["meters"].fillna(0.0)
    gdf = gdf[gdf["meters"] <= max_distance_m]
    mem_mb = gdf.memory_usage(deep=True).sum() / BYTES_PER_MB
    logger.debug("[get_filtered_gps_df] shape=%s mem=%.2f MB", gdf.shape, mem_mb)
    return gdf


@timed
def gps_df_to_deck_map(
    bbox: GeoBoundingBox,
    trips_df: pd.DataFrame,
    points_df: pd.DataFrame = pd.DataFrame(),
    filename: str = GPS_MAP_FILENAME,
) -> tuple[pdk.Deck, int, float]:
    """Generate a Deck map based on provided DataFrames. Returns (deck, points_count, file_size_mb)."""
    layers = []
    if not points_df.empty:
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                points_df,
                get_position=["lon", "lat"],
                get_radius=GPS_POINT_RADIUS_PX,
                get_fill_color=[255, 111, 0, GPS_POINT_OPACITY],
                pickable=True,
            )
        )

    if not trips_df.empty:
        layers.append(
            pdk.Layer(
                "TripsLayer",
                trips_df,
                get_path="geometry",
                get_color=[255, 111, 0, 200],
                width_min_pixels=3,
                rounded=True,
                pickable=True,
            )
        )

    view_state = pdk.ViewState(
        longitude=bbox.center.lon,
        latitude=bbox.center.lat,
        zoom=12,
        pitch=0,
        bearing=0,
    )

    r = pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        api_keys={"mapbox": MAPBOX_API_KEY, "google_maps": GOOGLE_MAPS_API_KEY},
        map_provider="google_maps",
        map_style="satellite",  # ‘light’, ‘dark’, ‘road’, ‘satellite’,
    )
    r.to_html(filename=filename)
    size_bytes = os.path.getsize(filename)
    size_mb = size_bytes / BYTES_PER_MB
    points_count = len(points_df)
    if not trips_df.empty and "geometry" in trips_df.columns:
        points_count += sum(len(geom) for geom in trips_df["geometry"])
    logger.debug(
        "[gps_df_to_deck_map] points_shape=%s trips_shape=%s filename=%s size_mb=%.2f",
        points_df.shape,
        trips_df.shape,
        filename,
        size_mb,
    )
    return r, points_count, size_mb
