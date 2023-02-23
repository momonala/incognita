import logging
from time import time

import folium
import pandas as pd
import pydeck as pdk

from incognita.data_models import GeoBoundingBox
from incognita.secrets import MAPBOX_API_KEY, GOOGLE_MAPS_API_KEY
from incognita.utils import timed

logger = logging.getLogger(__name__)


@timed
def get_folium_map(
    bbox: GeoBoundingBox,
    trips_df: pd.DataFrame,
    stationary_groups: pd.DataFrame = None,
    df: pd.DataFrame = None,
) -> folium.folium.Map:
    """Generate a Folium map based on provided DataFrames.
    Args:
        bbox: bbox to center map on
        trips_df: return object from incognita.processing.split_into_trips
        stationary_groups: DataFrame with column "geometry" containing Points - indicating locations where stationary
        df: DataFrame with columns [lon, lat], for plotting invidual coordinates.
    """
    t0 = time()

    trips_df.set_crs("EPSG:4326", inplace=True)  # set coordinate reference system for background map
    base_map = trips_df.explore(marker_kwds={"size": 0.5})
    if stationary_groups is not None:
        base_map = stationary_groups.explore(m=base_map, marker_kwds={"size": 3}, color="purple")
    if df is not None:
        base_map = df.explore(m=base_map, marker_kwds={"size": 1})  # add points to map
    base_map.fit_bounds([bbox.sw.as_tuple(), bbox.ne.as_tuple()])
    logger.info(f"generated Folium map in {round(time() - t0, 1)}s")
    return base_map


def get_map_deck(
    bbox: GeoBoundingBox,
    trips_df: pd.DataFrame,
    stationary_groups: pd.DataFrame = None,
    df: pd.DataFrame = None,
) -> pdk.Deck:
    """Generate a Deck map based on provided DataFrames.
    Args:
        bbox: bbox to center map on
        trips_df: return object from incognita.processing.split_into_trips
        stationary_groups: DataFrame with column "geometry" containing Points - indicating locations where stationary
        df: DataFrames with columns [lon, lat], for plotting invidual coordinates.
    """
    t0 = time()
    trips = pdk.Layer(
        "TripsLayer",
        pd.DataFrame(trips_df["geometry"]),  # use df split_into_trips
        get_path="geometry",
        get_color=[255, 111, 0, 50],
        width_min_pixels=2,
        rounded=True,
    )
    heatmap = pdk.Layer(
        "HeatmapLayer",
        df[::7],
        get_position=["lon", "lat"],
        #     get_weight="speed_calc",
        aggregation="MEAN",
    )
    # stationary points
    stationary_points = pdk.Layer(
        "ScatterplotLayer",
        stationary_groups,
        get_position=["lon", "lat"],
        get_radius="num_points",
        filled=True,
        opacity=0.7,
        get_fill_color=[0, 255, 0],
    )
    # Set the viewport location
    view_state = pdk.ViewState(
        longitude=bbox.center.lon,
        latitude=bbox.center.lat,
        zoom=9,
        pitch=0,
        bearing=0,
    )
    # Render
    deck = pdk.Deck(
        layers=[
            trips,
            stationary_points,
            heatmap,
        ],
        initial_view_state=view_state,
        api_keys={"mapbox": MAPBOX_API_KEY, "google_maps": GOOGLE_MAPS_API_KEY},
        map_provider="google_maps",
        map_style="satellite",  # ‘light’, ‘dark’, ‘road’, ‘satellite’,
    )
    logger.info(f"generated Deck map in {round(time() - t0, 1)}s")
    return deck
