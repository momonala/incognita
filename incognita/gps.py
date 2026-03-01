import logging
import os
from datetime import datetime
from functools import lru_cache

import pandas as pd
import pydeck as pdk

from incognita.data_models import GeoBoundingBox
from incognita.database import DB_FILE, get_gdf_from_db
from incognita.processing import (add_speed_to_gdf, get_stationary_groups,
                                  split_into_trips)
from incognita.utils import disk_memory, timed
from incognita.values import (GOOGLE_MAPS_API_KEY, MAPBOX_API_KEY,
                              gps_map_filename)

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


@timed
@lru_cache
@disk_memory.cache
def _get_all_data(date_modified: datetime):
    logger.info(f"Fetching data from db. Last modified: {date_modified}")
    gdf = add_speed_to_gdf(get_gdf_from_db())
    gdf["timestamp"] = pd.to_datetime(gdf["timestamp"])
    logger.info(f"{gdf.shape=}")
    logger.debug(gdf.tail(1))
    trips = split_into_trips(gdf, max_dist_meters=60)
    stationary_points = get_stationary_groups(gdf)
    return gdf, stationary_points, trips


@timed
def get_data_for_maps(start_date: str, end_date: str):
    start_date = pd.to_datetime(start_date, utc=True).replace(hour=0, minute=0)
    end_date = pd.to_datetime(end_date, utc=True).replace(hour=23, minute=59)

    modified_time = datetime.fromtimestamp(os.path.getmtime(DB_FILE))
    gdf, stationary_points, trips = _get_all_data(modified_time)

    # filter by time
    gdf = gdf[(gdf["timestamp"] >= start_date) & (gdf["timestamp"] <= end_date)]
    stationary_points = stationary_points[
        (stationary_points["start"] >= start_date) & (stationary_points["end"] <= end_date)
    ]
    trips = trips[(trips["start"] >= start_date) & (trips["end"] <= end_date)]
    return gdf, stationary_points, trips


@timed
def get_deck_map_html(start_date: str, end_date: str, bbox: GeoBoundingBox) -> str:
    df, stationary_groups, trips_df = get_data_for_maps(start_date, end_date)
    return gps_df_to_deck_map(bbox, trips_df, stationary_groups, df)


@timed
def gps_df_to_deck_map(
    bbox: GeoBoundingBox,
    trips_df: pd.DataFrame,
    stationary_groups: pd.DataFrame = None,
    df: pd.DataFrame = None,  # noqa
) -> pdk.Deck:
    """Generate a Deck map based on provided DataFrames.
    Args:
        bbox: bbox to center map on
        trips_df: return object from incognita.processing.split_into_trips
        stationary_groups: DataFrame with column "geometry" containing Points - indicating locations where stationary
        df: DataFrames with columns [lon, lat], for plotting invidual coordinates.
    """
    trips = pdk.Layer(
        "TripsLayer",
        trips_df,
        get_path="geometry",
        get_color=[255, 111, 0, 200],
        width_min_pixels=3,
        rounded=True,
        pickable=True,
    )
    # heatmap = pdk.Layer(
    #     "HeatmapLayer",
    #     df[::10],
    #     get_position=["lon", "lat"],
    #     # get_weight="speed_calc",
    #     aggregation="MEAN",
    #     pickable=True,  # allows hover
    # )

    stationary_points = pdk.Layer(
        "ScatterplotLayer",
        stationary_groups,
        get_position=["lon", "lat"],
        get_radius="num_points",
        filled=True,
        opacity=0.7,
        get_fill_color=[0, 255, 0],
        pickable=True,
    )

    view_state = pdk.ViewState(
        longitude=bbox.center.lon,
        latitude=bbox.center.lat,
        zoom=12,
        pitch=0,
        bearing=0,
    )

    r = pdk.Deck(
        layers=[
            trips,
            stationary_points,
            # heatmap,
        ],
        initial_view_state=view_state,
        api_keys={"mapbox": MAPBOX_API_KEY, "google_maps": GOOGLE_MAPS_API_KEY},
        map_provider="google_maps",
        map_style="satellite",  # ‘light’, ‘dark’, ‘road’, ‘satellite’,
    )
    r.to_html(filename=gps_map_filename)
    return r
