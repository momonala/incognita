import logging

import pydeck as pdk
from joblib import Memory

from incognita.data_models import GeoBoundingBox
from incognita.database import get_start_end_date
from incognita.processing import get_data_for_maps
from incognita.utils import coordinates_from_place_name, timed
from incognita.view import get_map_deck

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
disk_memory = Memory("joblib_cache")


@timed
@disk_memory.cache
def get_deck_map_html(start_date: str, end_date: str, bbox: GeoBoundingBox, show_flights: bool) -> pdk.Deck:
    df, stationary_groups, trips_df = get_data_for_maps(start_date, end_date, show_flights)
    deck = get_map_deck(bbox, trips_df, stationary_groups, df)
    deck.to_html(f"maps/{start_date}_{end_date}.html")
    return deck


if __name__ == "__main__":
    start_date_base, end_date_base = tuple(x.split("T")[0] for x in get_start_end_date())
    # start_date_base = "2023-03-01"  # small range for debugging
    # end_date_base = "2023-01-29"
    default_location = coordinates_from_place_name("Berlin, De")
    map_html = get_deck_map_html(start_date_base, end_date_base, default_location, False)
