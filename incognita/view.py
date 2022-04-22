import folium
import geopandas


def generate_folium(
    trips: geopandas.GeoDataFrame,
    stationary_points: geopandas.GeoDataFrame = None,
    gdf_points: geopandas.GeoDataFrame = None,
) -> folium.folium.Map:
    """Generate a Folium map based on provided GeoDataFrames.
    Args:
        trips: return object from incognita.processing.split_into_trips
        stationary_points: GeoDataFrame with column "geometry" containing Points - indicating locations where stationary
        gdf_points: GeoDataFrames with columns [lon, lat], for plotting invidual coordinates. SLOW!!
    returns:
         map object
    """
    trips.crs = "EPSG:4326"  # set coordinate reference system for background map
    base_map = trips.explore(marker_kwds={"size": 0.5})
    if stationary_points is not None:
        base_map = stationary_points.explore(m=base_map, marker_kwds={"size": 3}, color="purple")
    if gdf_points is not None:
        base_map = gdf_points.explore(m=base_map, marker_kwds={"size": 1})  # add points to map
    return base_map
