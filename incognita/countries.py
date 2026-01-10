import geopandas as gpd
import pandas as pd
import pycountry
import pydeck as pdk
from shapely.geometry import MultiPolygon

from incognita.utils import df_from_gsheets, disk_memory, google_sheets_url, timed
from incognita.values import visited_map_filename


def explode_multi_polygons(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df_exploded = pd.DataFrame(columns=df.columns)
    for index, row in df.iterrows():
        if isinstance(row["geometry"], MultiPolygon):
            for geom_part in row["geometry"].geoms:
                new_row = row.copy()
                new_row["geometry"] = geom_part
                df_exploded = pd.concat([df_exploded, pd.DataFrame([new_row])], ignore_index=True)
        else:
            df_exploded = pd.concat([df_exploded, pd.DataFrame([row])], ignore_index=True)
    return df_exploded


def get_country_info(row) -> tuple[str, str, str]:
    alpha_3 = row["SOV_A3"]
    # get country name from ISO 3166-1 alpha-3 - apply mapping if needed
    alpha_3 = {
        "US1": "USA",
        "GB1": "GBR",
        "SDS": "SSD",
        "NZ1": "NZL",
        "NL1": "NLD",
        "AU1": "AUS",
        "FR1": "FRA",
        "FI1": "FIN",
        "CH1": "CHN",
        "DN1": "DNK",
        "IS1": "ISR",
        "KA1": "KAZ",
        "CU1": "CUB",
        "SAH": "ESH",
        "CYN": "CYP",
    }.get(alpha_3, alpha_3)
    country_info = pycountry.countries.get(alpha_3=alpha_3)
    if country_info is not None:
        return country_info.name, alpha_3, country_info.flag

    # manually get flag and name for disputed territories
    flag = {
        "KOS": "🇽🇰",  # kosovo
        "KAS": "🏴󠁩󠁮󠁪󠁫󠁿",  # kashmir
        # "SOL": ,  # somaliland
    }.get(alpha_3, "👀")
    name = row["SOVEREIGNT"]
    return name, alpha_3, flag


@timed
@disk_memory.cache
def get_countries_df(resolution: int = 10) -> gpd.GeoDataFrame:
    assert resolution in [10, 50, 110]
    shapefile = f"static/shapefiles/ne_{resolution}m_admin_0_countries.shp"
    world = gpd.read_file(shapefile)
    df_exploded = explode_multi_polygons(world)
    df_exploded["geometry"] = df_exploded["geometry"].apply(lambda x: x.__geo_interface__)
    df_exploded[["name", "alpha_3", "flag"]] = df_exploded.apply(
        get_country_info, axis=1, result_type="expand"
    )
    countries_df = df_exploded[["name", "alpha_3", "flag", "geometry"]]
    return countries_df


def get_countries_visited():
    countries_df = get_countries_df()
    visited_df = df_from_gsheets(google_sheets_url("countries"))
    visited_gdf = pd.merge(
        visited_df, countries_df[["name", "flag", "alpha_3", "geometry"]], on="name", how="inner"
    )
    return visited_gdf


def get_visited_stats(visited_gdf: gpd.GeoDataFrame) -> dict:
    return {"Countries": len(set(visited_gdf.name))}


def visited_df_to_deck_map(visited_gdf: gpd.GeoDataFrame) -> pdk.Deck:
    tooltip = {
        "html": """
    <div style='font-family: "Helvetica Neue",Helvetica,Arial,sans-serif; font-weight: 200;'>
        <div>{flag} {name} ({alpha_3})</div>
    </div>
        """,
        "style": {
            "backgroundColor": "rgba(25, 25, 25, 0.9)",
            "color": "white",
            "border": "1px solid black",
        },
    }

    polygon_layer = pdk.Layer(
        "PolygonLayer",
        data=visited_gdf,
        get_polygon="geometry.coordinates",
        get_line_color=[255, 100, 0],
        get_fill_color=[255, 100, 0],
        lineWidthMinPixels=0,
        opacity=0.4,
        filled=True,
        pickable=True,
    )

    view_state = pdk.ViewState(latitude=30, longitude=0, zoom=1, bearing=0, pitch=0)
    r = pdk.Deck(layers=[polygon_layer], initial_view_state=view_state, tooltip=tooltip)
    r.to_html(visited_map_filename)
    return r
