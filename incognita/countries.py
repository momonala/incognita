from pathlib import Path

import geopandas as gpd
import pandas as pd
import pycountry
import pydeck as pdk

from incognita.config import VISITED_MAP_FILENAME
from incognita.utils import google_sheets_export_csv_url, read_google_sheets_csv

# Only 10 is used by get_countries_df (passport); 50 and 110 are reserved for future use.
RESOLUTION_OPTIONS = (10, 50, 110)
EQUAL_AREA_CRS = "EPSG:6933"
AREA_M2_TO_KM2 = 1e6

SOV_A3_TO_ALPHA3 = {
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
}


COUNTRY_COLUMNS = ["name", "alpha_3", "flag", "alpha_2", "official_name", "geometry", "area_km2"]

DISPUTED_FLAGS = {
    "KOS": "🇽🇰",
    "KAS": "🏴󠁩󠁮󠁪󠁫󠁿",
}

SHEET_NAME_TO_SHAPEFILE = {"Macau": "Macao"}

FLAG_OVERRIDES = {"HKG": "🇭🇰", "MAC": "🇲🇴"}


def get_country_info(row: pd.Series) -> tuple[str, str, str, str, str]:
    """Resolve name, alpha_3, flag, alpha_2, official_name from pycountry (or fallback for disputed)."""
    alpha_3 = SOV_A3_TO_ALPHA3.get(row["SOV_A3"], row["SOV_A3"])
    country_info = pycountry.countries.get(alpha_3=alpha_3)
    if country_info is not None:
        flag = FLAG_OVERRIDES.get(alpha_3, country_info.flag)
        official = getattr(country_info, "official_name", None) or country_info.name
        return country_info.name, alpha_3, flag, country_info.alpha_2, official
    flag = DISPUTED_FLAGS.get(alpha_3) or FLAG_OVERRIDES.get(alpha_3) or "👀"
    return row["SOVEREIGNT"], alpha_3, flag, "", row["SOVEREIGNT"]


def _country_info_lookup(world: gpd.GeoDataFrame) -> pd.DataFrame:
    """One row per SOV_A3 with name, alpha_3, flag, alpha_2, official_name."""
    unique = world[["SOV_A3", "SOVEREIGNT"]].drop_duplicates(subset=["SOV_A3"])
    rows = [get_country_info(row) for _, row in unique.iterrows()]
    return pd.DataFrame(rows, columns=["name", "alpha_3", "flag", "alpha_2", "official_name"]).assign(
        SOV_A3=unique["SOV_A3"].values
    )


def get_countries_df(resolution: int = 10) -> gpd.GeoDataFrame:
    """Load countries from Natural Earth shapefile with name, alpha_3, flag, alpha_2, official_name, geometry, area_km2."""
    if resolution not in RESOLUTION_OPTIONS:
        raise ValueError(f"resolution must be one of {RESOLUTION_OPTIONS}")
    shapefile = Path(
        f"static/shapefiles/ne_{resolution}m_admin_0_countries/ne_{resolution}m_admin_0_countries.shp"
    )
    if not shapefile.exists():
        raise FileNotFoundError(f"Shapefile {shapefile} not found")
    world = gpd.read_file(shapefile)
    df_exploded = world.explode(index_parts=False).reset_index(drop=True)
    area_m2 = df_exploded.to_crs(EQUAL_AREA_CRS).geometry.area
    df_exploded["area_km2"] = (area_m2 / AREA_M2_TO_KM2).values
    geo_dicts = [g.__geo_interface__ for g in df_exploded["geometry"]]
    df_exploded = pd.DataFrame(df_exploded.drop(columns=["geometry"]))
    df_exploded["geometry"] = geo_dicts
    info = _country_info_lookup(world)
    df_exploded = df_exploded.merge(info, on="SOV_A3", how="left")
    # Shapefile has SOV_A3=CH1 for Hong Kong/Macao; patch name and flag from ADM0_A3/NAME so sheet merge matches.
    for adm0 in ("HKG", "MAC"):
        mask = df_exploded["ADM0_A3"] == adm0
        if not mask.any():
            continue
        c = pycountry.countries.get(alpha_3=adm0)
        df_exploded.loc[mask, "name"] = df_exploded.loc[mask, "NAME"]
        df_exploded.loc[mask, "alpha_3"] = adm0
        df_exploded.loc[mask, "flag"] = FLAG_OVERRIDES[adm0]
        if c:
            df_exploded.loc[mask, "alpha_2"] = c.alpha_2
            df_exploded.loc[mask, "official_name"] = getattr(c, "official_name", None) or c.name
    return df_exploded[COUNTRY_COLUMNS].copy()


def get_countries_visited(countries_df: gpd.GeoDataFrame | None = None) -> pd.DataFrame:
    """Merge countries sheet with geometries. Sheet has columns: name, year (e.g. "January 1994"). Sorted most recent first."""
    if countries_df is None:
        countries_df = get_countries_df()
    visited_df = read_google_sheets_csv(google_sheets_export_csv_url("countries"))
    visited_df.columns = visited_df.columns.str.strip().str.lower()
    visited_df["name"] = visited_df["name"].replace(SHEET_NAME_TO_SHAPEFILE)
    visited_df["_year_parsed"] = pd.to_datetime(visited_df["year"], format="%B %Y", errors="coerce")
    visited_df = visited_df.sort_values("_year_parsed", ascending=False, na_position="last").drop(
        columns=["_year_parsed"]
    )
    return pd.merge(visited_df, countries_df[COUNTRY_COLUMNS], on="name", how="inner")


def get_visited_stats(
    visited_gdf: pd.DataFrame, countries_df: gpd.GeoDataFrame | None = None
) -> dict[str, int | float]:
    """Compute visited count and percentage of world by country count and by area."""
    if countries_df is None:
        countries_df = get_countries_df()
    total_count = countries_df["name"].nunique()
    total_area_km2 = countries_df["area_km2"].sum()
    visited_count = visited_gdf["name"].nunique()
    visited_area_km2 = visited_gdf["area_km2"].sum()
    pct_count = round(100 * visited_count / total_count, 1) if total_count > 0 else 0.0
    pct_area = round(100 * visited_area_km2 / total_area_km2, 1) if total_area_km2 > 0 else 0.0
    return {
        "Countries": visited_count,
        "total_count": total_count,
        "total_area_km2": total_area_km2,
        "visited_area_km2": visited_area_km2,
        "pct_by_count": pct_count,
        "pct_by_area": pct_area,
    }


def _tooltip_row(row: pd.Series) -> str:
    """Build tooltip HTML for one row: flag, name, alpha_3, optional alpha_2, official_name, and year."""
    parts = [f"<div>{row['flag']} {row['name']} ({row['alpha_3']})</div>"]
    if row.get("alpha_2"):
        parts.append(f"<div>Code: {row['alpha_2']}</div>")
    official = row.get("official_name")
    if official and str(official).strip() != str(row["name"]).strip():
        parts.append(f"<div>{official}</div>")
    year_val = row.get("year")
    if year_val is not None and pd.notna(year_val) and str(year_val).strip():
        parts.append(f"<div>Visited: {year_val}</div>")
    return "".join(parts)


def visited_df_to_deck_map(visited_gdf: pd.DataFrame) -> pdk.Deck:
    """Render visited countries to an HTML map and return the Deck instance."""
    visited_gdf = visited_gdf.copy()
    visited_gdf["tooltip"] = visited_gdf.apply(_tooltip_row, axis=1)
    tooltip = {
        "html": "<div style='font-family: \"Helvetica Neue\",Helvetica,Arial,sans-serif; font-weight: 200;'>{tooltip}</div>",
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
    r.to_html(VISITED_MAP_FILENAME)
    return r
