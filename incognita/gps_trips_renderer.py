import hashlib
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from tqdm import tqdm

from incognita.data_models import GeoBoundingBox, TripDisplayStats
from incognita.database import extract_properties_from_geojson, read_geojson_file
from incognita.gps import gps_df_to_deck_map
from incognita.gps_point_series import add_speed_to_gdf
from incognita.utils import DEFAULT_MAP_BOX, timed

logger = logging.getLogger(__name__)

RAW_DATA_ROOT = Path(__file__).resolve().parent.parent.parent / "incognita_raw_data"
GPS_POINT_COLUMNS = ["lon", "lat", "timestamp"]
METERS_PER_DEGREE = 111_000.0
MAX_WORKERS_CAP = 32
CPU_EXTRA_WORKERS = 4

memory = joblib.Memory(location=Path(".cache"), verbose=0)


def _month_root(year: int, month: int) -> Path:
    return RAW_DATA_ROOT / f"{year:04d}" / f"{month:02d}"


def _month_range(start: datetime, end: datetime) -> list[tuple[int, int]]:
    """Return (year, month) pairs from start through end inclusive."""
    months: list[tuple[int, int]] = []
    year, month = start.year, start.month
    end_year, end_month = end.year, end.month
    while (year, month) <= (end_year, end_month):
        months.append((year, month))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return months


def _compute_month_dir_hash(month_root: Path) -> str:
    """Hash for cache invalidation from the month directory's size and mtime.

    One stat() only. Hash changes when files are added/removed (dir mtime/size
    change). In-place edits to existing files may not change it; fine for caching.
    """
    stat = month_root.stat()
    hasher = hashlib.sha256()
    hasher.update(str(stat.st_size).encode("utf-8"))
    hasher.update(str(stat.st_mtime_ns).encode("utf-8"))
    return hasher.hexdigest()


def _load_one_geojson(path: Path) -> list[dict]:
    """Read one GeoJSON file and return list of parsed point dicts, or [] on failure."""
    raw = read_geojson_file(str(path))
    return extract_properties_from_geojson(raw) if raw else []


def _load_month_points(year: int, month: int) -> pd.DataFrame:
    """Load all raw GPS points for the given year and month from incognita_raw_data/."""
    month_root = _month_root(year, month)
    if not month_root.exists():
        logger.info(f"No incognita_raw_data directory for {year:04d}-{month:02d} ({month_root})")
        return pd.DataFrame(columns=GPS_POINT_COLUMNS)

    paths = list(month_root.rglob("*.geojson"))
    max_workers = min(MAX_WORKERS_CAP, (os.cpu_count() or 1) + CPU_EXTRA_WORKERS)
    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for chunk in executor.map(_load_one_geojson, paths):
            rows.extend(chunk)

    logger.debug(f"[_load_month_points] {month_root} | {len(rows)} points")
    if not rows:
        logger.info(f"[_load_month_points] {month_root} | no points")
        return pd.DataFrame(columns=GPS_POINT_COLUMNS)

    df = pd.DataFrame(rows)
    if df.empty:
        logger.info(f"Parsed empty DataFrame for {year}-{month} ({month_root})")
        return df

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def _segment_trips(
    gdf: pd.DataFrame,
    max_gap_seconds: float,
    max_gap_meters: float,
    min_points: int,
) -> list[pd.DataFrame]:
    """Split a points DataFrame into trip segments based on time/distance gaps."""
    if gdf.empty:
        return []

    gdf = add_speed_to_gdf(gdf)
    gdf["meters"] = gdf["meters"].fillna(0.0)

    time_gap = (gdf["time_diff"] > max_gap_seconds).fillna(False)
    dist_gap = (gdf["meters"] > max_gap_meters).fillna(False)
    is_gap = time_gap | dist_gap

    split_indices = list(gdf.index[is_gap])
    segment_starts: list[int] = [0] + [gdf.index.get_loc(idx) for idx in split_indices] + [len(gdf)]

    trips: list[pd.DataFrame] = []
    for i in range(len(segment_starts) - 1):
        start_pos = segment_starts[i]
        end_pos = segment_starts[i + 1]
        trip_df = gdf.iloc[start_pos:end_pos]
        if len(trip_df) < min_points:
            continue
        trips.append(trip_df)
    logger.info(f"[_segment_trips] {len(gdf)} raw points into {len(trips)} trips")
    return trips


def _douglas_peucker(coords: np.ndarray, tolerance_meters: float) -> tuple[np.ndarray, np.ndarray]:
    """Douglas–Peucker simplification; returns (simplified_coords, keep_mask).

    keep_mask indexes into the original coords so callers can align timestamps.
    """
    if coords.shape[0] <= 2:
        keep = np.ones(coords.shape[0], dtype=bool)
        return coords.copy(), keep

    tol_deg = tolerance_meters / METERS_PER_DEGREE
    keep = np.zeros(coords.shape[0], dtype=bool)
    keep[0] = True
    keep[-1] = True
    stack: list[tuple[int, int]] = [(0, coords.shape[0] - 1)]

    while stack:
        start_idx, end_idx = stack.pop()
        if end_idx - start_idx <= 1:
            continue
        start = coords[start_idx]
        end = coords[end_idx]
        segment = end - start
        seg_len = np.hypot(segment[0], segment[1])
        if seg_len == 0.0:
            continue
        idx_range = np.arange(start_idx + 1, end_idx)
        pts = coords[idx_range]
        v = pts - start
        cross = np.abs(segment[0] * v[:, 1] - segment[1] * v[:, 0])
        dist = cross / seg_len
        max_idx_rel = int(np.argmax(dist))
        if float(dist[max_idx_rel]) > tol_deg:
            idx_keep = idx_range[max_idx_rel]
            keep[idx_keep] = True
            stack.append((start_idx, idx_keep))
            stack.append((idx_keep, end_idx))

    return coords[keep], keep


def _trip_df_to_points_with_ts(trip_df: pd.DataFrame, simplify_tolerance_m: float) -> list[list[float]]:
    """Convert one segment to simplified path as list of [lon, lat, ts] (ts = unix seconds)."""
    coords = trip_df[["lon", "lat"]].to_numpy(dtype="float64")
    ts = trip_df["timestamp"].map(lambda t: t.timestamp()).to_numpy(dtype="float64")
    simplified, keep = _douglas_peucker(coords, tolerance_meters=simplify_tolerance_m)
    if simplified.shape[0] < 2:
        simplified = coords[[0, -1]]
        ts_kept = ts[[0, -1]]
    else:
        ts_kept = ts[keep]
    return [
        [float(simplified[i, 0]), float(simplified[i, 1]), float(ts_kept[i])] for i in range(len(ts_kept))
    ]


@timed
@memory.cache
def _get_month_trips_cached_impl(
    year: int,
    month: int,
    max_gap_seconds: float,
    max_gap_meters: float,
    min_points: int,
    simplify_tolerance_m: float,
    month_hash: str,
) -> list[list[list[float]]]:
    """Load points, segment into trips, simplify. Each point is [lon, lat, ts] (ts = unix sec)."""
    gdf = _load_month_points(year, month)
    if gdf.empty:
        return []

    segments = _segment_trips(
        gdf,
        max_gap_seconds=max_gap_seconds,
        max_gap_meters=max_gap_meters,
        min_points=min_points,
    )
    if not segments:
        logger.info(f"No trips found for {year}-{month} after segmentation")
        return []

    trips: list[list[list[float]]] = []
    total_points_before = 0
    total_points_after = 0
    for trip_df in segments:
        total_points_before += len(trip_df)
        path = _trip_df_to_points_with_ts(trip_df, simplify_tolerance_m)
        total_points_after += len(path)
        trips.append(path)

    if total_points_before == 0:
        reduction_pct = 0.0
    else:
        reduction_pct = 100.0 * (1.0 - total_points_after / total_points_before)
    logger.info(
        f"[_get_month_trips_cached_impl] "
        f"{month_hash[:8]} | {year}-{month:02d} |"
        f" {len(trips)} trips | points {total_points_before} → {total_points_after} |"
        f" {reduction_pct:.1f}% reduction | tolerance={simplify_tolerance_m:.1f}m"
    )
    return trips


def _truncate_trips_to_date_range(
    trips_with_ts: list[list[list[float]]],
    start: datetime,
    end: datetime,
) -> list[list[list[float]]]:
    """Filter trip points to [start, end] (per-day resolution); return paths as [lon, lat] only."""
    start_ts = start.timestamp()
    end_ts = end.timestamp()
    out: list[list[list[float]]] = []
    for path in trips_with_ts:
        in_range = [[lon, lat] for lon, lat, ts in path if start_ts <= ts <= end_ts]
        if len(in_range) >= 2:
            out.append(in_range)
    return out


@timed
def get_trips_for_date_range(
    start: datetime,
    end: datetime,
    max_gap_seconds: float = 60.0,
    max_gap_meters: float = 100.0,
    min_points: int = 10,
    simplify_tolerance_m: float = 5.0,
) -> tuple[list[list[list[float]]] | None, TripDisplayStats]:
    """Load and prepare trip paths for the date range. Does not write any file.

    Returns (paths_for_map, stats) when there are trips; (None, empty stats) otherwise.
    paths_for_map is a list of paths, each path a list of [lon, lat] points.
    """
    if start > end:
        raise ValueError("start must be <= end")

    trips: list[list[list[float]]] = []
    months = _month_range(start, end)

    for year, month in tqdm(months, desc="Processing months"):
        month_root = _month_root(year, month)
        if not month_root.exists():
            logger.info(f"No incognita_raw_data directory for {year:04d}-{month:02d} ({month_root})")
            continue
        month_hash = _compute_month_dir_hash(month_root)
        monthly_trips = _get_month_trips_cached_impl(
            year=year,
            month=month,
            max_gap_seconds=max_gap_seconds,
            max_gap_meters=max_gap_meters,
            min_points=min_points,
            simplify_tolerance_m=simplify_tolerance_m,
            month_hash=month_hash,
        )
        trips.extend(monthly_trips)

    if not trips:
        logger.info(f"No trips to render between {start.isoformat()} and {end.isoformat()}")
        return None, TripDisplayStats(track_points=0, trips_count=0)

    paths_for_map = _truncate_trips_to_date_range(trips, start, end)
    if not paths_for_map:
        logger.info(
            f"No trip points in range {start.date().isoformat()} → {end.date().isoformat()} after truncation"
        )
        return None, TripDisplayStats(track_points=0, trips_count=0)

    track_points = sum(len(p) for p in paths_for_map)
    stats = TripDisplayStats(track_points=track_points, trips_count=len(paths_for_map))
    return paths_for_map, stats


def render_trips_to_file(
    paths_for_map: list[list[list[float]]],
    gps_map_filename: Path,
    bbox: GeoBoundingBox,
) -> None:
    """Write trip paths to an HTML map file. No return value."""
    trips_df = pd.DataFrame({"geometry": paths_for_map})
    gps_df_to_deck_map(
        bbox,
        trips_df,
        points_df=pd.DataFrame(),
        filename=str(gps_map_filename),
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    resolution_meters = 4
    start_date = datetime(2021, 10, 1)
    end_date = datetime.today()
    filename = f"douglas_peucker-{start_date.year}{start_date.month:02d}-{end_date.year}{end_date.month:02d}-{resolution_meters}.html"
    gps_map_filename = Path("tmp", filename)
    paths, stats = get_trips_for_date_range(
        start_date,
        end_date,
        simplify_tolerance_m=resolution_meters,
    )
    if paths is not None:
        render_trips_to_file(paths, gps_map_filename, DEFAULT_MAP_BOX)
        os.system(f"open {gps_map_filename}")
