import argparse
import logging
import math
import os
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import cv2
import numpy as np
import pandas as pd
from tqdm import tqdm

from incognita.data_models import GeoCoords
from incognita.database import get_gdf_from_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
logger = logging.getLogger(__name__)
pd.options.mode.chained_assignment = None

MINUTES_PER_DAY = 24 * 60
VIDEO_FPS = 30
VIDEO_CODEC = "mp4v"
OVERLAY_PADDING = 20
OVERLAY_TEXT_Y = 70
MAP_TITLE = "Berlin"
VIDEO_WIDTH = 1920
VIDEO_HEIGHT = 1080

BERLIN_CENTER = GeoCoords(52.511626, 13.395842)
# Half-width in degrees (lat); lon half-width is derived from canvas aspect to fill frame
LAT_HALF_WIDTH = 0.205 * 0.75


def remove_data_outside_region(
    gdf: pd.DataFrame,
    center: GeoCoords,
    lat_half_width: float,
    lon_half_width: float,
) -> pd.DataFrame:
    """Filter to points inside the rectangular region and add lat_norm, lon_norm."""
    lat_lo, lat_hi = center.lat - lat_half_width, center.lat + lat_half_width
    lon_lo, lon_hi = center.lon - lon_half_width, center.lon + lon_half_width
    gdf_region = gdf[
        (gdf.lat >= lat_lo) & (gdf.lat <= lat_hi) & (gdf.lon >= lon_lo) & (gdf.lon <= lon_hi)
    ].copy()
    gdf_region["lat_norm"] = gdf_region.lat - lat_lo
    gdf_region["lon_norm"] = gdf_region.lon - lon_lo
    logger.info("[remove_data_outside_region] Found %s data points in region", gdf_region.shape)
    return gdf_region


def min_max_scaler(series: pd.Series, new_max: int) -> pd.Series:
    """Scale series to [0, new_max] by min-max; returns zeros if series has no spread."""
    shifted = series - series.min()
    span = shifted.max()
    if span == 0:
        return pd.Series(0, index=series.index, dtype=int)
    return ((shifted / span) * new_max).astype(int)


def scale_data(
    gdf: pd.DataFrame,
    center_lat: float,
    map_h: int,
    map_w: int,
) -> pd.DataFrame:
    """Scale lat/lon to map pixels with latitude correction (cos) for correct aspect."""
    lat_rad = math.radians(center_lat)
    lon_corrected = gdf.lon_norm * math.cos(lat_rad)
    gdf = gdf.copy()
    gdf["pix_x"] = (map_h - 1) - min_max_scaler(gdf.lat_norm, map_h - 1)
    gdf["pix_y"] = min_max_scaler(lon_corrected, map_w - 1)
    gdf["datetime"] = pd.to_datetime(gdf.timestamp)
    gdf["day"] = gdf["datetime"].dt.normalize()
    gdf["minute_of_day"] = gdf["datetime"].dt.hour * 60 + gdf["datetime"].dt.minute
    logger.info("[scale_data] Scaled data")
    return gdf


def build_by_minute(
    gdf: pd.DataFrame,
    map_h: int,
    map_w: int,
) -> tuple[dict[int, tuple[np.ndarray, np.ndarray]], dict[int, str]]:
    """Build minute -> (pix_x, pix_y) arrays and minute -> display time."""
    by_minute: dict[int, tuple[np.ndarray, np.ndarray]] = {}
    minute_to_time: dict[int, str] = {}
    for minute_of_day, grp in gdf.groupby("minute_of_day"):
        minute = int(minute_of_day)
        px = np.clip(grp["pix_x"].to_numpy(dtype=np.int32), 0, map_h - 1)
        py = np.clip(grp["pix_y"].to_numpy(dtype=np.int32), 0, map_w - 1)
        by_minute[minute] = (px, py)
        minute_to_time[minute] = grp["datetime"].iloc[0].strftime("%I:%M%p")
    return by_minute, minute_to_time


def _lon_half_width(canvas_w: int, canvas_h: int, lat_half_width: float, center_lat: float) -> float:
    """Lon half-width so aspect-corrected region fills canvas (more data in view, no cropping)."""
    return lat_half_width * (canvas_w / canvas_h) / math.cos(math.radians(center_lat))


_EMPTY_PIX = (np.array([], dtype=np.int32), np.array([], dtype=np.int32))


def gdf_to_frames(
    by_minute: dict[int, tuple[np.ndarray, np.ndarray]],
    minute_to_time: dict[int, str],
    video_type: str,
    fade_range: int,
    map_h: int,
    map_w: int,
    canvas_w: int,
    canvas_h: int,
) -> Iterator[np.ndarray]:
    """Yield video frames on demand (no 3D buffer). Map at (map_h, map_w), centered if smaller."""
    font = cv2.FONT_HERSHEY_SIMPLEX
    font_scale = 1.5
    thickness = 2
    text_gray = 255
    x_off = 0 if map_w >= canvas_w else (canvas_w - map_w) // 2
    y_off = 0 if map_h >= canvas_h else (canvas_h - map_h) // 2
    fade_decay = int(255 / fade_range)

    accum = np.zeros((map_h, map_w), dtype=np.uint8) if video_type == "persistent" else None

    for idx in tqdm(range(MINUTES_PER_DAY)):
        if video_type == "persistent":
            px, py = by_minute.get(idx, _EMPTY_PIX)
            if len(px) > 0:
                accum[px, py] = 255
            map_frame = accum.copy()
        else:
            map_frame = np.zeros((map_h, map_w), dtype=np.uint8)
            # Draw oldest (dimmest) first so current minute (brightest) stays on top
            for fade_level in range(fade_range - 1, -1, -1):
                minute = max(0, idx - fade_level)
                px, py = by_minute.get(minute, _EMPTY_PIX)
                if len(px) > 0:
                    map_frame[px, py] = 255 - fade_level * fade_decay

        canvas = np.zeros((canvas_h, canvas_w), dtype=np.uint8)
        canvas[y_off : y_off + map_h, x_off : x_off + map_w] = map_frame
        date_str = minute_to_time.get(idx, "12:00AM")
        time_label = f"time: {date_str}"
        cv2.putText(
            canvas,
            MAP_TITLE,
            (OVERLAY_PADDING, OVERLAY_TEXT_Y),
            font,
            font_scale,
            text_gray,
            thickness,
            cv2.LINE_AA,
        )
        (time_w, _), _ = cv2.getTextSize(time_label, font, font_scale, thickness)
        time_x = canvas_w - time_w - OVERLAY_PADDING
        cv2.putText(
            canvas,
            time_label,
            (time_x, OVERLAY_TEXT_Y),
            font,
            font_scale,
            text_gray,
            thickness,
            cv2.LINE_AA,
        )
        yield canvas.reshape(canvas_h, canvas_w, 1)


def write_video(frame_iter: Iterator[np.ndarray], outname: str, width: int, height: int) -> None:
    if os.path.exists(outname):
        os.remove(outname)
    fourcc = cv2.VideoWriter_fourcc(*VIDEO_CODEC)
    out = cv2.VideoWriter(outname, fourcc, VIDEO_FPS, (width, height), False)
    for frame in tqdm(frame_iter, total=MINUTES_PER_DAY):
        out.write(frame)
    out.release()


def main(
    outname: Path | str,
    video_type: str,
    fade_range: int,
) -> None:
    canvas_w, canvas_h = VIDEO_WIDTH, VIDEO_HEIGHT
    center = BERLIN_CENTER
    lon_half_width = _lon_half_width(canvas_w, canvas_h, LAT_HALF_WIDTH, center.lat)
    map_w, map_h = canvas_w, canvas_h

    date_min, date_max = None, None
    gdf = get_gdf_from_db(date_min=date_min, date_max=date_max)
    logger.info("[main] Fetched %s data points in DB", gdf.shape)
    gdf_ber = remove_data_outside_region(gdf, center, LAT_HALF_WIDTH, lon_half_width)
    gdf_ber = scale_data(gdf_ber, center.lat, map_h, map_w)
    by_minute, minute_to_time = build_by_minute(gdf_ber, map_h, map_w)
    formatted_frames = gdf_to_frames(
        by_minute,
        minute_to_time,
        video_type,
        fade_range,
        map_h,
        map_w,
        canvas_w,
        canvas_h,
    )
    write_video(formatted_frames, str(outname), canvas_w, canvas_h)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--outname", type=str, default="busy-bee")
    parser.add_argument("--video-type", type=str, default="fading", help="fading or persistent")
    parser.add_argument(
        "--fade",
        type=int,
        default=120,
        help="how many points in the past to include in the fade",
    )
    args = parser.parse_args()

    assert args.video_type in ["fading", "persistent"], "video_type must be fading or persistent"
    assert args.fade >= 0, "fade must be greater than or equal to 0"
    today = datetime.now().strftime("%Y-%m-%d")
    outname = Path("tmp") / f"{args.outname}-{today}-{args.video_type}-{args.fade}.mp4"
    logger.info(f"[main] Starting video generation {args.outname}-{today}-{args.video_type}-{args.fade}.mp4")
    main(outname, args.video_type, args.fade)
