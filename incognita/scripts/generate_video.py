import argparse
import cProfile
import logging
import os
import pstats
import time
from datetime import datetime
from pathlib import Path

import cv2
import numpy as np
import pandas as pd
from tqdm import tqdm

from incognita.data_models import GeoBoundingBox, GeoCoords
from incognita.database import get_gdf_from_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
logger = logging.getLogger(__name__)
pd.options.mode.chained_assignment = None

IMG_SIZE = 1000
MINUTES_PER_DAY = 24 * 60


def remove_data_outside_region(gdf: pd.DataFrame) -> pd.DataFrame:
    lat_long_width = 0.205  # size of berlin, roughly
    berlin_bbox = GeoBoundingBox(center=GeoCoords(52.511626, 13.395842), width=lat_long_width)
    gdf_ber = gdf[
        (gdf.lat > berlin_bbox.ne.lat)
        & (gdf.lat < berlin_bbox.sw.lat)
        & (gdf.lon > berlin_bbox.ne.lon)
        & (gdf.lon < berlin_bbox.sw.lon)
    ]
    # scale lat/long in bounding box to pixel values in frame
    gdf_ber["lat_norm"] = gdf_ber.lat - berlin_bbox.ne.lat
    gdf_ber["lon_norm"] = gdf_ber.lon - berlin_bbox.ne.lon
    gdf_ber = gdf_ber[(gdf_ber.lat_norm > 0) & (gdf_ber.lon_norm > 0)]
    logger.info(f"[remove_data_outside_region] Found {gdf_ber.shape} data points in region")
    return gdf_ber


def min_max_scaler(series: pd.Series, new_max=IMG_SIZE - 1) -> pd.Series:
    subt_min = series - series.min()
    return ((subt_min / subt_min.max()) * new_max).astype(int)


def scale_data(gdf_ber: pd.DataFrame) -> pd.DataFrame:
    gdf_ber["pix_x"] = IMG_SIZE - min_max_scaler(gdf_ber.lat_norm)
    gdf_ber["pix_y"] = min_max_scaler(gdf_ber.lon_norm)
    gdf_ber["datetime"] = pd.to_datetime(gdf_ber.timestamp)
    gdf_ber["day"] = gdf_ber["datetime"].dt.normalize()
    gdf_ber["minute_of_day"] = gdf_ber["datetime"].dt.hour * 60 + gdf_ber["datetime"].dt.minute
    logger.info("[scale_data] Scaled data")
    return gdf_ber


def build_by_minute(gdf_ber: pd.DataFrame) -> tuple[dict[int, tuple[np.ndarray, np.ndarray]], dict[int, str]]:
    """Build minute -> (pix_x, pix_y) arrays and minute -> display time for the frame loop."""
    by_minute: dict[int, tuple[np.ndarray, np.ndarray]] = {}
    minute_to_time: dict[int, str] = {}
    for minute_of_day, grp in gdf_ber.groupby("minute_of_day"):
        m = int(minute_of_day)
        px = np.clip(grp["pix_x"].to_numpy(dtype=np.int32) - 1, 0, IMG_SIZE - 1)
        py = np.clip(grp["pix_y"].to_numpy(dtype=np.int32) - 1, 0, IMG_SIZE - 1)
        by_minute[m] = (px, py)
        minute_to_time[m] = grp["datetime"].iloc[0].strftime("%I:%M%p")
    return by_minute, minute_to_time


def gdf_to_frames(
    by_minute: dict[int, tuple[np.ndarray, np.ndarray]],
    minute_to_time: dict[int, str],
    video_type: str,
    fade_range: int,
):
    """Yield video frames one at a time. Uses by_minute (no groupby) and in-place putText (no frame copy)."""
    FONT = cv2.FONT_HERSHEY_SIMPLEX
    ORG = (50, 70)
    FONTSCALE = 1.5
    COLOR = (255, 0, 0)
    THICKNESS = 2

    fade_decay_factor = int(255 / fade_range)
    out_mat = np.zeros((IMG_SIZE, IMG_SIZE, MINUTES_PER_DAY), dtype=np.uint8)

    t_index = 0.0
    t_text = 0.0
    for minute_of_day in tqdm(range(MINUTES_PER_DAY)):
        pix_x, pix_y = by_minute.get(
            minute_of_day, (np.array([], dtype=np.int32), np.array([], dtype=np.int32))
        )

        t0 = time.perf_counter()
        if len(pix_x) > 0:
            if video_type == "fading":
                out_mat[pix_x, pix_y, minute_of_day] = 255
                for fade_level in range(1, fade_range):
                    past_minute = max(0, minute_of_day - fade_level)
                    out_mat[pix_x, pix_y, past_minute] = 255 - fade_level * fade_decay_factor
            if video_type == "persistent":
                out_mat[pix_x, pix_y, minute_of_day:] = 255
        t_index += time.perf_counter() - t0

        t0 = time.perf_counter()
        frame = np.ascontiguousarray(out_mat[:, :, minute_of_day])
        date_str = minute_to_time.get(minute_of_day, "12:00AM")
        cv2.putText(frame, f"time: {date_str}", ORG, FONT, FONTSCALE, COLOR, THICKNESS, cv2.LINE_AA)
        out_mat[:, :, minute_of_day] = frame
        t_text += time.perf_counter() - t0

    logger.info(
        "gdf_to_frames timing: index=%.1fs putText=%.1fs total=%.1fs",
        t_index,
        t_text,
        t_index + t_text,
    )

    for idx in range(out_mat.shape[-1]):
        yield out_mat[:, :, idx].reshape(IMG_SIZE, IMG_SIZE, 1)


def write_video(frame_iter, outname: str):
    if os.path.exists(outname):
        os.remove(outname)
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    fps = 30
    out = cv2.VideoWriter(outname, fourcc, fps, (IMG_SIZE, IMG_SIZE), False)
    for frame in tqdm(frame_iter, total=MINUTES_PER_DAY):
        out.write(frame)
    out.release()


def main(outname, video_type, fade_range, days_back: int | None = None):
    logger.info("[main] Starting video generation")
    if days_back is not None:
        end_ts = pd.Timestamp.now(tz="UTC")
        start_ts = end_ts - pd.Timedelta(days=days_back)
        date_min = start_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        date_max = end_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        gdf = get_gdf_from_db(date_min=date_min, date_max=date_max)
    else:
        gdf = get_gdf_from_db()
    logger.info(f"[main] Fetched {gdf.shape} data points in DB")
    gdf_ber = remove_data_outside_region(gdf)
    gdf_ber = scale_data(gdf_ber)
    by_minute, minute_to_time = build_by_minute(gdf_ber)
    formatted_frames = gdf_to_frames(by_minute, minute_to_time, video_type, fade_range)
    write_video(formatted_frames, outname)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--outname", type=str, default="busy-bee")
    parser.add_argument(
        "--video-type", type=str, default="fading", required=False, help="fading or persistent"
    )
    parser.add_argument(
        "--fade_range",
        type=int,
        default=12,
        required=False,
        help="how many points in the past to include in the fade",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=None,
        metavar="N",
        help="limit data to last N days (reduces DB load and memory)",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="run under cProfile and print cumulative time stats after completion",
    )
    args = parser.parse_args()

    assert args.video_type in ["fading", "persistent"], "video_type must be fading or persistent"
    assert args.fade_range >= 0, "fade_range must be greater than or equal to 0"
    today = datetime.now().strftime("%Y-%m-%d")
    outname = Path("tmp") / f"{args.outname}-{today}-{args.video_type}-{args.fade_range}days.mp4"

    if args.profile:
        profiler = cProfile.Profile()
        profiler.enable()
        main(outname, args.video_type, args.fade_range, args.days_back)
        profiler.disable()
        stats = pstats.Stats(profiler)
        stats.sort_stats(pstats.SortKey.CUMULATIVE)
        stats.print_stats(40)
    else:
        main(outname, args.video_type, args.fade_range, args.days_back)
