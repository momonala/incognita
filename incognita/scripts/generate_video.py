import argparse
import logging
import os

import cv2
import numpy as np
import pandas as pd
from tqdm import tqdm

from incognita.data_models import GeoBoundingBox, GeoCoords
from incognita.database import get_gdf_from_db
from incognita.processing import add_speed_to_gdf

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
pd.options.mode.chained_assignment = None

IMG_SIZE = 1200


def remove_data_outside_region(gdf: pd.DataFrame) -> pd.DataFrame:
    lat_long_width = 0.205  # size of berlin, roughly
    berlin_bbox = GeoBoundingBox(center=GeoCoords(52.511626, 13.395842), width=lat_long_width)
    gdf_ber = gdf[
        (gdf.lat > berlin_bbox.ne.lat)
        & (gdf.lat < berlin_bbox.sw.lat)
        & (gdf.lon > berlin_bbox.ne.lon)
        & (gdf.lon < berlin_bbox.sw.lon)
    ]
    logger.info(f"Found {gdf_ber.shape} data points in region")
    # scale lat/long in bounding box to pixel values in frame
    gdf_ber["lat_norm"] = gdf_ber.lat - berlin_bbox.ne.lat
    gdf_ber["lon_norm"] = gdf_ber.lon - berlin_bbox.ne.lon
    gdf_ber = gdf_ber[(gdf_ber.lat_norm > 0) & (gdf_ber.lon_norm > 0)]
    return gdf_ber


def min_max_scaler(series: pd.Series, new_max=IMG_SIZE - 1) -> pd.Series:
    subt_min = series - series.min()
    return ((subt_min / subt_min.max()) * new_max).astype(int)


def get_minute_of_day(ts: pd.Timestamp) -> int:
    return ts.hour * 60 + ts.minute


def scale_data(gdf_ber: pd.DataFrame) -> pd.DataFrame:
    gdf_ber["pix_x"] = IMG_SIZE - min_max_scaler(gdf_ber.lat_norm)
    gdf_ber["pix_y"] = min_max_scaler(gdf_ber.lon_norm)
    # group data into days
    gdf_ber["datetime"] = pd.to_datetime(gdf_ber.timestamp)
    gdf_ber["day"] = gdf_ber["datetime"].apply(lambda x: x.replace(microsecond=0, second=0, minute=0, hour=0))
    gdf_ber["minute_of_day"] = gdf_ber.datetime.apply(get_minute_of_day)
    return gdf_ber


def gdf_to_frames(gdf_ber: pd.DataFrame, video_type: str, fade_range: int) -> np.array:
    FONT = cv2.FONT_HERSHEY_SIMPLEX
    ORG = (50, 70)
    FONTSCALE = 1.5
    COLOR = (255, 0, 0)
    THICKNESS = 2

    fade_decay_factor = np.floor(255 / fade_range).astype(int)  # how quickly we dim the pixels out
    minuts_per_day = 24 * 60
    out_mat = np.zeros((IMG_SIZE, IMG_SIZE, minuts_per_day), dtype=np.uint8)
    grouped_by_minute_of_day = gdf_ber.groupby("minute_of_day")

    for ts, df in tqdm(grouped_by_minute_of_day):
        minute_of_day = df.minute_of_day.iloc[0]

        if video_type == "fading":
            # this code will have fading trails
            out_mat[df.pix_x - 1, df.pix_y - 1, minute_of_day] = 255
            for fade_level in range(fade_range):
                grouped_by_minute_of_day.get_group(fade_level)
                out_mat[df.pix_x - 1, df.pix_y - 1, minute_of_day - fade_level] = (
                    255 - fade_level * fade_decay_factor
                )

        if video_type == "persistent":
            # this code will have persistent trails
            out_mat[df.pix_x - 1, df.pix_y - 1, minute_of_day:] = 255

        frame = out_mat[:, :, minute_of_day].astype(np.uint8)
        date = f"time: {df.datetime.iloc[0].strftime('%I:%M%p')}"

        out_mat[..., minute_of_day] = cv2.putText(
            frame, date, ORG, FONT, FONTSCALE, COLOR, THICKNESS, cv2.LINE_AA
        )
        img_frame_sequence = [
            out_mat[:, :, idx].reshape(IMG_SIZE, IMG_SIZE, 1) for idx in range(out_mat.shape[-1])
        ]
        return img_frame_sequence


def write_video(img_frame_sequence: np.array, outname: str):
    if os.path.exists(outname):
        os.remove(outname)
    fourcc = cv2.VideoWriter_fourcc(*"XVID")
    fps = 30
    out = cv2.VideoWriter(outname, fourcc, fps, (IMG_SIZE, IMG_SIZE), False)
    for frame in tqdm(img_frame_sequence):
        out.write(frame)
    out.release()


def main(outname, video_type, fade_range):
    gdf = add_speed_to_gdf(get_gdf_from_db())
    gdf_ber = remove_data_outside_region(gdf)
    gdf_ber = scale_data(gdf_ber)
    formatted_frames = gdf_to_frames(gdf_ber, video_type, fade_range)
    write_video(formatted_frames, outname)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--outname", type=str)
    parser.add_argument(
        "--video-type", type=str, default="fading", required=False, help="fading or persistent"
    )
    parser.add_argument(
        "--fade_range",
        type=int,
        default=4,
        required=False,
        help="how many points in the past to include in the fade",
    )
    args = parser.parse_args()

    main(args.outname, args.video_type, args.fade_range)
