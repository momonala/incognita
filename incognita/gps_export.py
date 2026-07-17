"""Build context for standalone GPS trace export pages."""

from datetime import datetime, timezone

from incognita.gps_geometry import get_haversine_dist
from incognita.gps_trips_renderer import get_trips_for_date_range
from incognita.health_database import get_health_dump_for_date_range

GPS_EXPORT_TITLE_MAX_LEN = 120
DATE_FMT = "%Y-%m-%d"

STAT_LABELS = (
    ("total_dist", "Distance"),
    ("walk_dist", "Walking"),
    ("steps", "Steps"),
    ("calories", "Calories"),
    ("stairs", "Stairs"),
)

DISTANCE_KEYS = frozenset({"total_dist", "walk_dist"})


def validate_export_title(title: str) -> str:
    """Return a trimmed export title or raise ValueError."""
    stripped = title.strip()
    if not stripped:
        raise ValueError("Title is required")
    if len(stripped) > GPS_EXPORT_TITLE_MAX_LEN:
        raise ValueError(f"Title must be at most {GPS_EXPORT_TITLE_MAX_LEN} characters")
    return stripped


def parse_export_dates(start_date: str, end_date: str) -> tuple[datetime, datetime, int]:
    """Parse and validate export date strings; return UTC bounds and inclusive day count."""
    try:
        start_dt = datetime.strptime(start_date, DATE_FMT).replace(tzinfo=timezone.utc)
        end_day = datetime.strptime(end_date, DATE_FMT)
        end_dt = end_day.replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError("Dates must be YYYY-MM-DD") from exc

    if start_dt > end_dt:
        raise ValueError("Start date must be on or before end date")

    day_count = (end_day.date() - start_dt.date()).days + 1
    return start_dt, end_dt, day_count


def format_display_date(date_str: str) -> str:
    """Format YYYY-MM-DD as '15 July, 2026'."""
    dt = datetime.strptime(date_str, DATE_FMT)
    return f"{dt.day} {dt.strftime('%B')}, {dt.year}"


def compute_paths_distance_km(paths: list[list[list[float]]]) -> float:
    """Sum haversine segment lengths across all trip paths."""
    total_m = 0.0
    for path in paths:
        for index in range(1, len(path)):
            lon1, lat1 = path[index - 1][0], path[index - 1][1]
            lon2, lat2 = path[index][0], path[index][1]
            total_m += get_haversine_dist(lat1, lon1, lat2, lon2)
    return round(total_m / 1000.0, 1)


def compute_bbox(paths: list[list[list[float]]]) -> dict[str, float]:
    """Return bounding box and center for trip paths."""
    min_lng = min(point[0] for path in paths for point in path)
    max_lng = max(point[0] for path in paths for point in path)
    min_lat = min(point[1] for path in paths for point in path)
    max_lat = max(point[1] for path in paths for point in path)
    return {
        "min_lng": min_lng,
        "max_lng": max_lng,
        "min_lat": min_lat,
        "max_lat": max_lat,
        "center_lng": (min_lng + max_lng) / 2,
        "center_lat": (min_lat + max_lat) / 2,
    }


def format_steps(steps: int | float | None) -> str:
    """Format step count for display."""
    if steps is None:
        return "—"
    steps_int = int(round(steps))
    if steps_int >= 1000:
        return f"{steps_int / 1000:.1f}k"
    return str(steps_int)


def format_km(km: float | None) -> str:
    """Format kilometers for display."""
    if km is None:
        return "—"
    return f"{km:.1f} km"


def format_calories(kcals: float | None) -> str:
    """Format active energy for display."""
    if kcals is None:
        return "—"
    return f"{int(round(kcals))} kcal"


def format_stairs(flights: int | float | None) -> str:
    """Format flights climbed (stairs) for display."""
    if flights is None:
        return "—"
    return str(int(flights))


def _stat_raw(
    *,
    total_dist_km: float | None,
    walk_dist_km: float | None,
    steps: int | float | None,
    kcals: float | None,
    stairs: int | float | None,
) -> dict[str, float | None]:
    """Return raw numeric values keyed by stat id."""
    return {
        "total_dist": total_dist_km,
        "walk_dist": walk_dist_km,
        "steps": float(steps) if steps is not None else None,
        "calories": float(kcals) if kcals is not None else None,
        "stairs": float(stairs) if stairs is not None else None,
    }


def _stat_cards_from_raw(raw: dict[str, float | None]) -> list[dict]:
    """Build template-ready stat cards from raw values (distances stored as km)."""
    cards: list[dict] = []
    for key, label in STAT_LABELS:
        value = raw[key]
        if key in DISTANCE_KEYS:
            cards.append(
                {
                    "key": key,
                    "label": label,
                    "value": format_km(value),
                    "km": None if value is None else round(float(value), 1),
                }
            )
            continue
        if key == "steps":
            display = format_steps(value)
        elif key == "calories":
            display = format_calories(value)
        else:
            display = format_stairs(value)
        cards.append({"key": key, "label": label, "value": display, "km": None})
    return cards


def _divide_avg(value: float | int | None, day_count: int) -> float | None:
    """Return value divided by day count, or None when value is missing."""
    if value is None:
        return None
    return float(value) / day_count


def build_gps_export_context(title: str, start_date: str, end_date: str) -> dict:
    """Load trips and summary stats for a GPS export page."""
    validated_title = validate_export_title(title)
    start_dt, end_dt, day_count = parse_export_dates(start_date, end_date)

    paths, _trip_stats = get_trips_for_date_range(start_dt, end_dt)
    path_list = paths or []

    health = get_health_dump_for_date_range(start_date, end_date)
    totals = health["totals"]
    total_km = compute_paths_distance_km(path_list) if path_list else 0.0

    total_raw = _stat_raw(
        total_dist_km=total_km,
        walk_dist_km=totals["km"],
        steps=totals["steps"],
        kcals=totals["kcals"],
        stairs=totals["flights_climbed"],
    )
    avg_raw = _stat_raw(
        total_dist_km=_divide_avg(total_km, day_count),
        walk_dist_km=_divide_avg(totals["km"], day_count),
        steps=_divide_avg(totals["steps"], day_count),
        kcals=_divide_avg(totals["kcals"], day_count),
        stairs=_divide_avg(totals["flights_climbed"], day_count),
    )

    bbox = compute_bbox(path_list) if path_list else None

    return {
        "title": validated_title,
        "start_date": start_date,
        "end_date": end_date,
        "start_date_display": format_display_date(start_date),
        "end_date_display": format_display_date(end_date),
        "day_count": day_count,
        "paths": path_list,
        "bbox": bbox,
        "stat_cards": _stat_cards_from_raw(total_raw),
        "avg_stat_cards": _stat_cards_from_raw(avg_raw),
        "has_paths": len(path_list) > 0,
    }
