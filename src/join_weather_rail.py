from __future__ import annotations

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional

from .geo import nearest_site_index
from .config import TIME_TOL_MINUTES, MAX_STATION_DISTANCE_KM
from .io_schema import RAIL, WEATHER

@dataclass
class JoinStats:
    rail_rows: int
    weather_rows: int
    joined_rows: int
    dropped_time: int
    dropped_distance: int

def _to_utc(df: pd.DataFrame, col: str) -> pd.Series:
    # Convert timestamps robustly; keep as UTC
    ts = pd.to_datetime(df[col], errors="coerce", utc=True)
    return ts

def join_rail_with_weather(
    rail_df: pd.DataFrame,
    weather_df: pd.DataFrame,
    time_tolerance_minutes: int = TIME_TOL_MINUTES,
    max_station_distance_km: Optional[float] = MAX_STATION_DISTANCE_KM,
) -> tuple[pd.DataFrame, JoinStats]:
    """
    Steps:
      1) For each rail event, find nearest weather site by haversine distance.
      2) Within that site's weather history, join by nearest timestamp (merge_asof) with tolerance.
    """
    rail = rail_df.copy()
    w = weather_df.copy()

    # Normalize timestamps
    rail["_t"] = _to_utc(rail, RAIL["time"])
    w["_t"] = _to_utc(w, WEATHER["time"])

    rail = rail.dropna(subset=["_t", RAIL["lat"], RAIL["lon"], RAIL["target"]])
    w = w.dropna(subset=["_t", WEATHER["lat"], WEATHER["lon"]])

    # Pre-extract site coords
    site_coords = (
        w[[WEATHER["site"], WEATHER["lat"], WEATHER["lon"]]]
        .dropna()
        .drop_duplicates(subset=[WEATHER["site"]])
        .reset_index(drop=True)
    )
    site_lats = site_coords[WEATHER["lat"]].to_numpy()
    site_lons = site_coords[WEATHER["lon"]].to_numpy()

    # For each rail event, pick nearest site
    nearest_sites = []
    nearest_dists = []
    for lat, lon in zip(rail[RAIL["lat"]].to_numpy(), rail[RAIL["lon"]].to_numpy()):
        idx, dist_km = nearest_site_index(lat, lon, site_lats, site_lons)
        nearest_sites.append(site_coords.loc[idx, WEATHER["site"]])
        nearest_dists.append(dist_km)

    rail["_nearest_site"] = nearest_sites
    rail["_site_dist_km"] = nearest_dists

    # Optional distance filtering
    dropped_distance = 0
    if max_station_distance_km is not None:
        before = len(rail)
        rail = rail[rail["_site_dist_km"] <= float(max_station_distance_km)].copy()
        dropped_distance = before - len(rail)

    # Now time-align per site using merge_asof
    # Sort for merge_asof
    rail = rail.sort_values("_t")
    w = w.sort_values("_t")

    # We'll merge weather features by nearest time, grouped by nearest site.
    # Approach: split rail by site; merge_asof with weather filtered to that site.
    joined_parts = []
    dropped_time = 0

    tol = pd.Timedelta(minutes=int(time_tolerance_minutes))

    for site, rail_part in rail.groupby("_nearest_site", sort=False):
        w_part = w[w[WEATHER["site"]] == site].copy()
        if w_part.empty:
            # no weather for that site
            dropped_time += len(rail_part)
            continue

        # Keep only relevant columns from weather
        keep_cols = ["_t", WEATHER["site"], WEATHER["lat"], WEATHER["lon"]] + WEATHER["features"]
        w_part = w_part[keep_cols].dropna(subset=["_t"])

        merged = pd.merge_asof(
            rail_part.sort_values("_t"),
            w_part.sort_values("_t"),
            on="_t",
            direction="nearest",
            tolerance=tol,
        )

        # Rows with no match have NaNs in weather features
        ok = merged[WEATHER["features"][0]].notna() if WEATHER["features"] else merged[WEATHER["site"]].notna()
        dropped_time += int((~ok).sum())
        merged = merged[ok].copy()

        joined_parts.append(merged)

    if joined_parts:
        joined = pd.concat(joined_parts, ignore_index=True)
    else:
        joined = rail.iloc[0:0].copy()

    stats = JoinStats(
        rail_rows=int(len(rail_df)),
        weather_rows=int(len(weather_df)),
        joined_rows=int(len(joined)),
        dropped_time=int(dropped_time),
        dropped_distance=int(dropped_distance),
    )
    return joined, stats
