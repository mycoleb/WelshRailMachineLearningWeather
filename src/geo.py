from __future__ import annotations
import numpy as np

def haversine_km(lat1, lon1, lat2, lon2) -> np.ndarray:
    """
    Vectorized haversine distance.
    lat1/lon1 may be scalars; lat2/lon2 arrays (or vice versa).
    Returns distance in km.
    """
    R = 6371.0
    lat1 = np.radians(lat1)
    lon1 = np.radians(lon1)
    lat2 = np.radians(lat2)
    lon2 = np.radians(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))

def nearest_site_index(event_lat: float, event_lon: float, site_lats: np.ndarray, site_lons: np.ndarray) -> tuple[int, float]:
    d = haversine_km(event_lat, event_lon, site_lats, site_lons)
    idx = int(np.argmin(d))
    return idx, float(d[idx])
