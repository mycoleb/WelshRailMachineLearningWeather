
---

## Source code

### `src/config.py`

```python
TIME_COL_RAIL = "event_time"
TIME_COL_WEATHER = "obs_time"

# Tolerance for time alignment (minutes)
TIME_TOL_MINUTES = 60

# If weather is sparse, you can allow farther stations; set to None to disable
MAX_STATION_DISTANCE_KM = 50.0

TARGET_COL = "delay_minutes"
RANDOM_SEED = 42
# If your CSV headers differ, change them here.

RAIL = {
    "time": "event_time",
    "station": "station_name",
    "lat": "lat",
    "lon": "lon",
    "target": "delay_minutes",
}

WEATHER = {
    "time": "obs_time",
    "site": "site_name",
    "lat": "lat",
    "lon": "lon",
    # weather feature columns you want to use
    "features": [
        "air_temp_c",
        "rain_mm",
        "wind_speed_mps",
        # add optional ones if present
        # "humidity_pct",
        # "pressure_hpa",
    ],
}
