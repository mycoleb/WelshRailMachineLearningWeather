from __future__ import annotations
import pandas as pd
import numpy as np

from .config import TARGET_COL
from .io_schema import WEATHER

def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    t = pd.to_datetime(out["_t"], utc=True)
    out["hour"] = t.dt.hour
    out["dow"] = t.dt.dayofweek  # 0=Mon
    out["month"] = t.dt.month
    out["is_weekend"] = (out["dow"] >= 5).astype(int)
    return out

def make_xy(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    out = add_time_features(df)

    base_feats = []
    for c in WEATHER["features"]:
        if c in out.columns:
            base_feats.append(c)

    engineered = ["hour", "dow", "month", "is_weekend", "_site_dist_km"]
    feats = base_feats + engineered

    X = out[feats].copy()
    y = out[TARGET_COL].astype(float).copy()

    # Fill small missing values (ideally you shouldn't have many after join)
    X = X.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return X, y
