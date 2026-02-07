from __future__ import annotations
import joblib
import numpy as np
from dataclasses import dataclass
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score

from .config import RANDOM_SEED

@dataclass
class TrainResult:
    model: RandomForestRegressor
    mae: float
    r2: float
    y_true: np.ndarray
    y_pred: np.ndarray
    feature_names: list[str]

def train_random_forest(X, y) -> TrainResult:
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=RANDOM_SEED
    )

    model = RandomForestRegressor(
        n_estimators=400,
        random_state=RANDOM_SEED,
        n_jobs=-1,
        max_depth=None,
        min_samples_leaf=2,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    mae = float(mean_absolute_error(y_test, y_pred))
    r2 = float(r2_score(y_test, y_pred))

    return TrainResult(
        model=model,
        mae=mae,
        r2=r2,
        y_true=y_test.to_numpy(),
        y_pred=y_pred,
        feature_names=list(X.columns),
    )

def save_model(model, path: str) -> None:
    joblib.dump(model, path)

def load_model(path: str):
    return joblib.load(path)
