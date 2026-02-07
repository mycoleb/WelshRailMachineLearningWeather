import argparse
import os
import pandas as pd

from src.features import make_xy
from src.model import train_random_forest
from src.viz import plot_actual_vs_pred, plot_feature_importance, plot_residuals
from src.config import TARGET_COL
from src.io_schema import WEATHER

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="Joined parquet from make_features.py")
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    df = pd.read_parquet(args.data)
    X, y = make_xy(df)

    res = train_random_forest(X, y)
    print(f"MAE={res.mae:.3f} minutes, R2={res.r2:.3f}")

    os.makedirs(args.outdir, exist_ok=True)
    plot_actual_vs_pred(res.y_true, res.y_pred, os.path.join(args.outdir, "actual_vs_pred.png"))
    plot_residuals(res.y_true, res.y_pred, os.path.join(args.outdir, "residuals.png"))
    plot_feature_importance(res.model, res.feature_names, os.path.join(args.outdir, "feature_importance.png"))

    # Optional: a “story” plot for one key weather variable if present
    for f in WEATHER["features"]:
        if f in df.columns:
            from src.viz import plot_weather_sensitivity
            plot_weather_sensitivity(df, f, TARGET_COL, os.path.join(args.outdir, "weather_sensitivity.png"))
            break

    print(f"Wrote figures to: {args.outdir}")

if __name__ == "__main__":
    main()
