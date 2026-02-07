import argparse
import pandas as pd
import folium

from src.features import make_xy
from src.model import train_random_forest
from src.io_schema import RAIL

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--top-n", type=int, default=300)
    args = ap.parse_args()

    df = pd.read_parquet(args.data)

    # Train quickly on all joined data (simple demo approach)
    X, y = make_xy(df)
    res = train_random_forest(X, y)

    df = df.copy()
    df["pred_delay"] = res.model.predict(X)

    # Choose top predicted delays
    df_top = df.sort_values("pred_delay", ascending=False).head(args.top_n)

    # Center map on Wales-ish mean
    center_lat = float(df_top[RAIL["lat"]].mean())
    center_lon = float(df_top[RAIL["lon"]].mean())
    m = folium.Map(location=[center_lat, center_lon], zoom_start=7)

    for _, r in df_top.iterrows():
        folium.CircleMarker(
            location=[float(r[RAIL["lat"]]), float(r[RAIL["lon"]])],
            radius=5,
            popup=f"{r.get(RAIL['station'], 'station')}<br>pred={r['pred_delay']:.1f} min<br>actual={float(r['delay_minutes']):.1f}",
        ).add_to(m)

    m.save(args.out)
    print(f"Wrote map: {args.out}")

if __name__ == "__main__":
    main()
