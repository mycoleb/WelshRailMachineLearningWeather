import argparse
import pandas as pd

from src.join_weather_rail import join_rail_with_weather
from src.io_schema import RAIL, WEATHER

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rail", required=True)
    ap.add_argument("--weather", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--time-tol-min", type=int, default=None)
    ap.add_argument("--max-dist-km", type=float, default=None)
    args = ap.parse_args()

    rail = pd.read_csv(args.rail)
    weather = pd.read_csv(args.weather)

    joined, stats = join_rail_with_weather(
        rail,
        weather,
        time_tolerance_minutes=args.time_tol_min if args.time_tol_min is not None else 60,
        max_station_distance_km=args.max_dist_km,
    )

    print("JOIN STATS:", stats)

    # Save as parquet for speed + types
    joined.to_parquet(args.out, index=False)
    print(f"Wrote: {args.out} rows={len(joined)}")

if __name__ == "__main__":
    main()
