import os
import argparse
import pandas as pd
import requests

CEDA_BASE = "https://dap.ceda.ac.uk/badc/ukmo-midas-open/data"

def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise SystemExit(f"Missing env var {name}. Set it and restart terminal.")
    return v

def download_file(url: str, out_path: str, auth) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    if os.path.exists(out_path):
        return
    r = requests.get(url, auth=auth, stream=True, timeout=120)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="uk-hourly-weather-obs", help="MIDAS dataset folder")
    ap.add_argument("--version", default="dataset-version-202007", help="MIDAS dataset version folder")
    ap.add_argument("--qc", default="qc-version-1", help="qc-version-0 or qc-version-1")
    ap.add_argument("--stations", required=True, help="Comma-separated MIDAS src_id list, e.g. 123,456")
    ap.add_argument("--county", required=True, help="historic_county folder name in MIDAS path")
    ap.add_argument("--years", required=True, help="Comma-separated years, e.g. 2023,2024")
    ap.add_argument("--out", default="data/raw/metoffice_weather_wales.csv")
    ap.add_argument("--props", default="air_temperature,wind_speed,precipitation_amount",
                    help="Comma-separated column names to keep if present")
    args = ap.parse_args()

    user = require_env("CEDA_USER")
    pw = require_env("CEDA_PASSWORD")
    auth = (user, pw)

    stations = [s.strip() for s in args.stations.split(",") if s.strip()]
    years = [int(y.strip()) for y in args.years.split(",") if y.strip()]
    props = [p.strip() for p in args.props.split(",") if p.strip()]

    frames = []

    for sid in stations:
        # MIDAS uses 5-digit padded station id folder prefix like 00009_...
        # The Julia script used stationList.csv to map src_id -> station_file_name.
        # In Python, you should also use station metadata to get the station folder name.
        #
        # For now, this script expects you to provide the *full station folder name* once you know it.
        # See note below on how to get it automatically.
        raise SystemExit(
            "This script needs the station folder name mapping (src_id -> station folder). "
            "Next step below shows how to fetch station metadata and do this automatically."
        )

    # If you implement mapping, you'd concatenate frames and write args.out.

if __name__ == "__main__":
    main()
