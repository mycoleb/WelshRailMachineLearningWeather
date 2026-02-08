import os
import csv
import json
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

import requests


HSP_METRICS_URL = "https://hsp-prod.rockshore.net/api/v1/serviceMetrics"
HSP_DETAILS_URL = "https://hsp-prod.rockshore.net/api/v1/serviceDetails"

print("Running fetchhsp.py")
def env_creds():
    user = os.getenv("HSP_USER")
    pw = os.getenv("HSP_PASS")
    if not user or not pw:
        raise SystemExit(
            "Missing HSP_USER / HSP_PASS env vars. Set them in PowerShell:\n"
            '  setx HSP_USER "YOUR_USERNAME"\n'
            '  setx HSP_PASS "YOUR_PASSWORD"\n'
            "Then restart your terminal."
        )
    return user, pw


import time
import requests

import time, random
import requests
import json

def post_json(url: str, payload: dict, auth, timeout=120, retries=6) -> dict:
    retry_statuses = {502, 503, 504}
    for attempt in range(1, retries + 1):
        print("This is retry number"+retries)
        try:
            print("try loop started")
            r = requests.post(url, json=payload, auth=auth, timeout=timeout)

            # Retryable server errors
            if r.status_code in retry_statuses:
                wait = min(2 ** attempt, 60) + random.uniform(0, 1.5)
                print(f"[WARN] HSP {r.status_code} (attempt {attempt}/{retries}). Waiting {wait:.1f}s then retry...")
                time.sleep(wait)
                continue

            # Non-retryable errors: print body then raise
            if r.status_code >= 400:
                print("\n=== HSP ERROR ===")
                print("URL:", url)
                print("Status:", r.status_code)
                print("Request payload:", json.dumps(payload, indent=2))
                print("Response text:", r.text[:2000])
                print("=== END HSP ERROR ===\n")
                r.raise_for_status()

            return r.json()

        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            wait = min(2 ** attempt, 60) + random.uniform(0, 1.5)
            print(f"[WARN] Network/timeout (attempt {attempt}/{retries}). Waiting {wait:.1f}s then retry...")
            time.sleep(wait)

    raise RuntimeError("HSP failed after retries (server 5xx / timeouts). Try smaller time windows or later.")



def parse_hhmm(hhmm: Optional[str]) -> Optional[int]:
    """Convert 'HHMM' to minutes since midnight."""
    if not hhmm:
        return None
    hhmm = hhmm.strip()
    if len(hhmm) != 4 or not hhmm.isdigit():
        return None
    hh = int(hhmm[:2])
    mm = int(hhmm[2:])
    return hh * 60 + mm


def compute_delay_minutes(sched_hhmm: Optional[str], actual_hhmm: Optional[str]) -> Optional[float]:
    """Delay = actual - scheduled (minutes). Handles midnight wrap roughly."""
    s = parse_hhmm(sched_hhmm)
    a = parse_hhmm(actual_hhmm)
    if s is None or a is None:
        return None
    d = a - s
    # handle wrap across midnight (e.g. 23:55 -> 00:10 = +15)
    if d < -600:
        d += 1440
    if d > 600:
        d -= 1440
    return float(d)


def extract_delay_from_service_details(details: Dict[str, Any], station_crs: str) -> Optional[Dict[str, Any]]:
    """
    Find the calling point for station_crs and compute delay at arrival (preferred) or departure.
    HSP details include 'serviceAttributes' and 'locations' style fields; structure varies slightly.
    """
    # Typical HSP response has a top-level list under "serviceAttributesDetails" or "serviceDetails"
    # We'll robustly scan for a list of location/calling points.
    locations = None

    # Common patterns seen in HSP docs/implementations:
    for key in ["serviceAttributesDetails", "serviceDetails", "details"]:
        if key in details and isinstance(details[key], dict):
            for lk in ["locations", "location", "callingPoints"]:
                if lk in details[key] and isinstance(details[key][lk], list):
                    locations = details[key][lk]
                    break

    if locations is None:
        # fallback: find first list of dicts containing CRS-like keys
        for v in details.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                locations = v
                break

    if not locations:
        return None

    # Search for matching CRS
    match = None
    for loc in locations:
        crs = loc.get("crs") or loc.get("crsCode") or loc.get("stationCode")
        if crs == station_crs:
            match = loc
            break

    if not match:
        return None

    # Scheduled vs actual times vary: gbttBookedArrival/Departure vs actual
    sched_arr = match.get("gbttBookedArrival") or match.get("gbtt_pta") or match.get("pta")
    actual_arr = match.get("actualArrival") or match.get("actual_ta") or match.get("arr")

    sched_dep = match.get("gbttBookedDeparture") or match.get("gbtt_ptd") or match.get("ptd")
    actual_dep = match.get("actualDeparture") or match.get("actual_td") or match.get("dep")

    delay = compute_delay_minutes(sched_arr, actual_arr)
    used = "arrival"
    if delay is None:
        delay = compute_delay_minutes(sched_dep, actual_dep)
        used = "departure"

    if delay is None:
        return None

    # Event time: prefer actual arrival/departure with service date; fallback to service run date
    service_date = details.get("serviceDate") or details.get("runDate") or details.get("date")
    # We'll just output ISO date + HH:MM for event_time for now
    t_hhmm = actual_arr or actual_dep or sched_arr or sched_dep
    if not service_date or not t_hhmm:
        return None

    # Build ISO-ish timestamp (UTC unknown; treat as local UK time for now)
    event_time = f"{service_date}T{t_hhmm[:2]}:{t_hhmm[2:]}:00"

    return {
        "event_time": event_time,
        "station_name": station_crs,
        "delay_minutes": delay,
        "delay_basis": used,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-crs", required=True, help="Origin CRS (e.g. CDF)")
    ap.add_argument("--to-crs", required=True, help="Destination CRS (e.g. SWA)")
    ap.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="End date YYYY-MM-DD (inclusive)")
    ap.add_argument("--out", default="data/raw/rail_delays_wales.csv")
    ap.add_argument("--max-rids", type=int, default=500, help="Limit services for demo/testing")
    args = ap.parse_args()

    user, pw = env_creds()
    auth = (user, pw)

    # HSP serviceMetrics payload pattern (see HSP docs)
    all_services = []
    for day_type in ["WEEKDAY"]:
        payload = {
            "from_loc": args.from_crs,
            "to_loc": args.to_crs,
            "from_time": "0600",
            "to_time": "2200",

            "from_date": args.start,
            "to_date": args.end,
            "days": day_type,
        }
        metrics = post_json(HSP_METRICS_URL, payload, auth=auth)

        services = None
        for k in ["Services", "services", "serviceMetrics", "ServiceMetrics"]:
            if k in metrics and isinstance(metrics[k], list):
                services = metrics[k]
                break
        if services is None:
            for v in metrics.values():
                if isinstance(v, list):
                    services = v
                    break

        if services:
            all_services.extend(services)

    services = all_services

    # Extract RIDs
    rids = []
    for s in services:
        rid = s.get("rid") or s.get("RID") or s.get("serviceRid")
        if rid:
            rids.append(rid)

    rids = rids[: args.max_rids]
    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    rows = []
    for rid in rids:
        det_payload = {"rid": rid}
        details = post_json(HSP_DETAILS_URL, det_payload, auth=auth)
        row = extract_delay_from_service_details(details, station_crs=args.to_crs)
        if row:
            rows.append(row)

    # Write CSV
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["event_time", "station_name", "delay_minutes", "delay_basis"])
        w.writeheader()
        w.writerows(rows)

    print(f"[OK] Wrote {len(rows)} rows to {args.out}")
    print("Next: add lat/lon for stations (or join via a station lookup), then run make_features.py")


if __name__ == "__main__":
    main()
