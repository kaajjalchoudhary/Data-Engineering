"""Generate a small synthetic dataset in the TLC yellow-taxi CSV layout.

Lets the pipeline run end to end without downloading anything. Some records
are deliberately broken (negative fares, time travel, duplicates...) so the
cleaning and quality steps have something to do.

    python -m taxi_pipeline.sample_data --rows 5000 --out data/sample
"""

import argparse
import csv
import os
import random
from datetime import datetime, timedelta

from taxi_pipeline.schema import COLUMNS

BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR"]
FMT = "%Y-%m-%d %H:%M:%S"


def _trip(rng: random.Random, start: datetime) -> dict:
    pickup = start + timedelta(seconds=rng.randint(0, 31 * 24 * 3600 - 1))
    distance = round(rng.uniform(0.3, 18.0), 2)
    minutes = max(2.0, distance * rng.uniform(2.5, 6.0))
    dropoff = pickup + timedelta(minutes=minutes)
    fare = round(3.0 + distance * 2.5 + minutes * 0.5, 2)
    payment = rng.choices([1, 2, 3, 4], weights=[70, 25, 3, 2])[0]
    tip = round(fare * rng.uniform(0.1, 0.3), 2) if payment == 1 else 0.0
    extra, mta, surcharge, congestion = 1.0, 0.5, 1.0, 2.5
    tolls = 6.94 if rng.random() < 0.05 else 0.0
    return {
        "VendorID": rng.choice([1, 2]),
        "tpep_pickup_datetime": pickup.strftime(FMT),
        "tpep_dropoff_datetime": dropoff.strftime(FMT),
        "passenger_count": rng.choices([1, 2, 3, 4, 5, ""], weights=[60, 20, 8, 5, 4, 3])[0],
        "trip_distance": distance,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
        "PULocationID": rng.randint(1, 40),
        "DOLocationID": rng.randint(1, 40),
        "payment_type": payment,
        "fare_amount": fare,
        "extra": extra,
        "mta_tax": mta,
        "tip_amount": tip,
        "tolls_amount": tolls,
        "improvement_surcharge": surcharge,
        "total_amount": round(fare + extra + mta + tip + tolls + surcharge + congestion, 2),
        "congestion_surcharge": congestion,
        "Airport_fee": 0.0,
    }


def _corrupt(rng: random.Random, row: dict) -> dict:
    row = dict(row)
    kind = rng.choice(
        ["negative_fare", "time_travel", "zero_distance", "null_location", "outside_period"]
    )
    if kind == "negative_fare":
        row["fare_amount"] = -abs(row["fare_amount"])
        row["total_amount"] = -abs(row["total_amount"])
    elif kind == "time_travel":
        row["tpep_dropoff_datetime"], row["tpep_pickup_datetime"] = (
            row["tpep_pickup_datetime"],
            row["tpep_dropoff_datetime"],
        )
    elif kind == "zero_distance":
        row["trip_distance"] = 0.0
    elif kind == "null_location":
        row["PULocationID"] = ""
    else:
        row["tpep_pickup_datetime"] = "2009-01-01 00:00:00"
        row["tpep_dropoff_datetime"] = "2009-01-01 00:20:00"
    return row


def generate(out_dir: str, rows: int = 5000, seed: int = 42, bad_ratio: float = 0.05) -> dict:
    rng = random.Random(seed)
    start = datetime(2024, 1, 1)
    os.makedirs(out_dir, exist_ok=True)

    records = []
    for _ in range(rows):
        row = _trip(rng, start)
        records.append(_corrupt(rng, row) if rng.random() < bad_ratio else row)
    records += rng.sample(records, k=max(1, rows // 100))  # exact duplicates

    trips_path = os.path.join(out_dir, "yellow_tripdata_sample.csv")
    with open(trips_path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(COLUMNS))
        writer.writeheader()
        writer.writerows(records)

    zones_path = os.path.join(out_dir, "taxi_zone_lookup_sample.csv")
    with open(zones_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["LocationID", "Borough", "Zone", "service_zone"])
        for loc in range(1, 41):
            writer.writerow(
                [loc, BOROUGHS[loc % len(BOROUGHS)], f"Synthetic Zone {loc:02d}", "Yellow Zone"]
            )

    return {"trips": trips_path, "zones": zones_path, "rows": len(records)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--out", default="data/sample")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    info = generate(args.out, args.rows, args.seed)
    print(f"Wrote {info['rows']} trips to {info['trips']} and zones to {info['zones']}")
