import json
import os

from taxi_pipeline.main import run
from taxi_pipeline.sample_data import generate


def test_pipeline_end_to_end(spark, tmp_path):
    sample = generate(str(tmp_path / "raw"), rows=500, seed=7)
    out = str(tmp_path / "curated")

    metrics = run(
        spark,
        sample["trips"],
        out,
        input_format="csv",
        zones_path=sample["zones"],
        period_start="2024-01-01",
        period_end="2024-02-01",
    )

    assert metrics["raw_rows"] == sample["rows"]
    assert 0 < metrics["clean_rows"] < metrics["raw_rows"]

    for table in ["trips", "daily_summary", "hourly_demand", "zone_performance", "payment_mix"]:
        assert os.path.isdir(os.path.join(out, table)), table

    trips = spark.read.parquet(os.path.join(out, "trips"))
    assert trips.count() == metrics["clean_rows"]
    assert trips.filter("pickup_zone IS NULL").count() == 0

    daily = spark.read.parquet(os.path.join(out, "daily_summary"))
    assert daily.agg({"trip_count": "sum"}).first()[0] == metrics["clean_rows"]

    with open(os.path.join(out, "quality_report.json")) as fh:
        report = json.load(fh)
    assert {r["name"] for r in report} >= {"not_empty", "unique:trip_id"}
