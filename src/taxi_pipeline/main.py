"""Pipeline entry point.

Example:
    python -m taxi_pipeline.main --input data/raw/yellow_tripdata_2024-01.parquet \
        --zones data/raw/taxi_zone_lookup.csv --output data/curated \
        --period-start 2024-01-01 --period-end 2024-02-01
"""

import argparse
import logging
import os
import time
from typing import Dict, Optional

from pyspark.sql import SparkSession

from taxi_pipeline.aggregates import ALL_AGGREGATES
from taxi_pipeline.extract import read_trips, read_zones
from taxi_pipeline.load import write_parquet, write_quality_report
from taxi_pipeline.quality import assert_quality, run_quality_checks
from taxi_pipeline.spark_session import get_spark
from taxi_pipeline.transform import add_zones, clean, enrich, standardize

log = logging.getLogger("taxi_pipeline")


def run(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    input_format: str = "parquet",
    zones_path: Optional[str] = None,
    period_start: Optional[str] = None,
    period_end: Optional[str] = None,
) -> Dict[str, int]:
    started = time.time()

    raw = read_trips(spark, input_path, input_format)
    raw_count = raw.count()
    log.info("Read %s raw rows from %s", f"{raw_count:,}", input_path)

    zones = read_zones(spark, zones_path) if zones_path else None
    trips = enrich(add_zones(clean(standardize(raw), period_start, period_end), zones)).cache()
    clean_count = trips.count()
    log.info(
        "Kept %s rows after cleaning (%s dropped)",
        f"{clean_count:,}",
        f"{raw_count - clean_count:,}",
    )

    results = run_quality_checks(trips)
    for r in results:
        log.info("[%s] %-28s %s", "PASS" if r.passed else r.severity.upper(), r.name, r.details)
    write_quality_report(results, output_path)
    assert_quality(results)

    write_parquet(trips, os.path.join(output_path, "trips"), ["pickup_year", "pickup_month"])
    for name, build in ALL_AGGREGATES.items():
        write_parquet(build(trips).coalesce(1), os.path.join(output_path, name))
        log.info("Wrote aggregate %s", name)

    trips.unpersist()
    log.info("Pipeline finished in %.1fs", time.time() - started)
    return {"raw_rows": raw_count, "clean_rows": clean_count}


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="NYC Taxi PySpark batch pipeline")
    p.add_argument("--input", required=True, help="Path to raw trip file(s)")
    p.add_argument("--input-format", default="parquet", choices=["parquet", "csv"])
    p.add_argument("--output", required=True, help="Output directory for curated data")
    p.add_argument("--zones", help="Optional taxi zone lookup CSV")
    p.add_argument("--period-start", help="Keep pickups on/after this date (YYYY-MM-DD)")
    p.add_argument("--period-end", help="Keep pickups before this date (YYYY-MM-DD)")
    return p.parse_args(argv)


def main(argv=None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run(
            spark,
            args.input,
            args.output,
            args.input_format,
            args.zones,
            args.period_start,
            args.period_end,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
