"""Readers for raw trip data and the taxi zone lookup table."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_trips(spark: SparkSession, path: str, fmt: str = "parquet") -> DataFrame:
    """Read raw trips. CSV columns are read as strings and typed later in `standardize`."""
    if fmt == "parquet":
        return spark.read.parquet(path)
    if fmt == "csv":
        return spark.read.option("header", True).csv(path)
    raise ValueError(f"Unsupported input format: {fmt!r} (use 'parquet' or 'csv')")


def read_zones(spark: SparkSession, path: str) -> DataFrame:
    """Read the TLC taxi zone lookup CSV (LocationID, Borough, Zone, service_zone)."""
    return (
        spark.read.option("header", True)
        .csv(path)
        .select(
            F.col("LocationID").cast("int").alias("location_id"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone"),
        )
    )
