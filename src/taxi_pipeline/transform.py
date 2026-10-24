"""Cleaning and enrichment transformations for taxi trips."""

from itertools import chain
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from taxi_pipeline.schema import CLEAN_COLUMNS, COLUMNS, PAYMENT_TYPES

MAX_TRIP_MINUTES = 240
MAX_TRIP_MILES = 200
MAX_SPEED_MPH = 100


def standardize(df: DataFrame) -> DataFrame:
    """Rename raw TLC columns to snake_case and cast to the target types.

    Column matching is case-insensitive (TLC files use both `Airport_fee` and
    `airport_fee`). Columns missing from older files are added as nulls.
    """
    available = {c.lower(): c for c in df.columns}
    selected = []
    for src, (dst, dtype) in COLUMNS.items():
        if src.lower() in available:
            selected.append(F.col(available[src.lower()]).cast(dtype).alias(dst))
        else:
            selected.append(F.lit(None).cast(dtype).alias(dst))
    return df.select(*selected)


def clean(
    df: DataFrame,
    period_start: Optional[str] = None,
    period_end: Optional[str] = None,
) -> DataFrame:
    """Remove duplicates and records that are physically or financially impossible.

    `period_start` / `period_end` (YYYY-MM-DD, end exclusive) drop trips whose
    pickup falls outside the file's reporting period, which TLC data contains.
    """
    duration_min = (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0

    out = (
        df.dropDuplicates(CLEAN_COLUMNS)
        .dropna(subset=["pickup_ts", "dropoff_ts", "pickup_location_id", "dropoff_location_id"])
        .withColumn("passenger_count", F.coalesce(F.col("passenger_count"), F.lit(1)))
        .withColumn("trip_duration_min", F.round(duration_min, 2))
        .filter((F.col("trip_duration_min") > 0) & (F.col("trip_duration_min") <= MAX_TRIP_MINUTES))
        .filter((F.col("trip_distance") > 0) & (F.col("trip_distance") <= MAX_TRIP_MILES))
        .filter((F.col("fare_amount") >= 0) & (F.col("total_amount") >= 0))
        .filter(F.col("passenger_count").between(1, 9))
        .filter(F.col("trip_distance") / (F.col("trip_duration_min") / 60.0) <= MAX_SPEED_MPH)
    )
    if period_start:
        out = out.filter(F.col("pickup_ts") >= F.to_timestamp(F.lit(period_start)))
    if period_end:
        out = out.filter(F.col("pickup_ts") < F.to_timestamp(F.lit(period_end)))
    return out


def enrich(df: DataFrame) -> DataFrame:
    """Add a surrogate key, calendar attributes and derived trip metrics."""
    payment_map = F.create_map(*[F.lit(x) for x in chain(*PAYMENT_TYPES.items())])
    hour = F.hour("pickup_ts")

    return (
        df.withColumn(
            "trip_id",
            F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in CLEAN_COLUMNS]), 256),
        )
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_year", F.year("pickup_ts"))
        .withColumn("pickup_month", F.month("pickup_ts"))
        .withColumn("pickup_hour", hour)
        .withColumn("pickup_day_of_week", F.date_format("pickup_ts", "E"))
        .withColumn("is_weekend", F.dayofweek("pickup_ts").isin(1, 7))
        .withColumn(
            "time_of_day",
            F.when(hour.between(6, 11), "morning")
            .when(hour.between(12, 16), "afternoon")
            .when(hour.between(17, 21), "evening")
            .otherwise("night"),
        )
        .withColumn(
            "avg_speed_mph",
            F.round(F.col("trip_distance") / (F.col("trip_duration_min") / 60.0), 2),
        )
        .withColumn(
            "tip_pct",
            F.when(
                F.col("fare_amount") > 0,
                F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2),
            ),
        )
        .withColumn(
            "payment_type_desc",
            F.coalesce(F.element_at(payment_map, F.col("payment_type")), F.lit("Unknown")),
        )
    )


def add_zones(df: DataFrame, zones: Optional[DataFrame] = None) -> DataFrame:
    """Attach pickup/dropoff borough and zone names via broadcast joins.

    Without a lookup table the columns are still created (as nulls) so the
    downstream schema stays stable.
    """
    if zones is None:
        for col in ["pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone"]:
            df = df.withColumn(col, F.lit(None).cast("string"))
        return df

    pickup = zones.select(
        F.col("location_id").alias("pickup_location_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone"),
    )
    dropoff = zones.select(
        F.col("location_id").alias("dropoff_location_id"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone").alias("dropoff_zone"),
    )
    return df.join(F.broadcast(pickup), "pickup_location_id", "left").join(
        F.broadcast(dropoff), "dropoff_location_id", "left"
    )
