"""Analytics-ready aggregate tables built from cleaned trips."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def daily_summary(trips: DataFrame) -> DataFrame:
    return (
        trips.groupBy("pickup_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("passenger_count").alias("total_passengers"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
            F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
        )
        .orderBy("pickup_date")
    )


def hourly_demand(trips: DataFrame) -> DataFrame:
    return (
        trips.groupBy("pickup_day_of_week", "pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("avg_speed_mph"), 2).alias("avg_speed_mph"),
        )
        .orderBy("pickup_day_of_week", "pickup_hour")
    )


def zone_performance(trips: DataFrame) -> DataFrame:
    ranking = Window.orderBy(F.desc("total_revenue"))
    return (
        trips.groupBy("pickup_location_id", "pickup_borough", "pickup_zone")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
        )
        .withColumn("revenue_rank", F.dense_rank().over(ranking))
        .orderBy("revenue_rank")
    )


def payment_mix(trips: DataFrame) -> DataFrame:
    everything = Window.partitionBy()
    return (
        trips.groupBy("payment_type_desc")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        )
        .withColumn(
            "share_of_trips_pct",
            F.round(F.col("trip_count") / F.sum("trip_count").over(everything) * 100, 2),
        )
        .orderBy(F.desc("trip_count"))
    )


ALL_AGGREGATES = {
    "daily_summary": daily_summary,
    "hourly_demand": hourly_demand,
    "zone_performance": zone_performance,
    "payment_mix": payment_mix,
}
