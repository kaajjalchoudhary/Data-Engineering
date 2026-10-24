from datetime import date

from conftest import make_raw_row
from taxi_pipeline.aggregates import daily_summary, payment_mix
from taxi_pipeline.transform import add_zones, clean, enrich, standardize


def test_standardize_renames_and_casts(raw_df):
    df = standardize(raw_df([make_raw_row()]))
    types = dict(df.dtypes)
    assert types["pickup_ts"] == "timestamp"
    assert types["pickup_location_id"] == "int"
    assert types["fare_amount"] == "double"
    assert "PULocationID" not in df.columns


def test_standardize_adds_missing_columns_as_null(spark):
    df = standardize(spark.createDataFrame([("1",)], ["VendorID"]))
    assert df.first()["airport_fee"] is None


def test_clean_drops_invalid_and_duplicate_rows(raw_df):
    rows = [
        make_raw_row(),
        make_raw_row(),  # exact duplicate
        make_raw_row(fare_amount="-5.0"),
        make_raw_row(tpep_dropoff_datetime="2024-01-15 07:00:00"),  # before pickup
        make_raw_row(PULocationID=None),
        make_raw_row(trip_distance="0"),
        make_raw_row(trip_distance="150.0"),  # 300 mph
        make_raw_row(passenger_count=None, VendorID="2"),  # null -> defaults to 1, kept
    ]
    result = clean(standardize(raw_df(rows))).collect()
    assert len(result) == 2
    assert all(r["passenger_count"] == 1 for r in result)


def test_clean_respects_reporting_period(raw_df):
    rows = [
        make_raw_row(),
        make_raw_row(
            tpep_pickup_datetime="2009-01-01 00:00:00", tpep_dropoff_datetime="2009-01-01 00:20:00"
        ),
    ]
    result = clean(standardize(raw_df(rows)), "2024-01-01", "2024-02-01")
    assert result.count() == 1


def test_enrich_derives_metrics(raw_df):
    row = enrich(clean(standardize(raw_df([make_raw_row()])))).first()
    assert row["trip_duration_min"] == 30.0
    assert row["avg_speed_mph"] == 20.0
    assert row["tip_pct"] == 20.0
    assert row["payment_type_desc"] == "Credit card"
    assert row["time_of_day"] == "morning"
    assert row["pickup_date"] == date(2024, 1, 15)
    assert row["pickup_day_of_week"] == "Mon"
    assert row["is_weekend"] is False
    assert len(row["trip_id"]) == 64


def test_add_zones_joins_lookup(spark, raw_df):
    zones = spark.createDataFrame(
        [(1, "Manhattan", "Midtown"), (2, "Queens", "Astoria")], ["location_id", "borough", "zone"]
    )
    row = add_zones(enrich(clean(standardize(raw_df([make_raw_row()])))), zones).first()
    assert row["pickup_zone"] == "Midtown"
    assert row["dropoff_borough"] == "Queens"


def test_daily_summary_and_payment_mix(raw_df):
    rows = [make_raw_row(), make_raw_row(VendorID="2", payment_type="2", tip_amount="0.0")]
    trips = add_zones(enrich(clean(standardize(raw_df(rows)))))

    day = daily_summary(trips).first()
    assert day["trip_count"] == 2
    assert day["total_revenue"] == 34.0

    mix = {r["payment_type_desc"]: r["share_of_trips_pct"] for r in payment_mix(trips).collect()}
    assert mix == {"Credit card": 50.0, "Cash": 50.0}
