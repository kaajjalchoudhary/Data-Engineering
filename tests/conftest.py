import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from taxi_pipeline.schema import COLUMNS


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("taxi-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_raw_row(**overrides):
    """A valid raw trip (as CSV strings) with optional overrides."""
    row = {
        "VendorID": "1",
        "tpep_pickup_datetime": "2024-01-15 08:00:00",
        "tpep_dropoff_datetime": "2024-01-15 08:30:00",
        "passenger_count": "1",
        "trip_distance": "10.0",
        "RatecodeID": "1",
        "store_and_fwd_flag": "N",
        "PULocationID": "1",
        "DOLocationID": "2",
        "payment_type": "1",
        "fare_amount": "10.0",
        "extra": "1.0",
        "mta_tax": "0.5",
        "tip_amount": "2.0",
        "tolls_amount": "0.0",
        "improvement_surcharge": "1.0",
        "total_amount": "17.0",
        "congestion_surcharge": "2.5",
        "Airport_fee": "0.0",
    }
    row.update(overrides)
    return row


@pytest.fixture
def raw_df(spark):
    def _build(rows):
        schema = StructType([StructField(c, StringType(), True) for c in COLUMNS])
        return spark.createDataFrame([tuple(r[c] for c in COLUMNS) for r in rows], schema)

    return _build
