"""Column mapping from the raw NYC TLC yellow-taxi layout to the clean schema."""

# raw column name -> (clean column name, Spark SQL type)
COLUMNS = {
    "VendorID": ("vendor_id", "int"),
    "tpep_pickup_datetime": ("pickup_ts", "timestamp"),
    "tpep_dropoff_datetime": ("dropoff_ts", "timestamp"),
    "passenger_count": ("passenger_count", "int"),
    "trip_distance": ("trip_distance", "double"),
    "RatecodeID": ("rate_code_id", "int"),
    "store_and_fwd_flag": ("store_and_fwd_flag", "string"),
    "PULocationID": ("pickup_location_id", "int"),
    "DOLocationID": ("dropoff_location_id", "int"),
    "payment_type": ("payment_type", "int"),
    "fare_amount": ("fare_amount", "double"),
    "extra": ("extra", "double"),
    "mta_tax": ("mta_tax", "double"),
    "tip_amount": ("tip_amount", "double"),
    "tolls_amount": ("tolls_amount", "double"),
    "improvement_surcharge": ("improvement_surcharge", "double"),
    "total_amount": ("total_amount", "double"),
    "congestion_surcharge": ("congestion_surcharge", "double"),
    "Airport_fee": ("airport_fee", "double"),
}

CLEAN_COLUMNS = [dst for dst, _ in COLUMNS.values()]

PAYMENT_TYPES = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}
