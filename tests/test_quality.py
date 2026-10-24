import pytest

from conftest import make_raw_row
from taxi_pipeline.quality import (
    DataQualityError,
    assert_quality,
    check_not_null,
    check_unique,
    run_quality_checks,
)
from taxi_pipeline.transform import add_zones, clean, enrich, standardize


def _trips(raw_df, rows):
    return add_zones(enrich(clean(standardize(raw_df(rows)))))


def test_clean_data_passes_all_checks(raw_df):
    results = run_quality_checks(_trips(raw_df, [make_raw_row(), make_raw_row(VendorID="2")]))
    assert all(r.passed for r in results), [r for r in results if not r.passed]
    assert_quality(results)  # does not raise


def test_empty_dataset_fails(raw_df):
    results = run_quality_checks(_trips(raw_df, [make_raw_row(fare_amount="-1")]))
    with pytest.raises(DataQualityError, match="not_empty"):
        assert_quality(results)


def test_not_null_and_unique_detect_problems(spark):
    df = spark.createDataFrame([("a", 1), ("a", None)], ["id", "value"])
    assert not check_unique(df, "id").passed
    (result,) = check_not_null(df, ["value"])
    assert not result.passed and result.details == "1 null values"
