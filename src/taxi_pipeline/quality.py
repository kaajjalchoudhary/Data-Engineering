"""Lightweight data quality checks that run before anything is written."""

from dataclasses import dataclass
from typing import Iterable, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(Exception):
    """Raised when one or more error-level checks fail."""


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str
    severity: str = "error"  # "error" fails the run, "warning" is only logged


def check_not_empty(df: DataFrame) -> CheckResult:
    count = df.limit(1).count()
    return CheckResult("not_empty", count > 0, "dataset has rows" if count else "dataset is empty")


def check_not_null(df: DataFrame, columns: Iterable[str]) -> List[CheckResult]:
    columns = list(columns)
    nulls = df.select(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]).first()
    return [
        CheckResult(f"not_null:{c}", (nulls[c] or 0) == 0, f"{nulls[c] or 0} null values")
        for c in columns
    ]


def check_unique(df: DataFrame, column: str) -> CheckResult:
    row = df.agg(F.count(column).alias("total"), F.countDistinct(column).alias("distinct")).first()
    dupes = row["total"] - row["distinct"]
    return CheckResult(f"unique:{column}", dupes == 0, f"{dupes} duplicate values")


def check_range(
    df: DataFrame, column: str, min_value: float, max_value: float, severity: str = "error"
) -> CheckResult:
    bad = df.filter(~F.col(column).between(min_value, max_value)).count()
    return CheckResult(
        f"range:{column}",
        bad == 0,
        f"{bad} values outside [{min_value}, {max_value}]",
        severity,
    )


def run_quality_checks(trips: DataFrame) -> List[CheckResult]:
    results = [check_not_empty(trips)]
    results += check_not_null(
        trips, ["trip_id", "pickup_ts", "dropoff_ts", "pickup_location_id", "total_amount"]
    )
    results.append(check_unique(trips, "trip_id"))
    results.append(check_range(trips, "trip_duration_min", 0, 240))
    results.append(check_range(trips, "passenger_count", 1, 9))
    results.append(check_range(trips, "tip_pct", 0, 100, severity="warning"))
    return results


def assert_quality(results: List[CheckResult]) -> None:
    failed = [r for r in results if not r.passed and r.severity == "error"]
    if failed:
        summary = "; ".join(f"{r.name} ({r.details})" for r in failed)
        raise DataQualityError(f"{len(failed)} data quality check(s) failed: {summary}")
