"""Writers for curated outputs."""

import json
import os
from dataclasses import asdict
from typing import List, Optional, Sequence

from pyspark.sql import DataFrame

from taxi_pipeline.quality import CheckResult


def write_parquet(
    df: DataFrame,
    path: str,
    partition_by: Optional[Sequence[str]] = None,
    mode: str = "overwrite",
) -> None:
    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)


def write_quality_report(results: List[CheckResult], output_dir: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "quality_report.json")
    with open(path, "w") as fh:
        json.dump([asdict(r) for r in results], fh, indent=2)
    return path
