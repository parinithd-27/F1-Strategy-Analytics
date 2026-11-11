"""Shared helpers for the model tests: a small grid and the generated rehearsal table."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from f1sa.model import rehearsal
from f1sa.model.train import TrainConfig

SMALL = TrainConfig(
    lr_grid=({"regParam": 0.0, "elasticNetParam": 0.0}, {"regParam": 0.1, "elasticNetParam": 0.0}),
    gbt_grid=(
        {"maxDepth": 2, "maxIter": 10, "stepSize": 0.1},
        {"maxDepth": 3, "maxIter": 10, "stepSize": 0.1},
    ),
)
SIZE: dict[str, Any] = {"races_per_season": 8, "attempts_per_race": 10}


def rehearsal_rows(**kw: Any) -> list[dict[str, Any]]:
    return rehearsal.generate_rows(7, **{**SIZE, **kw})


def frame(spark: SparkSession, rows: list[dict[str, Any]]) -> DataFrame:
    schema = rehearsal.model_input_schema()
    return spark.createDataFrame(
        [tuple(r.get(f.name) for f in schema.fields) for r in rows], schema
    )
