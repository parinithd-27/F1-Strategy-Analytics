"""Pure-Python scorer for the calibrated logistic-regression deployment model (Step 9c, for S12).

Everything is read from ``lr/scorer.json`` (written by ``model.deploy``); this module imports
neither Spark nor the feature registry, so the streaming hot path scores a candidate pair without a
JVM round trip. It reproduces, step by step, what the Spark pipeline does in training:

1. **assemble** (``dataset.feature_exprs`` + ``VectorAssembler(handleInvalid="error")``):
   a numeric input becomes ``value`` (``0.0`` when missing) and ``value_missing`` (``1.0`` when
   missing); a categorical input becomes a one-hot over its categories plus ``unknown`` (missing or
   unseen value). NaN or infinite numeric values raise, as Spark's assembler does;
2. **scale** (``StandardScaler(withMean=True, withStd=True)``): ``(x - mean) * (1 / std)``, and
   ``0.0`` where ``std == 0`` (Spark's rule);
3. **logistic regression**: ``sigmoid(coefficients . z + intercept)``;
4. **calibrate** (``calibrate.Calibrator.apply``): Platt ``sigmoid(a * logit(clip(p)) + b)`` with
   ``clip`` to ``[eps, 1 - eps]``, or identity.

Equality with Spark (within 1e-9) is tested in ``tests/unit/test_lr_scorer.py``.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCORER_FORMAT = "f1sa-lr-scorer-v1"
CALIBRATION_METHODS = ("identity", "platt")


class ScorerError(ValueError):
    """The scorer JSON is inconsistent, or an input cannot be scored."""


@dataclass(frozen=True)
class InputSpec:
    """One model input: ``numeric`` (value + missing indicator) or ``categorical`` (one-hot)."""

    name: str
    kind: str
    categories: tuple[str, ...] = ()

    @property
    def width(self) -> int:
        return 2 if self.kind == "numeric" else len(self.categories) + 1


def _sigmoid(z: float) -> float:
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    e = math.exp(z)
    return e / (1.0 + e)


@dataclass(frozen=True)
class LRScorer:
    inputs: tuple[InputSpec, ...]
    assembled_columns: tuple[str, ...]
    mean: tuple[float, ...]
    std: tuple[float, ...]
    coefficients: tuple[float, ...]
    intercept: float
    calibration_method: str
    calibration_params: Mapping[str, float]
    eps: float

    def __post_init__(self) -> None:
        n = len(self.assembled_columns)
        if sum(s.width for s in self.inputs) != n:
            raise ScorerError(
                f"inputs expand to {sum(s.width for s in self.inputs)} columns, "
                f"assembled_columns has {n}"
            )
        for name, vec in (("mean", self.mean), ("std", self.std), ("coef", self.coefficients)):
            if len(vec) != n:
                raise ScorerError(f"{name} has {len(vec)} entries, expected {n}")
        for s in self.inputs:
            if s.kind not in ("numeric", "categorical"):
                raise ScorerError(f"{s.name}: unknown kind {s.kind!r}")
            if s.kind == "categorical" and not s.categories:
                raise ScorerError(f"{s.name}: categorical input without categories")
        if self.calibration_method not in CALIBRATION_METHODS:
            raise ScorerError(
                f"calibration {self.calibration_method!r} not supported by the scorer "
                f"(supported: {CALIBRATION_METHODS})"
            )
        if self.calibration_method == "platt" and not {"a", "b"} <= set(self.calibration_params):
            raise ScorerError("platt calibration needs parameters 'a' and 'b'")
        if not 0.0 < self.eps < 0.5:
            raise ScorerError(f"eps must be in (0, 0.5), got {self.eps}")

    # ------------------------------------------------------------------ (de)serialisation
    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> LRScorer:
        if data.get("format") != SCORER_FORMAT:
            raise ScorerError(f"not a {SCORER_FORMAT} document (format={data.get('format')!r})")
        cal = data["calibrator"]
        return cls(
            inputs=tuple(
                InputSpec(str(s["name"]), str(s["kind"]), tuple(s.get("categories", ())))
                for s in data["inputs"]
            ),
            assembled_columns=tuple(str(c) for c in data["assembled_columns"]),
            mean=tuple(float(x) for x in data["scaler_mean"]),
            std=tuple(float(x) for x in data["scaler_std"]),
            coefficients=tuple(float(x) for x in data["coefficients"]),
            intercept=float(data["intercept"]),
            calibration_method=str(cal["method"]),
            calibration_params={str(k): float(v) for k, v in dict(cal.get("params", {})).items()},
            eps=float(data["eps"]),
        )

    @classmethod
    def load(cls, path: Path | str) -> LRScorer:
        return cls.from_json(json.loads(Path(path).read_text(encoding="utf-8")))

    # ------------------------------------------------------------------ scoring
    def assemble(self, row: Mapping[str, Any]) -> list[float]:
        """The assembled feature vector of ``row`` (raw input names -> values; absent = missing)."""
        out: list[float] = []
        for spec in self.inputs:
            value = row.get(spec.name)
            if spec.kind == "numeric":
                if value is None:
                    out += [0.0, 1.0]
                    continue
                if isinstance(value, bool):
                    raise ScorerError(f"{spec.name}: boolean is not a numeric input")
                x = float(value)
                if not math.isfinite(x):
                    raise ScorerError(
                        f"{spec.name}: non-finite value {x!r} (Spark's VectorAssembler rejects it)"
                    )
                out += [x, 0.0]
            else:
                hits = [1.0 if value is not None and value == c else 0.0 for c in spec.categories]
                out += [*hits, 0.0 if any(hits) else 1.0]
        return out

    def margin(self, vector: Sequence[float]) -> float:
        if len(vector) != len(self.coefficients):
            n = len(self.coefficients)
            raise ScorerError(f"vector has {len(vector)} entries, expected {n}")
        total = 0.0
        for x, m, s, c in zip(vector, self.mean, self.std, self.coefficients, strict=True):
            z = (x - m) * (1.0 / s) if s != 0.0 else 0.0
            total += c * z
        return total + self.intercept

    def raw_probability(self, row: Mapping[str, Any]) -> float:
        """P(success) of the uncalibrated LR (Spark's ``probability[1]``)."""
        return _sigmoid(self.margin(self.assemble(row)))

    def calibrate(self, p: float) -> float:
        if self.calibration_method == "identity":
            return p
        c = min(max(p, self.eps), 1.0 - self.eps)
        logit = math.log(c / (1.0 - c))
        a, b = self.calibration_params["a"], self.calibration_params["b"]
        return _sigmoid(a * logit + b)

    def probability(self, row: Mapping[str, Any]) -> float:
        """Calibrated P(success): the deployed model's output."""
        return self.calibrate(self.raw_probability(row))
