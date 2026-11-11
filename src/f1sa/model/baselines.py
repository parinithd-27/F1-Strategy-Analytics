"""Baselines (#17): majority class and a decision-time rule.

* **Majority class**: always the training majority. Its probability is the training positive rate
  (a constant), so ranking metrics equal the no-skill level and the Brier score is the prior's.
* **Rule**: the attacker's expected fresh-tyre gain before the defender can respond, against the
  gap. With ``k_expected = 1`` (the defender covers on the next lap)::

      gain_s = max(0, defender_deg_slope_s_per_lap) * defender_stint_laps * k_expected
               - attacker_expected_outlap_pace_residual_s   (0 when missing)
      P(success) = sigmoid((gain_s - gap_s) / scale_s)

  It predicts success when ``gap_s < gain_s``. All inputs are decision-time features. When the
  defender's slope or stint length is missing, the rule falls back to the training positive rate.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from f1sa.model.metrics import FloatArray


@dataclass(frozen=True)
class MajorityBaseline:
    positive_rate: float

    @property
    def majority(self) -> int:
        return int(self.positive_rate >= 0.5)

    @classmethod
    def fit(cls, train_labels: Any) -> MajorityBaseline:
        y = np.asarray(train_labels, dtype=np.float64)
        if y.size == 0:
            raise ValueError("cannot fit a baseline on an empty training split")
        return cls(float(y.mean()))

    def predict_proba(self, n: int) -> FloatArray:
        return np.full(n, self.positive_rate, dtype=np.float64)


@dataclass(frozen=True)
class RuleBaseline:
    fallback_rate: float
    k_expected: int = 1
    scale_s: float = 1.0

    def __post_init__(self) -> None:
        if self.k_expected < 1 or self.scale_s <= 0:
            raise ValueError("k_expected must be >= 1 and scale_s > 0")

    def gain_s(self, rows: Any) -> FloatArray:
        """Expected gain per row (nan when the defender's slope or stint length is missing)."""
        slope = np.asarray(rows["defender_deg_slope_s_per_lap"], dtype=np.float64)
        laps = np.asarray(rows["defender_stint_laps"], dtype=np.float64)
        resid = np.nan_to_num(
            np.asarray(rows["attacker_expected_outlap_pace_residual_s"], dtype=np.float64), nan=0.0
        )
        gain: FloatArray = np.maximum(slope, 0.0) * laps * self.k_expected - resid
        return gain

    def predict_proba(self, rows: Any) -> FloatArray:
        gain = self.gain_s(rows)
        gap = np.asarray(rows["gap_s"], dtype=np.float64)
        z = (gain - gap) / self.scale_s
        p = 1.0 / (1.0 + np.exp(-np.clip(z, -50.0, 50.0)))
        out: FloatArray = np.where(np.isfinite(p), p, self.fallback_rate)
        return out
