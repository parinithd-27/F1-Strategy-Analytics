"""Model feature registry: which columns a model may use, where they come from, and feature sets.

Every candidate column has a **provenance**:

* ``jolpica``: Jolpica timing, pit stops, results and the Step 6 pit-loss prior (always real);
* ``stint_boundary``: stint length from stint boundaries, which come from real pit stops;
* ``tyre_state``: compound and tyre age. In sample mode these include the synthetic compound and
  starting-age draws (``compound_is_synthetic``);
* ``telemetry``: per-lap telemetry aggregates at the decision lap (synthetic in sample mode);
* ``fastf1``: reserved for Step 16 (FastF1-only features);
* ``tracinginsights``: real compound, tyre age and telemetry at the decision lap from the Step 4C
  clean tables (``stints_tracinginsights``, ``telemetry_lap_real``; ``is_synthetic=false``).
  Telemetry is joined only through the Jolpica-keyed left join (``dataset.with_ti_inputs``).
  Coverage policy: every null is 0.0 plus a ``_missing`` indicator (a categorical's ``unknown``
  level is its missing indicator); a ``partial`` telemetry lap keeps its values and sets
  ``*_telemetry_partial_ti``; no race or attempt is dropped or down-weighted for low coverage.

A **feature set** is a set of provenances (``FEATURE_SETS``), optionally narrowed to some
features of a provenance (``FEATURE_SET_ONLY``: ``real_tyre`` keeps only the TracingInsights tyre
inputs). Adding ``with_fastf1`` in Step 16 is one new entry plus the new ``FeatureSpec`` rows;
nothing else changes.

Every column of ``features.undercut.DECISION_FEATURES`` is listed either as a ``FeatureSpec`` or in
``EXCLUDED`` with a reason, so no decision-time column is dropped silently.
"""

from __future__ import annotations

from dataclasses import dataclass

from f1sa.features.undercut import DECISION_FEATURES

JOLPICA = "jolpica"
STINT_BOUNDARY = "stint_boundary"
TYRE_STATE = "tyre_state"
TELEMETRY = "telemetry"
FASTF1 = "fastf1"
TRACINGINSIGHTS = "tracinginsights"
PROVENANCES = (JOLPICA, STINT_BOUNDARY, TYRE_STATE, TELEMETRY, FASTF1, TRACINGINSIGHTS)

FEATURE_SETS: dict[str, frozenset[str]] = {
    "real": frozenset({JOLPICA, STINT_BOUNDARY}),
    "with_synthetic": frozenset({JOLPICA, STINT_BOUNDARY, TYRE_STATE, TELEMETRY}),
    # TracingInsights sets: development mode, and the 2025 ablation of docs/model_spec_v2.md.
    "real_ti": frozenset({JOLPICA, STINT_BOUNDARY, TRACINGINSIGHTS}),
}
FEATURE_SETS["real_tyre"] = frozenset({JOLPICA, STINT_BOUNDARY, TRACINGINSIGHTS})
DEV_ONLY_FEATURE_SETS = frozenset({"real_ti", "real_tyre"})
"""Feature sets beyond ``real`` that ``--source full --no-test`` (development mode) accepts."""
TEST_FEATURE_SETS: tuple[str, ...] = ("real", "real_tyre", "real_ti")
"""The only feature sets ``--allow-test-season`` accepts: the list in ``docs/model_spec_v2.md``
(``real`` primary, ``real_tyre`` and ``real_ti`` ablation). A unit test keeps the two equal."""
DEFAULT_FEATURE_SET = "real"

SYNTHETIC_FLAGS: dict[str, tuple[str, ...]] = {
    TYRE_STATE: ("compound_is_synthetic",),
    TELEMETRY: ("attacker_telemetry_is_synthetic", "defender_telemetry_is_synthetic"),
}
"""Provenances that are synthetic in sample mode, and the row-level flag columns proving whether a
given dataset's values are real. In full mode every flag must be false on every selected row."""

COMPOUNDS = ("SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET")
TELEMETRY_COLUMNS = (
    "mean_speed_kph",
    "max_speed_kph",
    "pct_full_throttle",
    "brake_fraction",
    "gear_changes",
    "mean_rpm",
)
SIDES = ("attacker", "defender")
TI_SUFFIX = "_ti"
TI_TELEMETRY_COLUMNS = (*TELEMETRY_COLUMNS, "telemetry_partial")
"""Per-side TracingInsights telemetry inputs (``telemetry_partial``: 1.0 partial, 0.0 full/none)."""


@dataclass(frozen=True)
class FeatureSpec:
    """One model input column.

    ``kind`` is ``numeric`` (gets a value column filled with 0.0 plus a ``_missing`` indicator)
    or ``categorical`` (one-hot over ``categories`` plus an ``unknown`` level for null or unseen).
    """

    name: str
    provenance: str
    kind: str = "numeric"
    categories: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.provenance not in PROVENANCES:
            raise ValueError(f"{self.name}: unknown provenance {self.provenance!r}")
        if self.kind not in ("numeric", "categorical"):
            raise ValueError(f"{self.name}: unknown kind {self.kind!r}")
        if self.kind == "categorical" and not self.categories:
            raise ValueError(f"{self.name}: a categorical feature needs categories")


_SOURCES_PIT = ("circuit_prior", "global_prior", "none")
_SOURCES_PACE = ("driver_team", "driver_team_shrunk", "team", "global", "none")

REGISTRY: tuple[FeatureSpec, ...] = (
    FeatureSpec("gap_s", JOLPICA),
    FeatureSpec("gap_ahead_change_1l", JOLPICA),
    FeatureSpec("gap_ahead_change_3l", JOLPICA),
    FeatureSpec("attacker_position", JOLPICA),
    FeatureSpec("pace_delta_last3_s", JOLPICA),
    FeatureSpec("attacker_deg_slope_s_per_lap", JOLPICA),
    FeatureSpec("defender_deg_slope_s_per_lap", JOLPICA),
    FeatureSpec("deg_slope_delta_s_per_lap", JOLPICA),
    FeatureSpec("laps_remaining", JOLPICA),
    FeatureSpec("pit_loss_prior_s", JOLPICA),
    FeatureSpec("pit_loss_source", JOLPICA, "categorical", _SOURCES_PIT),
    FeatureSpec("rejoin_traffic_cars", JOLPICA),
    FeatureSpec("rejoin_nearest_ahead_s", JOLPICA),
    FeatureSpec("attacker_expected_outlap_pace_residual_s", JOLPICA),
    FeatureSpec("attacker_expected_outlap_pace_source", JOLPICA, "categorical", _SOURCES_PACE),
    FeatureSpec("defender_expected_outlap_pace_residual_s", JOLPICA),
    FeatureSpec("defender_expected_outlap_pace_source", JOLPICA, "categorical", _SOURCES_PACE),
    FeatureSpec("attacker_expected_stop_delta_s", JOLPICA),
    FeatureSpec("defender_expected_stop_delta_s", JOLPICA),
    FeatureSpec("attacker_stint_laps", STINT_BOUNDARY),
    FeatureSpec("defender_stint_laps", STINT_BOUNDARY),
    FeatureSpec("attacker_tyre_age_laps", TYRE_STATE),
    FeatureSpec("defender_tyre_age_laps", TYRE_STATE),
    FeatureSpec("tyre_age_delta_laps", TYRE_STATE),
    FeatureSpec("attacker_compound", TYRE_STATE, "categorical", COMPOUNDS),
    FeatureSpec("defender_compound", TYRE_STATE, "categorical", COMPOUNDS),
    *(
        FeatureSpec(f"{side}_{col}", TELEMETRY)
        for side in ("attacker", "defender")
        for col in TELEMETRY_COLUMNS
    ),
    # TracingInsights (feature set real_ti). Appended last so the assembled column order of
    # ``real`` and ``with_synthetic`` is unchanged.
    *(
        FeatureSpec(f"{side}_compound{TI_SUFFIX}", TRACINGINSIGHTS, "categorical", COMPOUNDS)
        for side in SIDES
    ),
    *(FeatureSpec(f"{side}_tyre_age_laps{TI_SUFFIX}", TRACINGINSIGHTS) for side in SIDES),
    FeatureSpec(f"tyre_age_delta_laps{TI_SUFFIX}", TRACINGINSIGHTS),
    *(
        FeatureSpec(f"{side}_{col}{TI_SUFFIX}", TRACINGINSIGHTS)
        for side in SIDES
        for col in TI_TELEMETRY_COLUMNS
    ),
)

EXCLUDED: dict[str, str] = {
    "attacker_pace_last3_s": "absolute lap time is a circuit proxy; pace_delta_last3_s is used",
    "defender_pace_last3_s": "absolute lap time is a circuit proxy; pace_delta_last3_s is used",
    "compound_is_synthetic": "a provenance flag (checked by the full-mode refusal), not a feature",
}
"""DECISION_FEATURES columns that are deliberately not model inputs, with the reason."""

DERIVED: dict[str, str] = {
    "attacker_stint_laps": "decision_lap - start_lap of A's stint + 1 (stints table, lap <= L-1)",
    "defender_stint_laps": "decision_lap - start_lap of D's stint + 1 (stints table, lap <= L-1)",
    **{
        f"{side}_{col}": f"telemetry_lap_features.{col} of {side} at decision_lap (L-1)"
        for side in ("attacker", "defender")
        for col in TELEMETRY_COLUMNS
    },
    **{
        f"{side}_compound{TI_SUFFIX}": (
            f"the single known TracingInsights per-lap compound of {side} over the laps "
            "start_lap..decision_lap (L-1) of the stint containing L-1; null if none or "
            "conflicting (labels after L-1 are never read)"
        )
        for side in SIDES
    },
    **{
        f"{side}_tyre_age_laps{TI_SUFFIX}": (
            f"tyre_age_at_start + decision_lap - start_lap + 1 of {side}'s TracingInsights stint "
            "(null if tyre_age_at_start is null: never assumed new)"
        )
        for side in SIDES
    },
    f"tyre_age_delta_laps{TI_SUFFIX}": "defender_tyre_age_laps_ti - attacker_tyre_age_laps_ti",
    **{
        f"{side}_{col}{TI_SUFFIX}": (
            f"telemetry_lap_real.{col} of {side} at decision_lap, via the Jolpica-keyed left join"
        )
        for side in SIDES
        for col in TELEMETRY_COLUMNS
    },
    **{
        f"{side}_telemetry_partial{TI_SUFFIX}": (
            f"1.0 if telemetry_lap_real.telemetry_coverage of {side} at decision_lap is partial, "
            "0.0 if full or none, null if there is no TracingInsights row"
        )
        for side in SIDES
    },
}
"""Model inputs that are not in DECISION_FEATURES: how each is derived (all at lap L-1)."""

BASELINE_COLUMNS = (
    "gap_s",
    "defender_deg_slope_s_per_lap",
    "defender_stint_laps",
    "attacker_expected_outlap_pace_residual_s",
)
"""Raw columns the rule-based baseline reads (kept un-imputed in every split)."""


FEATURE_SET_ONLY: dict[str, dict[str, frozenset[str]]] = {
    # real + compound and tyre age of attacker and defender at L-1 plus the tyre-age delta
    # (defender - attacker): real_ti without the telemetry inputs and the partial flag.
    "real_tyre": {
        TRACINGINSIGHTS: frozenset(
            {
                *(
                    f"{side}_{base}{TI_SUFFIX}"
                    for side in SIDES
                    for base in ("compound", "tyre_age_laps")
                ),
                f"tyre_age_delta_laps{TI_SUFFIX}",
            }
        )
    },
}
"""Per feature set and provenance: the only registry names kept (absent = the whole provenance)."""


def ti_feature_names() -> tuple[str, ...]:
    """Registry names of the ``tracinginsights`` provenance, in registry order."""
    return tuple(s.name for s in REGISTRY if s.provenance == TRACINGINSIGHTS)


def uses_tracinginsights(feature_set: str) -> bool:
    return TRACINGINSIGHTS in FEATURE_SETS.get(feature_set, frozenset())


def specs_for(feature_set: str) -> tuple[FeatureSpec, ...]:
    """The registry entries of ``feature_set``, in registry order."""
    if feature_set not in FEATURE_SETS:
        raise ValueError(f"unknown feature set {feature_set!r}; known: {sorted(FEATURE_SETS)}")
    allowed = FEATURE_SETS[feature_set]
    only = FEATURE_SET_ONLY.get(feature_set, {})
    return tuple(
        s
        for s in REGISTRY
        if s.provenance in allowed and (s.provenance not in only or s.name in only[s.provenance])
    )


def model_columns(spec: FeatureSpec) -> tuple[str, ...]:
    """Assembled column names of a spec (the ``f_`` prefix avoids clashes with raw columns)."""
    if spec.kind == "numeric":
        return (f"f_{spec.name}", f"f_{spec.name}_missing")
    return tuple(f"f_{spec.name}__{c.lower()}" for c in (*spec.categories, "unknown"))


def assembled_columns(feature_set: str) -> tuple[str, ...]:
    """Every column VectorAssembler receives for ``feature_set``, in a fixed order."""
    return tuple(c for s in specs_for(feature_set) for c in model_columns(s))


def base_feature_of(column: str) -> str:
    """The registry feature an assembled column belongs to (for grouped importances)."""
    name = column.removeprefix("f_")
    for spec in REGISTRY:
        if name == spec.name or name == f"{spec.name}_missing" or name.startswith(f"{spec.name}__"):
            return spec.name
    raise KeyError(column)


def required_flags(feature_set: str) -> tuple[str, ...]:
    """Flag columns that must be all-false for ``feature_set`` in full mode."""
    provs = FEATURE_SETS.get(feature_set, frozenset())
    return tuple(f for p in PROVENANCES if p in provs for f in SYNTHETIC_FLAGS.get(p, ()))


def check_registry() -> None:
    """Every DECISION_FEATURES column is a model input or excluded with a reason; every input is a
    decision-time column or a documented derivation; no ORACLE / outcome / legacy column."""
    names = [s.name for s in REGISTRY]
    if len(names) != len(set(names)):
        raise ValueError("duplicate feature names in REGISTRY")
    decision = set(DECISION_FEATURES)
    missing = decision - set(names) - set(EXCLUDED)
    if missing:
        raise ValueError(f"DECISION_FEATURES not registered or excluded: {sorted(missing)}")
    unknown = set(names) - decision - set(DERIVED)
    if unknown:
        raise ValueError(
            f"model inputs that are neither decision-time nor derived: {sorted(unknown)}"
        )
    bad = [n for n in names if n.startswith(("oracle_", "outcome_")) or n.endswith("_legacy")]
    if bad:
        raise ValueError(f"post-decision or legacy columns in REGISTRY: {bad}")


check_registry()
