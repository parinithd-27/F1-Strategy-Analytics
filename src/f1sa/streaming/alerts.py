"""Which undercut alerts are published (the alert policy; docs/architecture.md, "Alert policy").

The rule and the scorer still evaluate every eligible pair on every lap, and every driver hash
still carries ``undercut_flag`` / ``undercut_prob`` per lap. Only the ``undercut_alert`` messages on
the events channel are thinned, so a dashboard (and its aria-live feed) sees changes, not a
repeat of every candidate on every lap:

* ``opportunity`` when a pair (attacker, defender) becomes a candidate at decision lap L-1 (it
  was not one at L-2: ``new_candidate``), and again when its calibrated P crosses the threshold
  upward (below at L-2, at or above at L-1: ``probability_crossed``). At most one per pair and lap.
* ``attempt`` when the attacker of a candidate pair at L-1 actually pits on lap L, with P at L-1.
  It is emitted by whichever arrives second, the evaluated decision or the ``pit_in`` event, so it
  does not depend on how the stream is cut into micro-batches. At most one per attacker and lap.

The threshold is the deployed model's ``decision_threshold`` (0.5, fixed before any full-data
model, never tuned).
"""

from __future__ import annotations

from f1sa.streaming.events import UndercutAlert
from f1sa.streaming.live_state import Decision, EligibleRecord, RaceState

ALERT_THRESHOLD = 0.5


def opportunity_trigger(
    previous: EligibleRecord | None, defender_id: str, probability: float, threshold: float
) -> str | None:
    """``new_candidate``, ``probability_crossed`` or None for a pair eligible at this lap, given
    the attacker's eligible pair at the previous lap (None if it had none)."""
    if previous is None or previous.defender_id != defender_id:
        return "new_candidate"
    if previous.probability < threshold <= probability:
        return "probability_crossed"
    return None


def _attempt(state: RaceState, attacker: str, decision_lap: int, threshold: float) -> UndercutAlert:
    rec = state.eligible[(decision_lap, attacker)]
    state.attempts_alerted.add((attacker, decision_lap + 1))
    return UndercutAlert(
        reason="attempt",
        trigger="pit_in",
        season=state.season,
        round=state.round,
        attacker_id=attacker,
        defender_id=rec.defender_id,
        decision_lap=decision_lap,
        gap_s=rec.gap_s,
        undercut_prob=rec.probability,
        threshold=threshold,
        inputs=rec.inputs,
    )


def on_decision(
    state: RaceState, d: Decision, threshold: float = ALERT_THRESHOLD
) -> list[UndercutAlert]:
    """Record an eligible decision; the alerts it causes (opportunity, and attempt if the
    attacker's ``pit_in`` on the next lap has already arrived)."""
    lap, a = d.decision_lap, d.attacker_id
    trigger = opportunity_trigger(
        state.eligible.get((lap - 1, a)), d.defender_id, d.probability, threshold
    )
    state.eligible[(lap, a)] = EligibleRecord(d.defender_id, d.probability, d.gap_s, dict(d.inputs))
    out = []
    if trigger is not None:
        out.append(
            UndercutAlert(
                reason="opportunity",
                trigger=trigger,
                season=d.season,
                round=d.round,
                attacker_id=a,
                defender_id=d.defender_id,
                decision_lap=lap,
                gap_s=d.gap_s,
                undercut_prob=d.probability,
                threshold=threshold,
                inputs=d.inputs,
            )
        )
    if lap + 1 in state.in_laps.get(a, set()) and (a, lap + 1) not in state.attempts_alerted:
        out.append(_attempt(state, a, lap, threshold))
    return out


def on_pit_in(
    state: RaceState, attacker: str, lap: int, threshold: float = ALERT_THRESHOLD
) -> UndercutAlert | None:
    """The ``attempt`` alert for a ``pit_in`` on ``lap`` if the attacker had an eligible pair at
    ``lap - 1`` that was already evaluated (else ``on_decision`` emits it later)."""
    if (lap - 1, attacker) not in state.eligible or (attacker, lap) in state.attempts_alerted:
        return None
    return _attempt(state, attacker, lap - 1, threshold)
