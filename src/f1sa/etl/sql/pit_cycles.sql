-- One row per usable pit stop: the in-lap (the stop lap) and the out-lap (the next lap), with the
-- driver's reference pace.
-- Excluded: long stops (repairs or red flag), stops on lap 1 (the in-lap includes the standing
-- start), stops without an out-lap (last lap or retirement), in-laps or out-laps that are
-- neutralised or red-flag, overlapping cycles (the in-lap is also an out-lap, or the out-lap is
-- also an in-lap), unknown lap times, and drivers without a reference pace.
SELECT
  p.season,
  p.round,
  p.driver_id,
  r.constructor_id,
  ri.circuit_id,
  ri.race_seq,
  p.stop,
  p.lap,
  inl.lap_time_s AS in_lap_s,
  outl.lap_time_s AS out_lap_s,
  g.green_median_s
FROM pitstops p
JOIN race_index ri ON ri.season = p.season AND ri.round = p.round
JOIN laps inl
  ON inl.season = p.season AND inl.round = p.round AND inl.driver_id = p.driver_id
 AND inl.lap = p.lap
JOIN laps outl
  ON outl.season = p.season AND outl.round = p.round AND outl.driver_id = p.driver_id
 AND outl.lap = p.lap + 1
JOIN driver_green_pace g
  ON g.season = p.season AND g.round = p.round AND g.driver_id = p.driver_id
LEFT JOIN results r
  ON r.season = p.season AND r.round = p.round AND r.driver_id = p.driver_id
WHERE NOT coalesce(p.is_long_stop, false)
  AND p.lap > 1
  AND NOT coalesce(inl.is_neutralised_lap, false) AND NOT coalesce(inl.is_red_flag_lap, false)
  AND NOT coalesce(outl.is_neutralised_lap, false) AND NOT coalesce(outl.is_red_flag_lap, false)
  AND NOT coalesce(inl.is_pit_out_lap, false)
  AND NOT coalesce(outl.is_pit_in_lap, false)
  AND inl.lap_time_s IS NOT NULL
  AND outl.lap_time_s IS NOT NULL
