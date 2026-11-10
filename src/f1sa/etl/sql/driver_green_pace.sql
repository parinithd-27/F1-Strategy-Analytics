-- Reference pace per (race, driver): the median of green racing laps (not lap 1, not a pit in or
-- out lap, not neutralised or red-flag, time known). Drivers with fewer than :min_green_laps such
-- laps get no reference, and their stops are skipped downstream.
SELECT
  season,
  round,
  driver_id,
  median(lap_time_s) AS green_median_s,
  CAST(count(*) AS INT) AS green_laps
FROM laps
WHERE lap > 1
  AND NOT coalesce(is_pit_in_lap, false)
  AND NOT coalesce(is_pit_out_lap, false)
  AND NOT coalesce(is_neutralised_lap, false)
  AND NOT coalesce(is_red_flag_lap, false)
  AND lap_time_s IS NOT NULL
GROUP BY season, round, driver_id
HAVING count(*) >= :min_green_laps
