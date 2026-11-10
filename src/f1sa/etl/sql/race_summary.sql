-- Race summary aggregates. Each value describes the race itself, so this is reporting, not a
-- pre-race feature.
WITH entry AS (
  SELECT
    season,
    round,
    CAST(count(*) AS INT) AS entrants,
    CAST(sum(CASE WHEN finished THEN 1 ELSE 0 END) AS INT) AS finishers,
    CAST(sum(CASE WHEN classified THEN 1 ELSE 0 END) AS INT) AS classified,
    max(CASE WHEN position = 1 THEN laps END) AS winner_laps,
    max(CASE WHEN position = 1 THEN driver_id END) AS winner_driver_id,
    max(CASE WHEN position = 1 THEN constructor_id END) AS winner_constructor_id,
    max(CASE WHEN position = 1 THEN race_time_s END) AS winner_race_time_s
  FROM results
  GROUP BY season, round
),
stops AS (
  SELECT
    season,
    round,
    CAST(count(*) AS INT) AS pit_stops,
    CAST(sum(CASE WHEN is_long_stop THEN 1 ELSE 0 END) AS INT) AS long_stops
  FROM pitstops
  GROUP BY season, round
),
lap_stats AS (
  SELECT
    season,
    round,
    max(lap) AS max_lap,
    CAST(count(DISTINCT CASE WHEN is_neutralised_lap THEN lap END) AS INT) AS neutralised_laps,
    min(lap_time_s) AS fastest_lap_s,
    min_by(driver_id, lap_time_s) AS fastest_lap_driver_id,
    median(CASE
      WHEN lap > 1
       AND NOT coalesce(is_pit_in_lap, false) AND NOT coalesce(is_pit_out_lap, false)
       AND NOT coalesce(is_neutralised_lap, false) AND NOT coalesce(is_red_flag_lap, false)
      THEN lap_time_s END) AS green_median_lap_s
  FROM laps
  GROUP BY season, round
)
SELECT
  ra.season,
  ra.round,
  ra.circuit_id,
  ra.race_name,
  CAST(coalesce(e.winner_laps, l.max_lap) AS INT) AS race_laps,
  coalesce(e.entrants, 0) AS entrants,
  coalesce(e.finishers, 0) AS finishers,
  coalesce(e.classified, 0) AS classified,
  coalesce(s.pit_stops, 0) AS pit_stops,
  coalesce(s.long_stops, 0) AS long_stops,
  CASE WHEN e.entrants > 0 THEN ROUND(coalesce(s.pit_stops, 0) / e.entrants, 3) END
    AS stops_per_driver,
  coalesce(l.neutralised_laps, 0) AS neutralised_laps,
  ROUND(l.green_median_lap_s, 3) AS green_median_lap_s,
  l.fastest_lap_s,
  l.fastest_lap_driver_id,
  e.winner_driver_id,
  e.winner_constructor_id,
  e.winner_race_time_s
FROM races ra
LEFT JOIN entry e ON e.season = ra.season AND e.round = ra.round
LEFT JOIN stops s ON s.season = ra.season AND s.round = ra.round
LEFT JOIN lap_stats l ON l.season = ra.season AND l.round = ra.round
