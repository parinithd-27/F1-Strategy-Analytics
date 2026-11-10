-- Per race: the median pit loss from strictly EARLIER races only.
-- 1. One row per race (every race, even one with no usable stops) holding that race's stop values.
-- 2. Expanding window over race_seq, `RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`, which is
--    race_seq <= current - 1. That excludes the current race and every later race. The prior values
--    are gathered with flatten(collect_list(...)) because Spark does not allow median() with a
--    window frame.
-- 3. The median of the sorted prior values, interpolated linearly like Spark's median().
-- Fallback order: circuit median from prior races, then global median from prior races, then
-- null with pit_loss_missing = true and pit_loss_source = 'none'.
WITH per_race AS (
  SELECT
    ri.season,
    ri.round,
    ri.circuit_id,
    ri.race_seq,
    coalesce(s.vals, CAST(array() AS ARRAY<DOUBLE>)) AS vals
  FROM race_index ri
  LEFT JOIN (
    SELECT season, round, collect_list(pit_loss_s) AS vals
    FROM pit_loss_stops
    WHERE pit_loss_s IS NOT NULL
    GROUP BY season, round
  ) s ON s.season = ri.season AND s.round = ri.round
),
prior AS (
  SELECT
    season,
    round,
    circuit_id,
    race_seq,
    array_sort(coalesce(flatten(collect_list(vals) OVER circuit_w), array())) AS c,
    coalesce(sum(CASE WHEN size(vals) > 0 THEN 1 ELSE 0 END) OVER circuit_w, 0) AS c_races,
    array_sort(coalesce(flatten(collect_list(vals) OVER global_w), array())) AS g,
    coalesce(sum(CASE WHEN size(vals) > 0 THEN 1 ELSE 0 END) OVER global_w, 0) AS g_races
  FROM per_race
  WINDOW
    circuit_w AS (PARTITION BY circuit_id ORDER BY race_seq
                  RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
    global_w AS (ORDER BY race_seq RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
),
medians AS (
  SELECT
    season,
    round,
    circuit_id,
    race_seq,
    CASE
      WHEN size(c) = 0 THEN NULL
      WHEN size(c) % 2 = 1 THEN element_at(c, CAST((size(c) + 1) / 2 AS INT))
      ELSE (element_at(c, CAST(size(c) / 2 AS INT)) + element_at(c, CAST(size(c) / 2 AS INT) + 1)) / 2
    END AS circuit_prior_s,
    size(c) AS circuit_prior_stops,
    c_races AS circuit_prior_races,
    CASE
      WHEN size(g) = 0 THEN NULL
      WHEN size(g) % 2 = 1 THEN element_at(g, CAST((size(g) + 1) / 2 AS INT))
      ELSE (element_at(g, CAST(size(g) / 2 AS INT)) + element_at(g, CAST(size(g) / 2 AS INT) + 1)) / 2
    END AS global_prior_s,
    size(g) AS global_prior_stops,
    g_races AS global_prior_races
  FROM prior
)
SELECT
  season,
  round,
  circuit_id,
  race_seq,
  ROUND(coalesce(circuit_prior_s, global_prior_s), 3) AS pit_loss_prior_s,
  CASE
    WHEN circuit_prior_s IS NOT NULL THEN 'circuit_prior'
    WHEN global_prior_s IS NOT NULL THEN 'global_prior'
    ELSE 'none'
  END AS pit_loss_source,
  coalesce(circuit_prior_s, global_prior_s) IS NULL AS pit_loss_missing,
  ROUND(circuit_prior_s, 3) AS circuit_prior_s,
  CAST(circuit_prior_stops AS INT) AS circuit_prior_stops,
  CAST(circuit_prior_races AS INT) AS circuit_prior_races,
  ROUND(global_prior_s, 3) AS global_prior_s,
  CAST(global_prior_stops AS INT) AS global_prior_stops,
  CAST(global_prior_races AS INT) AS global_prior_races
FROM medians
