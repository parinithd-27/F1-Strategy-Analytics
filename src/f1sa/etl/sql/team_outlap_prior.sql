-- Per (race, constructor entered in that race): out-lap delta history from strictly EARLIER races.
-- Built the same way as pit_loss_prior: one row per (race, constructor) holding its values, then
-- flatten(collect_list(...)) over `RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING` on race_seq
-- (only earlier races), then the median of the sorted array.
-- Fallback order: the team's prior median, then the circuit's prior median (all teams), then the
-- global prior median, then null with outlap_missing = true and outlap_source = 'none'.
WITH entries AS (
  SELECT DISTINCT r.season, r.round, r.constructor_id, ri.circuit_id, ri.race_seq
  FROM results r
  JOIN race_index ri ON ri.season = r.season AND ri.round = r.round
  WHERE r.constructor_id IS NOT NULL
),
per_entry AS (
  SELECT
    e.season,
    e.round,
    e.constructor_id,
    e.circuit_id,
    e.race_seq,
    coalesce(d.vals, CAST(array() AS ARRAY<DOUBLE>)) AS vals
  FROM entries e
  LEFT JOIN (
    SELECT season, round, constructor_id, collect_list(outlap_delta_s) AS vals
    FROM outlap_deltas
    WHERE outlap_delta_s IS NOT NULL
    GROUP BY season, round, constructor_id
  ) d ON d.season = e.season AND d.round = e.round AND d.constructor_id = e.constructor_id
),
prior AS (
  SELECT
    season,
    round,
    constructor_id,
    circuit_id,
    race_seq,
    array_sort(coalesce(flatten(collect_list(vals) OVER team_w), array())) AS t,
    array_sort(coalesce(flatten(collect_list(vals) OVER circuit_w), array())) AS c,
    array_sort(coalesce(flatten(collect_list(vals) OVER global_w), array())) AS g
  FROM per_entry
  WINDOW
    team_w AS (PARTITION BY constructor_id ORDER BY race_seq
               RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
    circuit_w AS (PARTITION BY circuit_id ORDER BY race_seq
                  RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
    global_w AS (ORDER BY race_seq RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
),
medians AS (
  SELECT
    season,
    round,
    constructor_id,
    circuit_id,
    race_seq,
    CASE
      WHEN size(t) = 0 THEN NULL
      WHEN size(t) % 2 = 1 THEN element_at(t, CAST((size(t) + 1) / 2 AS INT))
      ELSE (element_at(t, CAST(size(t) / 2 AS INT)) + element_at(t, CAST(size(t) / 2 AS INT) + 1)) / 2
    END AS team_prior_s,
    size(t) AS team_prior_n,
    CASE
      WHEN size(c) = 0 THEN NULL
      WHEN size(c) % 2 = 1 THEN element_at(c, CAST((size(c) + 1) / 2 AS INT))
      ELSE (element_at(c, CAST(size(c) / 2 AS INT)) + element_at(c, CAST(size(c) / 2 AS INT) + 1)) / 2
    END AS circuit_prior_s,
    size(c) AS circuit_prior_n,
    CASE
      WHEN size(g) = 0 THEN NULL
      WHEN size(g) % 2 = 1 THEN element_at(g, CAST((size(g) + 1) / 2 AS INT))
      ELSE (element_at(g, CAST(size(g) / 2 AS INT)) + element_at(g, CAST(size(g) / 2 AS INT) + 1)) / 2
    END AS global_prior_s,
    size(g) AS global_prior_n
  FROM prior
)
SELECT
  season,
  round,
  constructor_id,
  circuit_id,
  race_seq,
  ROUND(coalesce(team_prior_s, circuit_prior_s, global_prior_s), 3) AS outlap_delta_prior_s,
  CASE
    WHEN team_prior_s IS NOT NULL THEN 'team_prior'
    WHEN circuit_prior_s IS NOT NULL THEN 'circuit_prior'
    WHEN global_prior_s IS NOT NULL THEN 'global_prior'
    ELSE 'none'
  END AS outlap_source,
  coalesce(team_prior_s, circuit_prior_s, global_prior_s) IS NULL AS outlap_missing,
  ROUND(team_prior_s, 3) AS team_prior_s,
  CAST(team_prior_n AS INT) AS team_prior_n,
  ROUND(circuit_prior_s, 3) AS circuit_prior_s,
  CAST(circuit_prior_n AS INT) AS circuit_prior_n,
  ROUND(global_prior_s, 3) AS global_prior_s,
  CAST(global_prior_n AS INT) AS global_prior_n
FROM medians
