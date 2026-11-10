-- One row per race with a chronological ordinal. race_seq = season*100 + round (rounds < 100),
-- so "earlier race" is exactly race_seq < current race_seq.
SELECT
  season,
  round,
  circuit_id,
  CAST(season * 100 + round AS INT) AS race_seq
FROM races
