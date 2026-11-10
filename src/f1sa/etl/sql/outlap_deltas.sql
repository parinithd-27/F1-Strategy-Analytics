-- Out-lap delta per usable stop: out-lap - the driver's green median lap. It covers pit exit and
-- tyre warm-up, and it is attributed to the driver's constructor in that race.
SELECT
  season,
  round,
  circuit_id,
  race_seq,
  constructor_id,
  driver_id,
  stop,
  ROUND(out_lap_s - green_median_s, 3) AS outlap_delta_s
FROM pit_cycles
WHERE constructor_id IS NOT NULL
