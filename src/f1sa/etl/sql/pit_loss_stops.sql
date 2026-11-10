-- Pit loss per usable stop: in-lap + out-lap - 2 x the driver's green median lap.
SELECT
  season,
  round,
  circuit_id,
  race_seq,
  driver_id,
  stop,
  ROUND(in_lap_s + out_lap_s - 2 * green_median_s, 3) AS pit_loss_s
FROM pit_cycles
