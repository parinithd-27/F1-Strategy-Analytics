/** Route parameters of ``/race/[season]/[round]``, with the API's key rules (serving/keys.py):
 * season 1950–2100, round 1–30, decimal digits only. */
export function parseRaceParams(season: string, round: string): { season: number; round: number } | null {
  if (!/^\d{4}$/.test(season) || !/^\d{1,2}$/.test(round)) return null;
  const s = Number(season);
  const r = Number(round);
  if (s < 1950 || s > 2100 || r < 1 || r > 30) return null;
  return { season: s, round: r };
}
