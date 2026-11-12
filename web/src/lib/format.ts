/** Display formatting. ``—`` (em dash) always means "unknown". */

export const DASH = "—";

/** ``max_verstappen`` → ``Verstappen``; ``de_vries`` → ``De Vries``. */
export function driverName(id: string): string {
  const parts = id.split("_").filter(Boolean);
  const surname = parts.length > 2 || parts[0] === "de" || parts[0] === "van" ? parts : parts.slice(-1);
  return surname.map((p) => p.charAt(0).toUpperCase() + p.slice(1)).join(" ") || id;
}

/** Three-letter timing-screen code from the surname: ``max_verstappen`` → ``VER``. */
export function driverCode(id: string): string {
  const name = driverName(id).replace(/\s+/g, "");
  return name.slice(0, 3).toUpperCase().padEnd(3, "·");
}

export function gap(seconds: number | null): string {
  if (seconds === null) return DASH;
  if (seconds === 0) return "Leader";
  return `+${seconds.toFixed(3)}`;
}

export function lapTime(seconds: number | null): string {
  if (seconds === null || seconds < 0) return DASH;
  const minutes = Math.floor(seconds / 60);
  const rest = seconds - minutes * 60;
  const secs = rest.toFixed(3).padStart(6, "0");
  return minutes > 0 ? `${minutes}:${secs}` : rest.toFixed(3);
}

export function probability(p: number | null): string {
  if (p === null) return DASH;
  return `${Math.round(p * 100)}%`;
}

export function laps(n: number | null): string {
  return n === null ? DASH : String(n);
}
