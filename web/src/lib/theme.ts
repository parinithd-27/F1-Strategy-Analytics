/** Colour tokens of the pit-wall theme: the single source for the CSS variables (``themeCss``) and
 * for the contrast tests. Dark = carbon pit wall, light = paper timing sheet. */

export interface Theme {
  bg: string;
  surface: string;
  surface2: string;
  text: string;
  muted: string;
  border: string;
  accent: string;
  accentInk: string;
  accentBg: string;
  focus: string;
  ok: string;
  warn: string;
  danger: string;
  info: string;
  bannerBg: string;
  bannerInk: string;
  grid: string;
  bar: string;
  lines: string[];
}

export const dark: Theme = {
  bg: "#0c0f13",
  surface: "#141920",
  surface2: "#1b222b",
  text: "#eef1f5",
  muted: "#a9b3bf",
  border: "#6e7a8a",
  accent: "#ffc72c",
  accentInk: "#ffc72c",
  accentBg: "#ffc72c",
  focus: "#ffc72c",
  ok: "#4ade80",
  warn: "#ffb454",
  danger: "#ff7a70",
  info: "#7cc4ff",
  bannerBg: "#ffc72c",
  bannerInk: "#14110a",
  grid: "#6b7787",
  bar: "#ffc72c",
  lines: ["#ffc72c", "#7cc4ff", "#ff7a70", "#4ade80", "#e0a8ff", "#ffffff", "#ff9d4d", "#5eead4", "#f9a8d4", "#c4b5fd"],
};

export const light: Theme = {
  bg: "#f3f0e8",
  surface: "#ffffff",
  surface2: "#e9e4d8",
  text: "#14181d",
  muted: "#4b5561",
  border: "#7a7466",
  accent: "#ffc72c",
  accentInk: "#7a4f00",
  accentBg: "#ffc72c",
  focus: "#7a4f00",
  ok: "#17703a",
  warn: "#8a4b00",
  danger: "#b3261e",
  info: "#0a58a8",
  bannerBg: "#14181d",
  bannerInk: "#ffc72c",
  grid: "#8c8578",
  bar: "#7a4f00",
  lines: ["#7a4f00", "#0a58a8", "#b3261e", "#17703a", "#7b3fa0", "#14181d", "#a34700", "#0f6e66", "#a3195b", "#4c3fa8"],
};

const VAR: Record<Exclude<keyof Theme, "lines">, string> = {
  bg: "--bg",
  surface: "--surface",
  surface2: "--surface-2",
  text: "--text",
  muted: "--muted",
  border: "--border",
  accent: "--accent",
  accentInk: "--accent-ink",
  accentBg: "--accent-bg",
  focus: "--focus",
  ok: "--ok",
  warn: "--warn",
  danger: "--danger",
  info: "--info",
  bannerBg: "--banner-bg",
  bannerInk: "--banner-ink",
  grid: "--grid",
  bar: "--bar",
};

function vars(t: Theme): string {
  const base = (Object.keys(VAR) as (keyof typeof VAR)[]).map((k) => `${VAR[k]}:${t[k]};`).join("");
  return base + t.lines.map((c, i) => `--line-${i}:${c};`).join("");
}

/** ``:root`` variables: light by default, dark under ``prefers-color-scheme: dark``. */
export const themeCss = `:root{color-scheme:light dark;${vars(light)}}@media (prefers-color-scheme: dark){:root{${vars(dark)}}}`;

function channel(c: number): number {
  const s = c / 255;
  return s <= 0.04045 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
}

export function luminance(hex: string): number {
  const m = /^#([0-9a-f]{6})$/i.exec(hex);
  if (!m || !m[1]) throw new Error(`not a #rrggbb colour: ${hex}`);
  const n = parseInt(m[1], 16);
  return 0.2126 * channel((n >> 16) & 255) + 0.7152 * channel((n >> 8) & 255) + 0.0722 * channel(n & 255);
}

/** WCAG 2.x contrast ratio of two #rrggbb colours (1–21). */
export function contrastRatio(a: string, b: string): number {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x) as [number, number];
  return (hi + 0.05) / (lo + 0.05);
}
