import { describe, expect, it } from "vitest";

import { contrastRatio, dark, light, type Theme } from "@/lib/theme";

const AA_TEXT = 4.5;
const AA_UI = 3;

function pairs(t: Theme): [string, string, string, number][] {
  const out: [string, string, string, number][] = [];
  for (const bg of ["bg", "surface", "surface2"] as const) {
    for (const fg of ["text", "muted", "accentInk", "ok", "warn", "danger", "info"] as const) {
      out.push([fg, bg, `${fg} on ${bg}`, AA_TEXT]);
    }
    out.push(["border", bg, `border on ${bg}`, AA_UI]);
    out.push(["focus", bg, `focus ring on ${bg}`, AA_UI]);
  }
  out.push(["bannerInk", "bannerBg", "banner", AA_TEXT]);
  return out.map(([a, b, label, min]) => [t[a as keyof Theme] as string, t[b as keyof Theme] as string, label, min]);
}

describe.each([
  ["dark", dark],
  ["light", light],
] as const)("%s theme meets WCAG AA", (_name, theme) => {
  it.each(pairs(theme))("%s on %s (%s) ≥ %d:1", (fg, bg, _label, min) => {
    expect(contrastRatio(fg, bg)).toBeGreaterThanOrEqual(min);
  });

  it("button ink on the accent background", () => {
    expect(contrastRatio("#14110a", theme.accentBg)).toBeGreaterThanOrEqual(AA_TEXT);
  });

  it("every chart line is ≥ 3:1 against the panel", () => {
    for (const c of theme.lines) expect(contrastRatio(c, theme.surface)).toBeGreaterThanOrEqual(AA_UI);
  });

  it("tyre-age bars are visible against their track (≥ 3:1)", () => {
    expect(contrastRatio(theme.bar, theme.surface2)).toBeGreaterThanOrEqual(AA_UI);
    expect(contrastRatio(theme.bar, theme.surface)).toBeGreaterThanOrEqual(AA_UI);
  });

  it("chart grid lines are visible (≥ 3:1)", () => {
    expect(contrastRatio(theme.grid, theme.surface)).toBeGreaterThanOrEqual(AA_UI);
  });
});

describe("contrastRatio", () => {
  it("matches the WCAG reference values", () => {
    expect(contrastRatio("#000000", "#ffffff")).toBeCloseTo(21, 5);
    expect(contrastRatio("#ffffff", "#ffffff")).toBeCloseTo(1, 5);
    expect(() => contrastRatio("red", "#fff")).toThrow();
  });
});
