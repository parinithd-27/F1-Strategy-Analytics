import type { Compound } from "@/lib/api";

/** A distinct shape per compound, so the compound never depends on colour alone. The compound name
 * is always rendered as text next to it; the glyph itself is decorative (aria-hidden). */
export function TyreGlyph({ compound, size = 16 }: { compound: Compound; size?: number }) {
  const common = { width: size, height: size, viewBox: "0 0 16 16", "aria-hidden": true, focusable: false } as const;
  switch (compound) {
    case "SOFT":
      return (
        <svg {...common} className="glyph glyph-soft">
          <circle cx="8" cy="8" r="6.5" fill="currentColor" />
        </svg>
      );
    case "MEDIUM":
      return (
        <svg {...common} className="glyph glyph-medium">
          <circle cx="8" cy="8" r="6" fill="none" stroke="currentColor" strokeWidth="1.5" />
          <path d="M8 2a6 6 0 0 1 0 12z" fill="currentColor" />
        </svg>
      );
    case "HARD":
      return (
        <svg {...common} className="glyph glyph-hard">
          <circle cx="8" cy="8" r="5.75" fill="none" stroke="currentColor" strokeWidth="2.5" />
        </svg>
      );
    case "INTERMEDIATE":
      return (
        <svg {...common} className="glyph glyph-inter">
          <rect x="2" y="2" width="12" height="12" rx="2" fill="none" stroke="currentColor" strokeWidth="1.5" />
          <path d="M2 10 10 2M6 14 14 6M2 14 14 2" stroke="currentColor" strokeWidth="1.25" />
        </svg>
      );
    case "WET":
      return (
        <svg {...common} className="glyph glyph-wet">
          <path d="M8 1.5C5.5 5.5 3.5 8 3.5 10.5a4.5 4.5 0 0 0 9 0C12.5 8 10.5 5.5 8 1.5z" fill="none" stroke="currentColor" strokeWidth="1.5" />
          <path d="M6 9v4M8 8v6M10 9v4" stroke="currentColor" strokeWidth="1.25" />
        </svg>
      );
    case "UNKNOWN":
      return (
        <svg {...common} className="glyph glyph-unknown">
          <circle cx="8" cy="8" r="6" fill="none" stroke="currentColor" strokeWidth="1.25" strokeDasharray="2 2" />
          <text x="8" y="11.5" textAnchor="middle" fontSize="9" fill="currentColor">?</text>
        </svg>
      );
  }
}

export const COMPOUND_LABEL: Record<Compound, string> = {
  SOFT: "Soft",
  MEDIUM: "Medium",
  HARD: "Hard",
  INTERMEDIATE: "Inter",
  WET: "Wet",
  UNKNOWN: "Unknown",
};
