import type { Metadata, Viewport } from "next";
import Link from "next/link";
import type { ReactNode } from "react";

import { DataBanner } from "@/components/DataBanner";
import { themeCss } from "@/lib/theme";

import "./globals.css";

export const metadata: Metadata = {
  title: { default: "Pit Wall · F1 Strategy Analytics", template: "%s · Pit Wall" },
  description: "Live undercut detection replay dashboard (IISc big-data project).",
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  themeColor: [
    { media: "(prefers-color-scheme: dark)", color: "#0c0f13" },
    { media: "(prefers-color-scheme: light)", color: "#f3f0e8" },
  ],
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <head>
        {/* Colour tokens from src/lib/theme.ts (the same values the contrast tests check). */}
        <style dangerouslySetInnerHTML={{ __html: themeCss }} />
      </head>
      <body>
        <a className="skip-link" href="#main">
          Skip to main content
        </a>
        <header className="topbar">
          <Link href="/" className="brand">
            <span className="brand-mark" aria-hidden="true">▮▮</span> PIT WALL
            <span className="brand-sub">F1 Strategy Analytics</span>
          </Link>
        </header>
        <DataBanner />
        <main id="main" tabIndex={-1}>
          {children}
        </main>
      </body>
    </html>
  );
}
