import type { NextConfig } from "next";

/** Security headers for every page. The app only talks to the API origin (NEXT_PUBLIC_API_URL). */
const securityHeaders = [
  { key: "X-Content-Type-Options", value: "nosniff" },
  { key: "Referrer-Policy", value: "no-referrer" },
  { key: "Content-Security-Policy", value: "frame-ancestors 'none'" },
  { key: "X-Frame-Options", value: "DENY" },
];

const config: NextConfig = {
  // The Docker image runs the standalone server (docker/web/Dockerfile sets NEXT_OUTPUT);
  // locally `next start` serves the normal build.
  ...(process.env.NEXT_OUTPUT === "standalone" ? { output: "standalone" as const } : {}),
  poweredByHeader: false,
  reactStrictMode: true,
  async headers() {
    return [{ source: "/:path*", headers: securityHeaders }];
  },
};

export default config;
