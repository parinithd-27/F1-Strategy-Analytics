/** Build-time configuration (Next.js inlines NEXT_PUBLIC_* values into the client bundle). */

const DEFAULT_API_URL = "http://localhost:8000";

/** ``raw`` as an http(s) origin without a trailing slash; the default when it is not one. */
export function parseApiUrl(raw: string | undefined): string {
  if (!raw) return DEFAULT_API_URL;
  try {
    const url = new URL(raw);
    if ((url.protocol !== "http:" && url.protocol !== "https:") || url.search || url.hash) {
      return DEFAULT_API_URL;
    }
    return `${url.protocol}//${url.host}${url.pathname.replace(/\/+$/, "")}`;
  } catch {
    return DEFAULT_API_URL;
  }
}

export const API_URL = parseApiUrl(process.env.NEXT_PUBLIC_API_URL);

/** Post UI render times to ``/api/debug/latency`` (the API must also run with ENABLE_DEBUG=1). */
export const DEBUG_LATENCY = process.env.NEXT_PUBLIC_DEBUG_LATENCY === "1";
