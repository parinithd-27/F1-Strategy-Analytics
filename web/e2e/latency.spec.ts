import { writeFileSync } from "node:fs";

import { expect, test } from "@playwright/test";

/**
 * UI latency samples (Step 16, #35), gated: skipped unless E2E_LATENCY=1. The harness
 * (`make stream-latency-ui`, `f1sa.bench.streaming_runs`) builds this app with
 * NEXT_PUBLIC_DEBUG_LATENCY=1, needs the API with ENABLE_DEBUG=1, and replays 2024 R5 at 60× itself
 * once this spec has written E2E_READY_FILE (the page is open and its SSE stream is live). The page
 * posts one render time per applied event id (at most 50 in flight; ids beyond that are dropped, so
 * the samples are a subset). The spec only watches: it starts no replay and ends after session_end.
 */

const API = process.env.E2E_API_URL ?? "http://localhost:8000";
const READY = process.env.E2E_READY_FILE ?? "";
const TIMEOUT_MS = Number(process.env.E2E_LATENCY_TIMEOUT_MS ?? "900000");

test.skip(process.env.E2E_LATENCY !== "1", "UI latency run only (E2E_LATENCY=1, make stream-latency-ui)");

test("2024 R5: the page renders the replayed events until session_end", async ({ page, request }) => {
  test.setTimeout(TIMEOUT_MS + 60_000);
  const health = await request.get(`${API}/api/health`).catch(() => null);
  expect(health, `API not reachable at ${API}`).not.toBeNull();
  expect(READY, "E2E_READY_FILE is required").not.toBe("");

  await page.goto("/race/2024/5");
  await expect(page.getByRole("heading", { level: 1 })).toContainText("Chinese Grand Prix");
  await expect(page.locator(".conn")).toContainText("Live", { timeout: 60_000 });
  writeFileSync(READY, `${Date.now()}\n`);

  await expect(page.locator(".conn")).toContainText("Closed (session ended)", { timeout: TIMEOUT_MS });
  await expect.poll(async () => page.locator("table.tower tbody tr").count()).toBeGreaterThanOrEqual(20);
  // Let the last paints post their samples.
  await page.waitForTimeout(3_000);
});
