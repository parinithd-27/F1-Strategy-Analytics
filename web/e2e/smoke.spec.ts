import { expect, type APIRequestContext, test } from "@playwright/test";

/**
 * Smoke test against the real API in sample mode with replay enabled (`make api-up REPLAY=1`):
 * open the sample race 2024 R5 from the picker with the keyboard, start a replay at 60×, and see
 * the tower fill (20 cars) and at least one lap update. A replay writes only data/demo/<run_id>.
 */

const API = process.env.E2E_API_URL ?? "http://localhost:8000";

async function waitForIdle(request: APIRequestContext, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const body = (await (await request.get(`${API}/api/replays`)).json()) as { running: unknown[] };
    if (body.running.length === 0) return true;
    await new Promise((r) => setTimeout(r, 2000));
  }
  return false;
}

test.beforeAll(async ({ request }) => {
  const health = await request.get(`${API}/api/health`).catch(() => null);
  test.skip(health === null, `API not reachable at ${API}: run 'make api-up REPLAY=1' first`);
});

test("sample race: a 60× replay fills the tower and updates laps", async ({ page, request }) => {
  const status = (await (await request.get(`${API}/api/replays`)).json()) as { enabled: boolean };
  test.skip(!status.enabled, "replay is disabled on the API: run 'make api-up REPLAY=1'");
  expect(await waitForIdle(request, 150_000), "another replay is still running").toBe(true);

  await page.goto("/");
  await expect(page.getByRole("note", { name: "Data notice" })).toContainText("SYNTHETIC DEMO DATA");
  await expect(page.locator("#race-count")).toContainText("of 2 races");

  // Keyboard only: search, move into the list with the arrow keys, open with Enter.
  await page.getByLabel("Search races").fill("chinese");
  await expect(page.locator("#race-count")).toContainText("1 of 2 races");
  const link = page.getByRole("link", { name: /Chinese Grand Prix/ });
  await link.focus();
  await page.keyboard.press("Home");
  await expect(link).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/\/race\/2024\/5$/);
  await expect(page.getByRole("heading", { level: 1 })).toContainText("Chinese Grand Prix");

  await page.getByLabel("60×").check();
  const start = page.getByRole("button", { name: "Start replay" });
  await expect(start).toBeEnabled({ timeout: 15_000 });
  await start.click();
  await expect(page.locator("#replay-status")).toContainText(/Starting|Running/, { timeout: 20_000 });
  await expect(start).toBeDisabled();

  // The tower fills from the stream (the stream job needs ~20-40 s to be ready).
  const rows = page.locator("table.tower tbody tr");
  await expect.poll(async () => rows.count(), { timeout: 120_000, intervals: [1000] }).toBeGreaterThanOrEqual(20);
  await expect(page.locator(".conn")).toContainText("Live");

  // At least one lap update after the tower filled.
  const lapNow = page.locator(".lc-now");
  const before = (await lapNow.textContent())?.trim() ?? "";
  await expect.poll(async () => (await lapNow.textContent())?.trim(), { timeout: 60_000 }).not.toBe(before);

  // What-if (S15), keyboard only: the defender defaults to the car ahead; run with Enter.
  const panel = page.getByRole("region", { name: "What-if: pit strategy" });
  const attacker = panel.getByLabel("Attacker");
  await attacker.focus();
  await page.keyboard.press("ArrowDown"); // the car in P3 (its defender is P2)
  const chosen = await attacker.inputValue();
  expect(chosen).not.toBe("");
  await expect(panel.getByLabel("Defender (car ahead)")).not.toHaveValue("");
  await panel.getByRole("button", { name: "Run what-if" }).focus();
  await page.keyboard.press("Enter");
  await expect(panel.locator("#whatif-status")).toContainText(/at lap \d+/, { timeout: 20_000 });
  const table = panel.getByRole("table");
  for (const name of ["Pit now", "Pit next lap", "Stay out (overcut)"]) {
    await expect(table.getByRole("rowheader", { name: new RegExp(name.replace(/[()]/g, "\\$&")) }).first()).toBeVisible();
  }
  await expect(table.locator("tbody").first()).toContainText(/\d+%|Not possible/);
  await expect(panel).toContainText("Model (calibrated LR, from the stream):");
  await expect(panel.getByRole("alert")).toHaveText("");

  // 375 px: no horizontal page scroll (the tower scrolls inside its own region).
  await page.setViewportSize({ width: 375, height: 812 });
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
  expect(overflow).toBeLessThanOrEqual(0);
});
