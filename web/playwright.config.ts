import { defineConfig, devices } from "@playwright/test";

/**
 * Smoke test against the running API (sample mode, `make api-up REPLAY=1`) and this app built and
 * started on 127.0.0.1:3000 (`make web-e2e` does both). The API origin must be the CORS origin
 * http://localhost:3000, so the browser uses localhost, not 127.0.0.1.
 */
export default defineConfig({
  testDir: "./e2e",
  timeout: 180_000,
  expect: { timeout: 15_000 },
  retries: 0,
  workers: 1,
  reporter: [["list"]],
  use: {
    baseURL: process.env.E2E_BASE_URL ?? "http://localhost:3000",
    trace: "retain-on-failure",
    reducedMotion: "reduce",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  ...(process.env.E2E_NO_SERVER
    ? {}
    : {
        webServer: {
          command: "npm run start",
          url: "http://localhost:3000",
          reuseExistingServer: false,
          timeout: 60_000,
        },
      }),
});
