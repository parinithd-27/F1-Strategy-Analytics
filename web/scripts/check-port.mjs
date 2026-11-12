// Port preflight for `make web` / `make web-e2e`: exits 2 with the holder and the remedy when the
// port is taken (usually the compose `web` container on 3000), instead of Next's bare EADDRINUSE.
import { execFile } from "node:child_process";
import net from "node:net";

const host = process.argv[2] ?? "127.0.0.1";
const port = Number(process.argv[3] ?? "3000");

function dockerHolder() {
  return new Promise((resolve) => {
    execFile("docker", ["ps", "--filter", `publish=${port}`, "--format", "{{.Names}}"], { timeout: 5000 }, (err, out) =>
      resolve(err ? null : out.trim().split(/\s+/).filter(Boolean).join(", ") || null),
    );
  });
}

const server = net.createServer();
server.once("error", async (err) => {
  if (err.code !== "EADDRINUSE") throw err;
  const holder = await dockerHolder();
  const who = holder ? `Docker container ${holder}` : `another process (try: ss -ltnp 'sport = :${port}')`;
  console.error(
    `error: port ${host}:${port} is already in use by ${who}. Stop it first ` +
      `(for the compose service: docker compose stop web). The API's CORS origins are ` +
      `localhost:${port} and 127.0.0.1:${port}, so the dev server must use this port.`,
  );
  process.exit(2);
});
server.listen(port, host, () => server.close(() => process.exit(0)));
