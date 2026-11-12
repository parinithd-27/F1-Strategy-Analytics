import type { Connection } from "@/lib/reducer";

const TEXT: Record<Connection, string> = {
  connecting: "Connecting",
  connected: "Live",
  reconnecting: "Reconnecting",
  closed: "Closed (session ended)",
};

const ICON: Record<Connection, string> = { connecting: "◌", connected: "●", reconnecting: "◌", closed: "■" };

export function ConnectionIndicator({ status, sessionEnded }: { status: Connection; sessionEnded: boolean }) {
  const text = status === "closed" && !sessionEnded ? "Closed" : TEXT[status];
  return (
    <p className={`conn conn-${status}`} role="status">
      <span className="conn-icon" aria-hidden="true">
        {ICON[status]}
      </span>{" "}
      <span className="visually-hidden">Connection: </span>
      {text}
    </p>
  );
}
