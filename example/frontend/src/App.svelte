<script lang="ts">
  import { createSyncRuntime, useSyncState, AppendGraph } from "lab-link/svelte";

  interface SensorState {
    active: boolean;
    value: number;
    last_updated: number;
  }

  const runtime = createSyncRuntime({ url: `ws://${window.location.host}/sync/ws` });
  const sensor = useSyncState<SensorState>(runtime);
  const historyStream = runtime.stream("sensor_history", "append");

  let status = $state(runtime.status);
  runtime.onStatus((s) => (status = s));

  async function toggle() {
    await runtime.sendCommand("toggle");
  }

  function fmt(ts: number) {
    if (!ts) return "—";
    return new Date(ts * 1000).toLocaleTimeString();
  }
</script>

<main>
  <h1>Sensor Demo</h1>

  <span class="badge" class:connected={status === "connected"}>
    {status}
  </span>

  <div class="card">
    <div class="value">
      {sensor.value != null ? sensor.value.toFixed(2) : "—"}
      <span class="unit">°C</span>
    </div>
    <div class="meta">last updated {fmt(sensor.last_updated)}</div>
  </div>

  <button onclick={toggle} class:on={sensor.active}>
    {sensor.active ? "● Sensor active" : "○ Sensor stopped"}
  </button>

  <div class="graph-wrap">
    <AppendGraph
      stream={historyStream}
      title="Temperature history (20 Hz)"
      yLabel="°C"
      color="#4ade80"
      maxPoints={100}
      height={200}
    />
  </div>
</main>

<style>
  :global(body) {
    margin: 0;
    background: #0f172a;
    color: #f1f5f9;
    font-family: "Inter", system-ui, sans-serif;
  }

  main {
    max-width: 900px;
    margin: 4rem;
    padding: 0 1.5rem;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 1.5rem;
  }

  h1 {
    font-size: 1.5rem;
    font-weight: 600;
    letter-spacing: -0.02em;
    margin: 0;
  }

  .badge {
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    padding: 0.25rem 0.75rem;
    border-radius: 999px;
    background: #1e293b;
    color: #64748b;
  }

  .badge.connected {
    background: #052e16;
    color: #4ade80;
  }

  .card {
    width: 100%;
    background: #1e293b;
    border-radius: 16px;
    padding: 1.5rem 2rem;
    text-align: center;
  }

  .value {
    font-size: 3rem;
    font-weight: 700;
    font-variant-numeric: tabular-nums;
    line-height: 1;
  }

  .unit {
    font-size: 1.5rem;
    color: #94a3b8;
  }

  .meta {
    margin-top: 0.5rem;
    font-size: 0.8rem;
    color: #475569;
  }

  button {
    padding: 0.75rem 2rem;
    font-size: 0.95rem;
    font-weight: 500;
    border-radius: 10px;
    border: none;
    cursor: pointer;
    background: #1e293b;
    color: #64748b;
    transition:
      background 0.15s,
      color 0.15s;
    width: 100%;
  }

  button.on {
    background: #052e16;
    color: #4ade80;
  }

  button:hover {
    filter: brightness(1.15);
  }

  .graph-wrap {
    width: 100%;
    background: #1e293b;
    border-radius: 16px;
    padding: 1.25rem 1.5rem;
  }
</style>
