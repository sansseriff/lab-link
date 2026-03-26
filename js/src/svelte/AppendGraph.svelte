<script lang="ts">
  import { onMount } from "svelte";
  import type { StreamHandle } from "../stream-handle.js";
  import { loadBokeh, createLineSource } from "../bokeh/runtime.js";

  let {
    stream,
    title = "",
    yLabel = "value",
    xLabel = "sample",
    maxPoints = 300,
    height = 260,
    color = "#4ade80",
    lineWidth = 2,
    outputBackend = "webgl",
  }: {
    stream: StreamHandle;
    title?: string;
    yLabel?: string;
    xLabel?: string;
    maxPoints?: number;
    height?: number;
    color?: string;
    lineWidth?: number;
    outputBackend?: "canvas" | "webgl";
  } = $props();

  let container = $state<HTMLDivElement | null>(null);
  let bokehError = $state<string | null>(null);

  onMount(() => {
    let disposed = false;
    let source: any = null;
    let sampleIndex = 0;
    // Pending points collected between animation frames
    const pending: number[] = [];
    let rafId: number | null = null;

    function flush() {
      rafId = null;
      if (!source || pending.length === 0) return;
      for (const y of pending) {
        source.data.x.push(sampleIndex++);
        source.data.y.push(y);
      }
      pending.length = 0;
      // Rolling window: drop oldest points beyond maxPoints
      const overflow = source.data.x.length - maxPoints;
      if (overflow > 0) {
        source.data.x.splice(0, overflow);
        source.data.y.splice(0, overflow);
      }
      source.change.emit();
    }

    function scheduleFlush() {
      if (rafId === null) {
        rafId = requestAnimationFrame(flush);
      }
    }

    // Pre-populate with buffered history sent on connect
    const unsubSnapshot = stream.onSnapshot((msg: any) => {
      if (!source) return;
      const pts = msg.buffer as number[];
      for (const y of pts) {
        source.data.x.push(sampleIndex++);
        source.data.y.push(y);
      }
      const overflow = source.data.x.length - maxPoints;
      if (overflow > 0) {
        source.data.x.splice(0, overflow);
        source.data.y.splice(0, overflow);
      }
      if (source.data.x.length > 0) source.change.emit();
    });

    // Batch incoming points; drain at up to 60fps via rAF
    const unsubAppend = stream.onAppend((points: unknown[]) => {
      for (const p of points) pending.push(p as number);
      scheduleFlush();
    });

    void (async () => {
      try {
        const Bokeh = await loadBokeh();
        if (disposed || !container) return;

        source = createLineSource(Bokeh);

        const plot = Bokeh.Plotting.figure({
          tools: "pan,xwheel_zoom,reset",
          active_scroll: "xwheel_zoom",
          sizing_mode: "stretch_width",
          height,
          output_backend: outputBackend,
          x_axis_label: xLabel,
          y_axis_label: yLabel,
        });
        plot.toolbar.logo = null;
        plot.line(
          { field: "x" },
          { field: "y" },
          {
            source,
            line_color: color,
            line_width: lineWidth,
          },
        );

        const doc = new Bokeh.Document();
        doc.add_root(plot);
        Bokeh.embed.add_document_standalone(doc, container);
      } catch (err) {
        bokehError = `BokehJS unavailable: ${String(err)}`;
      }
    })();

    return () => {
      disposed = true;
      if (rafId !== null) cancelAnimationFrame(rafId);
      unsubSnapshot();
      unsubAppend();
      if (container) container.innerHTML = "";
    };
  });
</script>

{#if title}
  <p class="graph-title">{title}</p>
{/if}
{#if bokehError}
  <div class="graph-error">{bokehError}</div>
{:else}
  <div bind:this={container} class="graph-container"></div>
{/if}

<style>
  .graph-title {
    margin: 0 0 0.5rem;
    font-size: 0.85rem;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }

  .graph-container {
    width: 100%;
  }

  .graph-error {
    padding: 0.75rem;
    background: #431407;
    color: #fb923c;
    border-radius: 6px;
    font-size: 0.85rem;
  }
</style>
