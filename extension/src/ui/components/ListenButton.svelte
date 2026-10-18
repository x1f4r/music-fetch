<script lang="ts">
  import { LIVE_VIEWBOX, livePath, MARK_PATH, MARK_VIEWBOX } from '../draw/wave';
  import { stillMotion } from '../motion';

  interface Props {
    /** Capture is running: halo and the live line. */
    live?: boolean;
    /** Humming: the ring fills with voiced time (0–100). */
    hum?: boolean;
    ring?: number;
    /** Accessible name of the ring ("Hum length") and its value in seconds. */
    ringLabel?: string;
    ringSeconds?: number;
    /** The engine is identifying: the last live line freezes at 70 %. */
    matching?: boolean;
    /** A link session: the link glyph replaces the mark. */
    link?: boolean;
    level?: number;
    label: string;
    describedBy?: string;
    /** The grant page shows the disc as a picture, not a button. */
    decorative?: boolean;
    onclick?: () => void;
  }

  let { live = false, hum = false, ring = 0, ringLabel, ringSeconds = 0, matching = false, link = false, level = 0, label, describedBy, decorative = false, onclick }: Props = $props();

  // The live line drifts between level updates so it reads as sound, not a chart; it
  // freezes on its last frame while matching.
  let phase = $state(0);
  $effect(() => {
    if (!live || stillMotion()) return;
    let frame = 0;
    const tick = () => {
      phase = (phase + 0.12) % (Math.PI * 2);
      frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frame);
  });
  const showLine = $derived(live || (matching && level > 0.02));
</script>

{#snippet glyph()}
  {#if showLine}
    <svg class="mark" viewBox={LIVE_VIEWBOX} fill="none" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
      <path d={livePath(Math.max(level, hum ? 0.3 : 0.08), phase)} />
    </svg>
  {:else}
    <svg class="mark" viewBox={MARK_VIEWBOX} fill="none" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
      <path d={MARK_PATH} />
    </svg>
  {/if}
  <span class="alt" aria-hidden="true"><span class="ic ic-link s24"></span></span>
{/snippet}

<div class="listen" class:live class:hum class:matching class:link>
  <div class="halo"></div>
  {#if hum}
    <div class="ring" style:--ring={ring} role="progressbar" aria-label={ringLabel} aria-valuemin="0" aria-valuemax="12" aria-valuenow={Math.min(12, ringSeconds)}></div>
  {:else}
    <div class="ring" style:--ring={ring}></div>
  {/if}
  {#if decorative}
    <div class="disc" aria-hidden="true">{@render glyph()}</div>
  {:else}
    <button class="disc" aria-label={label} aria-describedby={describedBy} {onclick}>{@render glyph()}</button>
  {/if}
</div>
