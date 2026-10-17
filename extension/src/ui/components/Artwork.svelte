<script lang="ts">
  interface Props {
    url: string | null | undefined;
    /** One of the design's sizes: 24, 32, 40, 44, 56, 64, 120, 160, 200. */
    size: 24 | 32 | 40 | 44 | 56 | 64 | 120 | 160 | 200;
  }

  let { url, size }: Props = $props();
  let failed = $state(false);
</script>

<div class="art art-{size}" class:unknown={!url || failed} aria-hidden="true">
  {#if url && !failed}
    <img src={url} alt="" loading="lazy" decoding="async" referrerpolicy="no-referrer" onerror={() => (failed = true)} />
  {/if}
</div>
