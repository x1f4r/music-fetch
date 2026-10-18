<script lang="ts">
  interface Props {
    url: string | null | undefined;
    /** One of the design's sizes, or a 16:9 video thumbnail (80 × 45; `video-l` 100 × 56). */
    size: 24 | 32 | 40 | 44 | 56 | 64 | 120 | 160 | 200 | 'video' | 'video-l';
    /** Only the picture: the parent is the `.art` box (the preview button in candidates). */
    bare?: boolean;
  }

  let { url, size, bare = false }: Props = $props();
  let failed = $state(false);
  const cls = $derived(size === 'video' ? 'video' : size === 'video-l' ? 'video l' : `art-${size}`);
</script>

{#snippet picture()}
  {#if url && !failed}
    <img src={url} alt="" loading="lazy" decoding="async" referrerpolicy="no-referrer" onerror={() => (failed = true)} />
  {/if}
{/snippet}

{#if bare}
  {@render picture()}
{:else}
  <div class="art {cls}" class:unknown={!url || failed} aria-hidden="true">{@render picture()}</div>
{/if}
