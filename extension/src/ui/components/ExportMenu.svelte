<script lang="ts">
  import { tick } from 'svelte';
  import { t } from '../../i18n';
  import { EXPORT_FORMATS } from '../session.svelte';
  import type { ExportFormat } from '../../engine/types';

  /** "Export ▾": Text, CSV, CUE, JSON, then Copy tracklist (DESIGN §7 export.*). */
  interface Props {
    /** A footer text button (side panel) instead of the toolbar's outlined button. */
    quiet?: boolean;
    up?: boolean;
    onexport: (format: ExportFormat) => void;
    oncopy: () => void;
  }

  let { quiet = false, up = false, onexport, oncopy }: Props = $props();
  let open = $state(false);
  let root = $state<HTMLElement | undefined>();

  async function toggle() {
    open = !open;
    if (!open) return;
    await tick();
    root?.querySelector<HTMLElement>('.menu .mi')?.focus();
  }

  function pick(run: () => void) {
    open = false;
    run();
  }

  function onkeydown(event: KeyboardEvent) {
    const items = [...(root?.querySelectorAll<HTMLElement>('.menu .mi') ?? [])];
    const index = items.indexOf(document.activeElement as HTMLElement);
    if (event.key === 'Escape') {
      event.preventDefault();
      event.stopPropagation();
      open = false;
      root?.querySelector<HTMLElement>('.export')?.focus();
    } else if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      items[(index + (event.key === 'ArrowDown' ? 1 : -1) + items.length) % items.length]?.focus();
    } else if (event.key === 'Tab') open = false;
  }

  $effect(() => {
    if (!open) return;
    const close = (event: PointerEvent) => {
      if (root && !root.contains(event.target as Node)) open = false;
    };
    document.addEventListener('pointerdown', close);
    return () => document.removeEventListener('pointerdown', close);
  });
</script>

<div class="menu-wrap" class:up bind:this={root}>
  <button class="btn export {quiet ? 'text quiet' : 'outlined'}" aria-haspopup="menu" aria-expanded={open} onclick={() => void toggle()}>
    <span class="ic ic-export s14"></span>{t('export.menu')}{#if !quiet}<span class="ic ic-chev-down s12 chev"></span>{/if}
  </button>
  {#if open}
    <div class="menu" role="menu" tabindex="-1" aria-label={t('export.menu')} {onkeydown}>
      {#each EXPORT_FORMATS as item (item.format)}
        <button class="mi" role="menuitem" tabindex="-1" onclick={() => pick(() => onexport(item.format))}><span class="k">{t(item.label)}</span><span class="meta">.{item.ext}</span></button>
      {/each}
      <div class="msep" role="separator"></div>
      <button class="mi" role="menuitem" tabindex="-1" onclick={() => pick(oncopy)}><span class="ic ic-copy s14"></span><span class="k">{t('export.copyTracklist')}</span></button>
    </div>
  {/if}
</div>
