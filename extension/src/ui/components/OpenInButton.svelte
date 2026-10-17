<script lang="ts">
  import type { Track } from '../../engine/types';
  import { t } from '../../i18n';
  import {
    otherLinks,
    primaryLink,
    providerLinks,
    type CustomProvider,
    type Provider,
    type ProviderLink,
  } from '../../providers/links';
  import { serviceName } from '../../providers/services';

  interface Props {
    track: Track;
    preferred: Provider;
    custom: CustomProvider | null;
    /** Open the menu above the button (the popup has little room below). */
    up?: boolean;
    small?: boolean;
    onopen: (url: string) => void;
  }

  let { track, preferred, custom, up = false, small = false, onopen }: Props = $props();

  const links = $derived(providerLinks(track, custom));
  const primary = $derived(primaryLink(links, preferred));
  const others = $derived(otherLinks(links, primary));
  let open = $state(false);
  let root = $state<HTMLElement | undefined>();
  let items = $state<HTMLButtonElement[]>([]);

  function name(link: ProviderLink): string {
    return link.provider.kind === 'custom' ? custom?.name.trim() || t('provider.customDefault') : serviceName(link.provider.service);
  }

  function choose(link: ProviderLink) {
    open = false;
    onopen(link.url);
  }

  function toggle() {
    open = !open;
    if (open) queueMicrotask(() => items[0]?.focus());
  }

  function onmenukey(event: KeyboardEvent) {
    const index = items.indexOf(document.activeElement as HTMLButtonElement);
    if (event.key === 'Escape') {
      event.preventDefault();
      open = false;
      root?.querySelector<HTMLButtonElement>('.chev-part')?.focus();
    } else if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      const step = event.key === 'ArrowDown' ? 1 : -1;
      items[(index + step + items.length) % items.length]?.focus();
    }
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

{#if primary}
  <div class="split-wrap" class:up bind:this={root}>
    <div class="split filled">
      <button class="btn main" class:small onclick={() => choose(primary)}>
        {t('result.openIn', name(primary))}
      </button>
      {#if others.length > 0}
        <button
          class="btn chev-part"
          class:small
          aria-label={t('result.moreProviders')}
          aria-haspopup="menu"
          aria-expanded={open}
          onclick={toggle}
        ><span class="ic ic-chev-down s14 chev"></span></button>
      {/if}
    </div>
    {#if open}
      <div class="menu" role="menu" tabindex="-1" onkeydown={onmenukey}>
        {#each others as link, i (link.url)}
          <button class="mi" role="menuitem" bind:this={items[i]} onclick={() => choose(link)}>
            <span class="k">{name(link)}</span>
            {#if link.isSearch}<span class="meta">{t('result.searchMeta')}</span>{/if}
          </button>
        {/each}
      </div>
    {/if}
  </div>
{/if}
