<script lang="ts">
  import { tick } from 'svelte';
  import type { Track } from '../../engine/types';
  import { t } from '../../i18n';
  import {
    otherLinks,
    primaryLink,
    providerLinks,
    sameProvider,
    webSearchUrl,
    type CustomProvider,
    type Provider,
    type ProviderLink,
  } from '../../providers/links';
  import { serviceName } from '../../providers/services';

  /**
   * The split "Open in ‹provider›" button (DESIGN §5.11, design spec "Split button"): the
   * main part opens the preferred provider; the chevron lists every provider (current one
   * checked, exact links first, "search" for search links), the custom provider and
   * "Search the web".
   */
  interface Props {
    track: Track;
    preferred: Provider;
    custom: CustomProvider | null;
    /** Open the menu above the button; by default it opens where there is room. */
    up?: boolean;
    small?: boolean;
    /** Fill the column (details column, touch): only the main part grows. */
    wide?: boolean;
    onopen: (url: string) => void;
  }

  let { track, preferred, custom, up, small = false, wide = false, onopen }: Props = $props();

  const links = $derived(providerLinks(track, custom));
  const primary = $derived(primaryLink(links, preferred));
  /** The current provider first, then exact links, then searches (custom and web search follow the separator). */
  const services = $derived([
    ...(primary && primary.provider.kind !== 'custom' ? [primary] : []),
    ...otherLinks(links, primary).filter((l) => l.provider.kind !== 'custom'),
  ]);
  const customLink = $derived(links.find((l) => l.provider.kind === 'custom') ?? null);
  const searchWeb = $derived(webSearchUrl(track));
  let open = $state(false);
  let openUp = $state(false);
  let scrolls = $state(false);
  let root = $state<HTMLElement | undefined>();
  let menu = $state<HTMLElement | undefined>();
  let typed = '';
  let typedAt = 0;

  function name(link: ProviderLink): string {
    return link.provider.kind === 'custom' ? custom?.name.trim() || t('provider.customDefault') : serviceName(link.provider.service);
  }

  function isCurrent(link: ProviderLink): boolean {
    return primary !== null && sameProvider(link.provider, primary.provider);
  }

  function choose(url: string) {
    open = false;
    onopen(url);
  }

  function items(): HTMLElement[] {
    return [...(menu?.querySelectorAll<HTMLElement>('.mi') ?? [])];
  }

  async function toggle() {
    if (open) {
      open = false;
      return;
    }
    // The popup has little room under the button: open upward when under 240 px remain.
    const rect = root?.getBoundingClientRect();
    openUp = up ?? (rect ? window.innerHeight - rect.bottom < 240 && rect.top > window.innerHeight - rect.bottom : false);
    open = true;
    await tick();
    scrolls = menu ? menu.scrollHeight > menu.clientHeight + 1 : false;
    items()[0]?.focus();
  }

  function close(focusChevron = true) {
    open = false;
    if (focusChevron) root?.querySelector<HTMLButtonElement>('.chev-part')?.focus();
  }

  function onmenukey(event: KeyboardEvent) {
    const list = items();
    const index = list.indexOf(document.activeElement as HTMLElement);
    const move = (to: number) => {
      event.preventDefault();
      list[(to + list.length) % list.length]?.focus();
    };
    if (event.key === 'Escape') {
      event.preventDefault();
      event.stopPropagation();
      close();
    } else if (event.key === 'ArrowDown') move(index + 1);
    else if (event.key === 'ArrowUp') move(index - 1);
    else if (event.key === 'Home') move(0);
    else if (event.key === 'End') move(list.length - 1);
    else if (event.key === 'Tab') close(false);
    else if (event.key.length === 1 && /\S/.test(event.key)) {
      // Type-ahead: jump to the next item starting with what was typed.
      const nowMs = performance.now();
      typed = nowMs - typedAt < 700 ? typed + event.key.toLowerCase() : event.key.toLowerCase();
      typedAt = nowMs;
      const match = list.findIndex((el, i) => i > (typed.length > 1 ? index - 1 : index) && el.textContent?.trim().toLowerCase().startsWith(typed));
      const wrap = match >= 0 ? match : list.findIndex((el) => el.textContent?.trim().toLowerCase().startsWith(typed));
      if (wrap >= 0) move(wrap);
    }
  }

  $effect(() => {
    if (!open) return;
    const onpointer = (event: PointerEvent) => {
      if (root && !root.contains(event.target as Node)) open = false;
    };
    document.addEventListener('pointerdown', onpointer);
    return () => document.removeEventListener('pointerdown', onpointer);
  });
</script>

{#if primary}
  <div class="split-wrap" class:up={openUp} class:wide bind:this={root}>
    <div class="split filled" role="group" aria-label={t('result.openIn', '').trim()}>
      <button class="btn main" class:small onclick={() => choose(primary.url)}>{t('result.openIn', name(primary))}</button>
      <button
        class="btn chev-part"
        class:small
        aria-label={t('result.moreProviders')}
        aria-haspopup="menu"
        aria-expanded={open}
        onclick={() => void toggle()}
      ><span class="ic ic-chev-down s14"></span></button>
    </div>
    {#if open}
      <div class="menu" class:scrolls role="menu" tabindex="-1" aria-label={t('result.moreProviders')} bind:this={menu} onkeydown={onmenukey}>
        {#each services as link (link.url)}
          <button class="mi" class:current={isCurrent(link)} role="menuitemradio" aria-checked={isCurrent(link)} tabindex="-1" onclick={() => choose(link.url)}>
            <span class="k">{name(link)}</span>
            {#if link.isSearch}<span class="meta">{t('result.searchMeta')}</span>{/if}
            {#if isCurrent(link)}<span class="ic ic-check s14"></span>{/if}
          </button>
        {/each}
        <div class="msep" role="separator"></div>
        {#if customLink}
          <button class="mi" class:current={isCurrent(customLink)} role="menuitemradio" aria-checked={isCurrent(customLink)} tabindex="-1" onclick={() => choose(customLink.url)}>
            <span class="k">{name(customLink)}</span><span class="meta">{t('result.customMeta')}</span>
            {#if isCurrent(customLink)}<span class="ic ic-check s14"></span>{/if}
          </button>
        {/if}
        <button class="mi" role="menuitem" tabindex="-1" onclick={() => choose(searchWeb)}><span class="k">{t('result.searchWeb')}</span></button>
        {#if scrolls}<div class="mfade" aria-hidden="true"></div>{/if}
      </div>
    {/if}
  </div>
{/if}
