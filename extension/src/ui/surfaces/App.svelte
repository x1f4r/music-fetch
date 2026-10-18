<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import type { EngineClient } from '../../engine/client';
  import { libraryQuery, type LibraryEntry } from '../../engine/types';
  import { t } from '../../i18n';
  import { isMixEntry } from '../../models/library';
  import { loadProviderSettings, watchProviderSettings, type ProviderSettings } from '../../settings/settings';
  import DetailsColumn from '../components/DetailsColumn.svelte';
  import { provideSurface, type Platform } from '../context';
  import type { DetailSubject } from '../details';
  import { Router, type Route } from '../router.svelte';
  import GrantMicRoute from '../routes/GrantMicRoute.svelte';
  import LibraryRoute from '../routes/LibraryRoute.svelte';
  import ListenRoute from '../routes/ListenRoute.svelte';
  import OpenRoute from '../routes/OpenRoute.svelte';
  import SessionRoute from '../routes/SessionRoute.svelte';
  import SettingsView from '../settings/SettingsView.svelte';
  import WelcomeRoute from '../routes/WelcomeRoute.svelte';

  /**
   * The full-page app tab (extension plan §5.1): ≥ 1000 px a sidebar (Listen, Library,
   * Pinned, Settings), the content and a details column; 700–999 without the details
   * column; under 700 one column with a bottom bar (the Android layout).
   */
  interface Props {
    engine: EngineClient;
    platform: Platform;
  }

  let { engine, platform }: Props = $props();
  // svelte-ignore state_referenced_locally
  provideSurface({ engine, platform });

  const PINNED_IN_SIDEBAR = 12;
  const router = new Router();
  let providers = $state<ProviderSettings | null>(null);
  let pinned = $state<LibraryEntry[]>([]);
  let width = $state(1280);
  let details = $state<DetailSubject | null>(null);
  let stops: (() => void)[] = [];

  const route = $derived(router.route);
  /** Welcome and the grant page are full-bleed: no navigation around them. */
  const bare = $derived(route.name === 'welcome' || route.name === 'grant-mic');
  const narrow = $derived(width < 700);
  const withDetails = $derived(width >= 1000 && (route.name === 'library' || route.name === 'session'));
  const pinnedTitle = (entry: LibraryEntry) => entry.title;

  async function loadPinned() {
    pinned = await engine.library(libraryQuery({ pinned_only: true, limit: 50 })).catch(() => []);
  }

  onMount(async () => {
    providers = await loadProviderSettings();
    stops.push(watchProviderSettings((value) => (providers = value)));
    stops.push(engine.subscribe({ libraryChanged: () => void loadPinned() }));
    await loadPinned();
    if (providers.needsPanel && !bare) router.go({ name: 'welcome' }, true);
  });

  onDestroy(() => stops.forEach((stop) => stop()));

  function is(name: Route['name']): boolean {
    return route.name === name;
  }

  function go(next: Route) {
    details = null;
    router.go(next);
  }

  function href(next: Route): string {
    return next.name === 'session' ? `#/session/${encodeURIComponent(next.id)}` : next.name === 'library' && next.pinned ? '#/library/pinned' : `#/${next.name}`;
  }
</script>

{#snippet nav(item: Route, icon: string, label: string, on: boolean)}
  <a class="ni" class:on href={href(item)} aria-current={on ? 'page' : undefined} onclick={(e) => (e.preventDefault(), go(item))}>
    <span class="ic {icon}"></span><span class="truncate">{label}</span>
  </a>
{/snippet}

<div class="app" class:bare class:narrow bind:clientWidth={width}>
  {#if !bare && !narrow}
    <nav class="sidebar" aria-label={t('nav.main')}>
      <div class="brand"><span class="mini" aria-hidden="true"><span class="ic ic-mark"></span></span>{t('app.name')}</div>
      <div class="nav-list">
        {@render nav({ name: 'listen' }, 'ic-hum', t('nav.listen'), is('listen'))}
        {@render nav({ name: 'library', pinned: false }, 'ic-stack', t('nav.library'), is('library') || (route.name === 'session' && !pinned.some((e) => e.id === route.id)))}
        {#if pinned.length > 0}
          <div class="nav-sec" id="nav-pinned">{t('nav.pinned')}</div>
          {#each pinned.slice(0, PINNED_IN_SIDEBAR) as entry (entry.id)}
            {@render nav({ name: 'session', id: entry.id }, isMixEntry(entry) ? 'ic-mix' : entry.kind === 'Hum' ? 'ic-hum' : 'ic-note', pinnedTitle(entry), route.name === 'session' && route.id === entry.id)}
          {/each}
          {#if pinned.length > PINNED_IN_SIDEBAR}
            <a class="ni more" href="#/library/pinned" onclick={(e) => (e.preventDefault(), go({ name: 'library', pinned: true }))}><span class="ic"></span>{t('nav.showAll')}</a>
          {/if}
        {/if}
      </div>
      <div class="grow"></div>
      <div class="nav-list">
        {@render nav({ name: 'settings', pane: 'general' }, 'ic-gear', t('nav.settings'), is('settings'))}
      </div>
    </nav>
  {/if}
  <main class="content">
    {#if route.name === 'welcome'}
      <WelcomeRoute {providers} onfinish={() => go({ name: 'listen' })} />
    {:else if route.name === 'grant-mic'}
      <GrantMicRoute initial={route.state} />
    {:else if route.name === 'library'}
      {#key route.pinned}
        <LibraryRoute {narrow} columns={withDetails} pinnedOnly={route.pinned} onopen={(id) => go({ name: 'session', id })} onlisten={() => go({ name: 'listen' })} onsettings={() => go({ name: 'settings', pane: 'general' })} ondetails={(subject) => (details = subject)} />
      {/key}
    {:else if route.name === 'session'}
      {#key route.id}
        <SessionRoute id={route.id} {providers} {narrow} columns={withDetails} onback={() => go({ name: 'library', pinned: false })} onlisten={() => go({ name: 'listen' })} ondetails={(subject) => (details = subject)} />
      {/key}
    {:else if route.name === 'settings'}
      <SettingsView pane={route.pane} {providers} {narrow} onpane={(pane) => go({ name: 'settings', pane })} />
    {:else if route.name === 'open'}
      <OpenRoute url={route.url} onstarted={(id) => router.go({ name: 'session', id }, true)} />
    {:else}
      <ListenRoute {providers} {narrow} onopen={(id) => go({ name: 'session', id })} onsettings={() => go({ name: 'settings', pane: 'general' })} />
    {/if}
  </main>
  {#if withDetails}
    <DetailsColumn
      subject={details}
      {providers}
      onopen={(url) => void platform.openUrl(url)}
      oncopy={(text) => void platform.copyText(text)}
      onopensession={(id) => go({ name: 'session', id })}
    />
  {/if}
  {#if narrow && !bare}
    <nav class="bottombar" aria-label={t('nav.main')}>
      <a class="bi" class:on={is('listen')} href="#/listen" aria-current={is('listen') ? 'page' : undefined}><span class="pill"><span class="ic ic-mark bar-mark"></span></span>{t('nav.listen')}</a>
      <a class="bi" class:on={is('library') || is('session')} href="#/library" aria-current={is('library') ? 'page' : undefined}><span class="pill"><span class="ic ic-stack s20"></span></span>{t('nav.library')}</a>
    </nav>
  {/if}
</div>
