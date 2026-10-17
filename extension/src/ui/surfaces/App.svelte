<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import type { EngineClient } from '../../engine/client';
  import { libraryQuery, type LibraryEntry } from '../../engine/types';
  import { t } from '../../i18n';
  import { loadProviderSettings, watchProviderSettings, type ProviderSettings } from '../../settings/settings';
  import { provideSurface, type Platform } from '../context';
  import { Router, type Route } from '../router.svelte';
  import GrantMicRoute from '../routes/GrantMicRoute.svelte';
  import LibraryRoute from '../routes/LibraryRoute.svelte';
  import ListenRoute from '../routes/ListenRoute.svelte';
  import OpenRoute from '../routes/OpenRoute.svelte';
  import SessionRoute from '../routes/SessionRoute.svelte';
  import SettingsRoute from '../routes/SettingsRoute.svelte';
  import WelcomeRoute from '../routes/WelcomeRoute.svelte';

  /** The full-page app tab: Listen, Library, sessions, Settings, first run and the microphone grant. */
  interface Props {
    engine: EngineClient;
    platform: Platform;
  }

  let { engine, platform }: Props = $props();
  // svelte-ignore state_referenced_locally
  provideSurface({ engine, platform });

  const router = new Router();
  let providers = $state<ProviderSettings | null>(null);
  let pinned = $state<LibraryEntry[]>([]);
  let stops: (() => void)[] = [];

  const route = $derived(router.route);
  /** Welcome and the grant page are full-bleed: no navigation around them. */
  const bare = $derived(route.name === 'welcome' || route.name === 'grant-mic');

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
    router.go(next);
  }
</script>

{#snippet nav(item: Route, icon: string, label: string, on: boolean)}
  <a class="ni" class:on href={`#/${item.name}`} aria-current={on ? 'page' : undefined} onclick={(e) => (e.preventDefault(), go(item))}>
    <span class="ic {icon}"></span><span class="truncate">{label}</span>
  </a>
{/snippet}

<div class="app" class:bare>
  {#if !bare}
    <aside class="sidebar">
      <div class="brand"><span class="mini" aria-hidden="true"><span class="ic ic-mark"></span></span>{t('app.name')}</div>
      <nav class="nav-list" aria-label={t('app.name')}>
        {@render nav({ name: 'listen' }, 'ic-hum', t('nav.listen'), is('listen'))}
        {@render nav({ name: 'library', pinned: false }, 'ic-stack', t('nav.library'), is('library'))}
        {#if pinned.length > 0}
          <div class="nav-sec">{t('nav.pinned')}</div>
          {#each pinned as entry (entry.id)}
            {@render nav({ name: 'session', id: entry.id }, entry.track_count > 1 ? 'ic-mix' : 'ic-note', entry.title, route.name === 'session' && route.id === entry.id)}
          {/each}
        {/if}
      </nav>
      <div class="grow"></div>
      <nav class="nav-list">
        {@render nav({ name: 'settings', pane: 'general' }, 'ic-gear', t('nav.settings'), is('settings'))}
      </nav>
    </aside>
  {/if}
  <main class="content">
    {#if route.name === 'welcome'}
      <WelcomeRoute {providers} onfinish={() => go({ name: 'listen' })} />
    {:else if route.name === 'grant-mic'}
      <GrantMicRoute />
    {:else if route.name === 'library'}
      <LibraryRoute onopen={(id) => go({ name: 'session', id })} />
    {:else if route.name === 'session'}
      {#key route.id}
        <SessionRoute id={route.id} {providers} onback={() => go({ name: 'library', pinned: false })} />
      {/key}
    {:else if route.name === 'settings'}
      <SettingsRoute pane={route.pane} {providers} onpane={(pane) => go({ name: 'settings', pane })} />
    {:else if route.name === 'open'}
      <OpenRoute url={route.url} onstarted={(id) => router.go({ name: 'session', id }, true)} />
    {:else}
      <ListenRoute {providers} onopen={(id) => go({ name: 'session', id })} />
    {/if}
  </main>
  {#if !bare}
    <nav class="bottombar" aria-label={t('app.name')}>
      <a class="bi" class:on={is('listen')} href="#/listen"><span class="pill"><span class="ic ic-hum"></span></span>{t('nav.listen')}</a>
      <a class="bi" class:on={is('library') || is('session')} href="#/library"><span class="pill"><span class="ic ic-stack"></span></span>{t('nav.library')}</a>
      <a class="bi" class:on={is('settings')} href="#/settings/general"><span class="pill"><span class="ic ic-gear"></span></span>{t('nav.settings')}</a>
    </nav>
  {/if}
</div>
