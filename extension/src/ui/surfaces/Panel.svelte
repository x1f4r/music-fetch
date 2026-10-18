<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import type { EngineClient } from '../../engine/client';
  import type { LibraryEntry, Segment } from '../../engine/types';
  import { t } from '../../i18n';
  import { isRunning, showsResult } from '../../models/stage';
  import { suggestProvider } from '../../providers/services';
  import AskSheet from '../components/AskSheet.svelte';
  import CandidatesView from '../components/CandidatesView.svelte';
  import ExportMenu from '../components/ExportMenu.svelte';
  import LibraryList from '../components/LibraryList.svelte';
  import LinkField from '../components/LinkField.svelte';
  import ProviderPicker from '../components/ProviderPicker.svelte';
  import ResultView from '../components/ResultView.svelte';
  import SessionContent from '../components/SessionContent.svelte';
  import Stage from '../components/Stage.svelte';
  import { provideSurface, type Platform } from '../context';
  import { LibraryController } from '../library.svelte';
  import { exportFileName, EXPORT_FORMATS, SessionController } from '../session.svelte';
  import { ListenSurface } from '../surface.svelte';

  /**
   * The Chromium side panel and the Firefox sidebar (320–560 wide, full height): the
   * listen stage on top and the recent sessions beneath; a session opens in place with its
   * own head row and footer. It stays open across tabs, so long hums and mixes live here.
   */
  interface Props {
    engine: EngineClient;
    platform: Platform;
  }

  let { engine, platform }: Props = $props();
  // svelte-ignore state_referenced_locally
  provideSurface({ engine, platform });
  // svelte-ignore state_referenced_locally
  const surface = new ListenSurface(engine, platform, { recentLimit: 50 });
  // svelte-ignore state_referenced_locally
  const library = new LibraryController(engine, { limit: 50, pinnedSection: false });
  const listen = surface.listen;
  let field = $state<LinkField | undefined>();
  let width = $state(360);
  /** The session pushed over the list. */
  let session = $state<SessionController | null>(null);
  let opener: HTMLElement | null = null;

  const stage = $derived(listen.stage);
  const providers = $derived(surface.providers);
  /** Firefox lets the sidebar shrink to about 200 px: below 320 the toggles drop their text. */
  const narrow = $derived(width < 320);
  const view = $derived(session?.view ?? null);

  onMount(() => {
    void surface.start();
    void library.start();
  });
  onDestroy(() => {
    surface.stop();
    library.dispose();
    session?.dispose();
  });

  function openSession(entry: LibraryEntry) {
    opener = document.activeElement as HTMLElement | null;
    session?.dispose();
    session = new SessionController(engine, entry.id);
    void session.start();
  }

  function closeSession() {
    session?.dispose();
    session = null;
    queueMicrotask(() => opener?.focus());
  }

  async function chooseCandidate(index: number) {
    const id = stage.sessionId;
    const candidate = stage.candidates[index];
    if (!id || !candidate) return;
    await engine.confirmCandidate(id, candidate.track);
    listen.stage = { ...stage, phase: 'matched', track: candidate.track };
  }

  async function exportAs(format: (typeof EXPORT_FORMATS)[number]['format']) {
    if (!session || !view) return;
    const item = EXPORT_FORMATS.find((f) => f.format === format)!;
    const text = await session.export(format);
    await platform.download(exportFileName(view.detail.entry.title, item.ext), item.mime, text);
  }

  function humPart(segment: Segment) {
    void segment;
    closeSession();
    void listen.listenWith('Hum');
  }

  function onkeydown(event: KeyboardEvent) {
    if (event.key !== 'Escape' || event.defaultPrevented) return;
    if (session) {
      event.preventDefault();
      closeSession();
    } else if (showsResult(stage)) {
      event.preventDefault();
      listen.reset();
    } else if (isRunning(stage)) {
      event.preventDefault();
      void listen.cancel();
    }
  }

  function onpaste(event: ClipboardEvent) {
    if ((event.target as HTMLElement | null)?.closest('input, textarea')) return;
    if (surface.pasted(event.clipboardData?.getData('text') ?? '')) event.preventDefault();
  }
</script>

<svelte:window onfocus={() => void surface.refreshTab()} {onkeydown} {onpaste} />

<main class="panel" class:narrow bind:clientWidth={width}>
  <h1 class="sr-only">{t('app.name')}</h1>
  {#if !providers}
    <!-- Waiting for the stored provider decision. -->
  {:else if providers.needsPanel}
    <div class="first-run">
      <ProviderPicker compact suggested={suggestProvider(surface.tab?.url)} savedCustom={providers.custom} />
    </div>
  {:else if session}
    <div class="head">
      <button class="btn icon plain" aria-label={t('action.backTo')} onclick={closeSession}><span class="ic ic-chev-left"></span></button>
      <span class="wtitle truncate">{view?.detail.entry.title ?? ''}</span>
      {#if view}
        <button class="btn icon plain" class:on={view.detail.entry.pinned} aria-pressed={view.detail.entry.pinned} aria-label={t('result.pin')} onclick={() => void session?.setPinned(!view.detail.entry.pinned)}>
          <span class="ic {view.detail.entry.pinned ? 'ic-pin-fill' : 'ic-pin'}"></span>
        </button>
      {/if}
    </div>
    <div class="session-body">
      <SessionContent
        {session}
        {providers}
        layout="panel"
        onopen={(url) => void platform.openUrl(url)}
        oncopy={(text) => void platform.copyText(text)}
        onhum={humPart}
      />
    </div>
    <nav class="footer" aria-label={t('nav.main')}>
      {#if session.shape === 'mix' || session.shape === 'playlist'}
        <ExportMenu quiet up onexport={(format) => void exportAs(format)} oncopy={() => void platform.copyText(session?.tracklist() ?? '')} />
        {#if !narrow}<button class="btn text quiet" onclick={() => void platform.copyText(session?.tracklist() ?? '')}><span class="ic ic-copy"></span>{t('export.copyTracklist')}</button>{/if}
      {/if}
      <span class="grow"></span>
      <button class="btn text quiet" aria-label={narrow ? t('nav.openInTab') : undefined} onclick={() => void platform.openApp(`/session/${session?.id}`)}><span class="ic ic-expand"></span>{narrow ? '' : t('nav.openInTab')}</button>
    </nav>
  {:else}
    <div class="top">
      {#if showsResult(stage)}
        <button class="back btn icon plain" aria-label={t('action.back')} onclick={() => listen.reset()}><span class="ic ic-chev-left"></span></button>
      {/if}
      <LinkField bind:this={field} onsubmit={(text) => surface.submit(text)} />
    </div>
    {#if showsResult(stage)}
      <div class="result-scroll">
        {#if stage.phase === 'matched' && stage.track}
          <ResultView
            track={stage.track}
            preferred={providers.provider}
            custom={providers.custom}
            artSize={120}
            context={surface.resultContext}
            onopen={(url) => void platform.openUrl(url)}
            oncopy={(text) => void platform.copyText(text)}
            onpin={(value) => stage.sessionId && void engine.setPinned(stage.sessionId, value)}
          />
        {:else}
          <CandidatesView
            candidates={stage.candidates}
            onchoose={(c) => void chooseCandidate(stage.candidates.indexOf(c))}
            onhumagain={() => void listen.listenWith('Hum')}
            ontypelyric={() => {
              listen.reset();
              queueMicrotask(() => field?.focus());
            }}
          />
        {/if}
      </div>
    {:else}
      <div class="chip-slot">
        {#if surface.chipVisible}
          <button class="chip" class:on={surface.ask !== null} aria-pressed={surface.ask !== null} onclick={() => void surface.analyzePage()}>
            <span class="ic ic-mix s14"></span><span>{t('chip.analyzePage')} <span class="host">· {surface.pageHost}</span></span>
          </button>
        {/if}
      </div>
      <div class="stage">
        <Stage
          {listen}
          sources={surface.sources}
          iconsOnly={narrow}
          ontypelyric={() => field?.focus()}
          onopensession={(id) => {
            const entry = library.entries.find((e) => e.id === id);
            if (entry) openSession(entry);
          }}
          onallowmic={() => void platform.openApp('/grant-mic')}
          onsitesettings={() => void platform.openSiteSettings()}
        />
      </div>
      <div class="list">
        <LibraryList {library} chevrons label={t('library.recent')} onopen={openSession} oncopy={(text) => void platform.copyText(text)} />
      </div>
    {/if}
    <nav class="footer" aria-label={t('nav.main')}>
      <button class="btn text quiet" onclick={() => void platform.openApp('/library')}><span class="ic ic-stack"></span>{t('nav.library')}</button>
      {#if !narrow}<button class="btn text quiet" onclick={() => void platform.openApp('/listen')}><span class="ic ic-expand"></span>{t('nav.openInTab')}</button>{/if}
      <span class="grow"></span>
      <button class="btn icon plain" aria-label={t('nav.settings')} onclick={() => void platform.openApp('/settings/general')}><span class="ic ic-gear"></span></button>
    </nav>
  {/if}
  {#if surface.ask}
    <AskSheet
      title={surface.ask.title}
      site={surface.ask.site}
      durationMs={surface.ask.durationMs}
      thumbnail={null}
      oncancel={() => (surface.ask = null)}
      onanalyze={(choice, remember) => void surface.answerAsk(choice, remember)}
    />
  {/if}
</main>
