<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import type { ExportFormat, Segment } from '../../engine/types';
  import { t } from '../../i18n';
  import { singleTrack } from '../../models/session';
  import { primaryLink, providerLinks } from '../../providers/links';
  import type { ProviderSettings } from '../../settings/settings';
  import ExportMenu from '../components/ExportMenu.svelte';
  import SessionContent from '../components/SessionContent.svelte';
  import { useSurface } from '../context';
  import type { DetailSubject } from '../details';
  import { heardRows } from '../format';
  import { exportFileName, EXPORT_FORMATS, SessionController } from '../session.svelte';

  /**
   * One session in the app tab: back, title, Export ▾, Pin and Listen in the toolbar; the
   * song, candidates, mix or playlist below; the details column shows the selection.
   */
  interface Props {
    id: string;
    providers: ProviderSettings | null;
    narrow: boolean;
    /** The details column is on screen (≥ 1000 px). */
    columns: boolean;
    onback: () => void;
    onlisten: () => void;
    ondetails: (subject: DetailSubject | null) => void;
  }

  let { id, providers, narrow, columns, onback, onlisten, ondetails }: Props = $props();
  const { engine, platform } = useSurface();
  // svelte-ignore state_referenced_locally
  const session = new SessionController(engine, id);
  let zoom = $state(1);
  const view = $derived(session.view);
  const shape = $derived(session.shape);
  const touch = platform.capabilities.android;

  onMount(() => void session.start());
  onDestroy(() => session.dispose());

  $effect(() => {
    if (!view) return ondetails(null);
    if (shape === 'single') {
      const track = singleTrack(view);
      return ondetails(track ? { kind: 'track', track, heard: heardRows(view.detail) } : null);
    }
    if (shape === 'candidates') {
      const index = Number(session.selectedId?.replace('cand-', '') ?? 0) || 0;
      const candidate = view.detail.candidates[index] ?? view.detail.candidates[0];
      return ondetails(candidate ? { kind: 'candidate', candidate } : null);
    }
    const segment = session.selectedSegment;
    if (shape === 'mix') return ondetails(segment ? { kind: 'segment', segment, detail: view.detail } : null);
    if (shape === 'playlist') return ondetails(segment?.track ? { kind: 'item', track: segment.track } : null);
    ondetails(null);
  });

  async function exportAs(format: ExportFormat) {
    if (!view) return;
    const item = EXPORT_FORMATS.find((f) => f.format === format)!;
    await platform.download(exportFileName(view.detail.entry.title, item.ext), item.mime, await session.export(format));
  }

  function selectedTrack() {
    if (!view) return null;
    if (shape === 'single') return singleTrack(view);
    return session.selectedSegment?.track ?? null;
  }

  function onkeydown(event: KeyboardEvent) {
    const mod = event.metaKey || event.ctrlKey;
    if (!mod || (event.target as HTMLElement | null)?.closest('input, textarea')) return;
    const key = event.key.toLowerCase();
    if (event.shiftKey && key === 'p' && view) {
      event.preventDefault();
      void session.setPinned(!view.detail.entry.pinned);
    } else if (shape === 'mix' && (key === '+' || key === '=')) {
      event.preventDefault();
      zoom = Math.min(8, zoom * 2);
    } else if (shape === 'mix' && key === '-') {
      event.preventDefault();
      zoom = Math.max(1, zoom / 2);
    } else if (shape === 'mix' && key === '0') {
      event.preventDefault();
      zoom = 1;
    } else if (key === 'enter' && providers) {
      const track = selectedTrack();
      const link = track ? primaryLink(providerLinks(track, providers.custom), providers.provider) : null;
      if (link) {
        event.preventDefault();
        void platform.openUrl(link.url);
      }
    } else if (key === 'c' && !window.getSelection()?.toString()) {
      const track = selectedTrack();
      if (track) {
        event.preventDefault();
        void platform.copyText(`${track.artist} – ${track.title}`);
      }
    }
  }

  function humPart(segment: Segment) {
    void segment;
    onlisten();
  }
</script>

<svelte:window {onkeydown} />

<div class="toolbar session-toolbar">
  <button class="btn icon plain" aria-label={t('action.backToLibrary')} onclick={onback}><span class="ic ic-chev-left"></span></button>
  <h1 class="wtitle truncate">{view?.detail.entry.title ?? ''}</h1>
  {#if view}
    {#if shape === 'mix' || shape === 'playlist' || shape === 'single'}
      <ExportMenu onexport={(format) => void exportAs(format)} oncopy={() => void platform.copyText(session.tracklist())} />
    {/if}
    <button class="btn icon plain" class:on={view.detail.entry.pinned} aria-pressed={view.detail.entry.pinned} aria-label={t('result.pin')} onclick={() => void session.setPinned(!view.detail.entry.pinned)}>
      <span class="ic {view.detail.entry.pinned ? 'ic-pin-fill' : 'ic-pin'}"></span>
    </button>
  {/if}
  {#if !narrow}<button class="btn filled" onclick={onlisten}><span class="ic ic-mark listen-mark"></span>{t('action.listen')}</button>{/if}
</div>
<div class="session-scroll" class:mixed={shape === 'mix' || shape === 'playlist'}>
  <SessionContent
    {session}
    {providers}
    layout="app"
    {zoom}
    {touch}
    inlineDetails={!columns}
    onopen={(url) => void platform.openUrl(url)}
    oncopy={(text) => void platform.copyText(text)}
    onhum={humPart}
    onhumagain={onlisten}
  />
</div>
