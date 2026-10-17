<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import type { Segment, SessionDetail } from '../../engine/types';
  import { t } from '../../i18n';
  import type { ProviderSettings } from '../../settings/settings';
  import Artwork from '../components/Artwork.svelte';
  import CandidatesView from '../components/CandidatesView.svelte';
  import OpenInButton from '../components/OpenInButton.svelte';
  import ResultView from '../components/ResultView.svelte';
  import { useSurface } from '../context';

  /**
   * One session in the app tab: the result for a single track, the candidates for a hum,
   * or the tracklist for a playlist or mix (the timeline strip and row actions are X8).
   */
  interface Props {
    id: string;
    providers: ProviderSettings | null;
    onback: () => void;
  }

  let { id, providers, onback }: Props = $props();
  const { engine, platform } = useSurface();
  let detail = $state<SessionDetail | null>(null);
  let missing = $state(false);
  let stop: (() => void) | null = null;

  const tracks = $derived((detail?.segments ?? []).filter((s): s is Segment & { track: NonNullable<Segment['track']> } => s.track !== null));
  const single = $derived(detail?.entry.track ?? (tracks.length === 1 && (detail?.source?.item_count ?? 1) <= 1 ? tracks[0]!.track : null));

  async function load() {
    detail = await engine.session(id).catch(() => null);
    missing = detail === null;
  }

  onMount(() => {
    stop = engine.subscribe({
      libraryChanged: () => void load(),
      sessionEvent: (sessionId) => {
        if (sessionId === id) void load();
      },
    });
    void load();
  });
  onDestroy(() => stop?.());

  function clock(ms: number): string {
    const s = Math.floor(ms / 1000);
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const ss = String(s % 60).padStart(2, '0');
    return h > 0 ? `${h}:${String(m).padStart(2, '0')}:${ss}` : `${m}:${ss}`;
  }

  async function pin(pinned: boolean) {
    await engine.setPinned(id, pinned);
    await load();
  }
</script>

<div class="toolbar">
  <button class="btn icon plain" aria-label={t('nav.library')} onclick={onback}><span class="ic ic-chev-left"></span></button>
  <span class="wtitle truncate">{detail?.entry.title ?? ''}</span>
  <span class="grow"></span>
  {#if detail}
    <button class="btn icon plain" aria-pressed={detail.entry.pinned} aria-label={detail.entry.pinned ? t('result.unpin') : t('result.pin')} onclick={() => void pin(!detail!.entry.pinned)}>
      <span class="ic {detail.entry.pinned ? 'ic-pin-fill' : 'ic-pin'}"></span>
    </button>
  {/if}
</div>
<div class="scroll-area">
  {#if missing}
    <div class="empty"><div class="headline">{t('session.notFound')}</div></div>
  {:else if detail && providers}
    <div class="session-pane">
      {#if single}
        <ResultView
          track={single}
          preferred={providers.provider}
          custom={providers.custom}
          artSize={200}
          onopen={(url) => void platform.openUrl(url)}
          oncopy={(text) => void platform.copyText(text)}
        />
      {:else if detail.candidates.length > 0}
        <CandidatesView candidates={detail.candidates} onchoose={(c) => void engine.confirmCandidate(id, c.track).then(load)} />
      {:else if tracks.length > 0}
        <div class="tracklist">
          {#each tracks as segment (segment.id)}
            <div class="row track">
              <span class="tc">{clock(segment.start_ms)}</span>
              <Artwork url={segment.track.artwork_url} size={32} />
              <div class="text">
                <div class="headline truncate">{segment.track.title}</div>
                <div class="callout secondary truncate">{segment.track.artist}</div>
              </div>
              <div class="trail">
                <OpenInButton track={segment.track} preferred={providers.provider} custom={providers.custom} small onopen={(url) => void platform.openUrl(url)} />
              </div>
            </div>
          {/each}
        </div>
      {:else}
        <div class="empty"><div class="headline">{detail.entry.status === 'Running' ? t('library.running') : t('state.noMatch')}</div></div>
      {/if}
    </div>
  {/if}
</div>
