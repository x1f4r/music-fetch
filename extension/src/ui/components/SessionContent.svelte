<script lang="ts">
  import type { Segment } from '../../engine/types';
  import { t } from '../../i18n';
  import { cueLink, singleTrack } from '../../models/session';
  import { primaryLink, providerLinks } from '../../providers/links';
  import type { ProviderSettings } from '../../settings/settings';
  import { heardRows } from '../format';
  import type { SessionController } from '../session.svelte';
  import CandidatesView from './CandidatesView.svelte';
  import MixHeader from './MixHeader.svelte';
  import ResultView from './ResultView.svelte';
  import Timeline from './Timeline.svelte';
  import Tracklist from './Tracklist.svelte';

  /**
   * A session's content in the side panel and the app tab: a song, candidates, a mix
   * (header, status row, timeline, tracklist) or a playlist (DESIGN §6.3–6.6).
   */
  interface Props {
    session: SessionController;
    providers: ProviderSettings | null;
    layout: 'panel' | 'app';
    zoom?: number;
    onopen: (url: string) => void;
    oncopy: (text: string) => void;
    /** "Hum it" on an unidentified part. */
    onhum?: (segment: Segment) => void;
    /** "Hum again" / "Type a lyric" on candidates. */
    onhumagain?: () => void;
    ontypelyric?: () => void;
    onshare?: () => void;
    touch?: boolean;
    /** The app tab without its details column (narrow windows) shows the song's details inline. */
    inlineDetails?: boolean;
  }

  let { session, providers, layout, zoom = 1, onopen, oncopy, onhum, onhumagain, ontypelyric, onshare, touch = false, inlineDetails = false }: Props = $props();
  const view = $derived(session.view);
  const shape = $derived(session.shape);
  const app = $derived(layout === 'app');
  let candidateIndex = $state(0);

  $effect(() => {
    // The details column follows the selected candidate.
    if (shape === 'candidates') session.selectedId = `cand-${candidateIndex}`;
  });

  function play(segment: Segment) {
    // Player control in the source tab is the capture coordinator's (X4); until then the deep link.
    const link = view ? cueLink(view.detail, segment) : null;
    if (link) onopen(link);
  }

  function activate(id: string) {
    const track = view?.detail.segments.find((s) => s.id === id)?.track;
    if (!track || !providers) return;
    session.selectedId = id;
    const link = primaryLink(providerLinks(track, providers.custom), providers.provider);
    if (link) onopen(link.url);
  }
</script>

{#if session.missing}
  <div class="empty"><div class="headline">{t('session.notFound')}</div></div>
{:else if view && providers}
  {#if shape === 'single'}
    {@const track = singleTrack(view)}
    {#if track}
      <div class="session-pane single">
        <ResultView
          {track}
          preferred={providers.provider}
          custom={providers.custom}
          artSize={app ? 200 : 120}
          pinned={view.detail.entry.pinned}
          details={!app || inlineDetails}
          heard={heardRows(view.detail)}
          {touch}
          {onopen}
          {oncopy}
          {onshare}
          onpin={app ? undefined : (pinned) => void session.setPinned(pinned)}
        />
      </div>
    {/if}
  {:else if shape === 'candidates'}
    <div class="session-pane">
      <CandidatesView
        candidates={view.detail.candidates}
        bind:selected={candidateIndex}
        humStrip={view.detail.entry.kind === 'Hum'}
        onchoose={(c) => void session.confirmCandidate(c.track)}
        {onhumagain}
        {ontypelyric}
      />
    </div>
  {:else if shape === 'mix' || shape === 'playlist'}
    <MixHeader {view} {shape} large={app} onstop={() => void session.stopAnalysis()} />
    {#if shape === 'mix'}
      <Timeline {view} selectedId={session.selectedId} {zoom} onselect={(id) => (session.selectedId = id)} />
    {/if}
    <div class={app ? 'pane' : 'tracks-scroll'}>
      <Tracklist
        {view}
        kind={shape}
        selectedId={session.selectedId}
        onselect={(id) => (session.selectedId = id)}
        onplay={play}
        onconfirm={(s) => void session.confirm(s)}
        onreject={(s) => void session.reject(s)}
        {onhum}
        onactivate={activate}
      />
    </div>
  {:else}
    <div class="empty">
      {#if view.running}
        <span class="spinner" aria-hidden="true"></span>
        <div class="headline">{t('session.running')}</div>
      {:else}
        <span class="ic ic-mark"></span>
        <div class="headline">{view.detail.entry.status === 'Failed' ? t('library.failed') : view.detail.entry.status === 'Waiting' ? t('library.waiting') : t('state.noMatch')}</div>
      {/if}
    </div>
  {/if}
{/if}
