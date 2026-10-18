<script lang="ts">
  import type { Candidate, Segment, Track } from '../../engine/types';
  import { t, tm } from '../../i18n';
  import {
    candidateWhere,
    clock,
    cueLink,
    evidenceSummary,
    identifiedCounts,
    sourceSite,
    tierLabel,
    trackFacts,
    type Fact,
  } from '../../models/session';
  import type { ProviderSettings } from '../../settings/settings';
  import type { DetailSubject } from '../details';
  import Artwork from './Artwork.svelte';
  import ContourOverlay from './ContourOverlay.svelte';
  import Facts from './Facts.svelte';
  import OpenInButton from './OpenInButton.svelte';

  /**
   * The app tab's details column (design spec "Details", 300 px at ≥ 1000): the selected
   * track, candidate, mix segment or playlist entry, or a mix or playlist from the library.
   */
  interface Props {
    subject: DetailSubject | null;
    providers: ProviderSettings | null;
    onopen: (url: string) => void;
    oncopy: (text: string) => void;
    onopensession?: (id: string) => void;
  }

  let { subject, providers, onopen, oncopy, onopensession }: Props = $props();

  const label = $derived(
    subject?.kind === 'segment' ? t('session.segmentDetails') : subject?.kind === 'item' ? t('session.trackDetails') : t('session.details'),
  );

  function candidateFacts(candidate: Candidate): Fact[] {
    const facts = trackFacts(candidate.track).filter((f) => f.key === 'result.album' || f.key === 'result.released');
    const source = candidate.sources[0];
    if (source) facts.push({ key: 'result.matchedBy', value: t(source.kind === 'Lyrics' ? 'result.matchedBy.typedLyrics' : source.kind === 'Fingerprint' ? 'result.matchedBy.shazamPlain' : 'result.matchedBy.melody') });
    const where = candidateWhere(candidate);
    if (where) facts.push({ key: 'result.where', value: where });
    return facts;
  }

  function segmentFacts(segment: Segment): Fact[] {
    const facts: Fact[] = [{ key: 'mix.match', value: t(tierLabel(segment)) }];
    const evidence = evidenceSummary(segment);
    if (evidence) facts.push({ key: 'mix.evidence', value: tm(evidence) });
    if (segment.track) facts.push(...trackFacts(segment.track).filter((f) => f.key === 'result.album' || f.key === 'result.released'));
    return facts;
  }
</script>

{#snippet openIn(track: Track)}
  {#if providers}
    <OpenInButton {track} preferred={providers.provider} custom={providers.custom} wide {onopen} />
  {/if}
{/snippet}

<aside class="details-col" aria-label={label}>
  {#if !subject}
    <div class="no-selection">{t('session.noSelection')}</div>
  {:else if subject.kind === 'track' || subject.kind === 'item'}
    {@const track = subject.track}
    <Artwork url={track.artwork_url} size={subject.kind === 'item' ? 64 : 120} />
    <div class="headline center">{track.title}</div>
    <div class="callout secondary center">{track.artists.join(', ') || track.artist}</div>
    <div class="dsec"><Facts facts={trackFacts(track)} /></div>
    {#if subject.kind === 'track' && subject.heard?.length}
      <div class="dsec"><div class="h">{t('result.heard')}</div><Facts facts={subject.heard} /></div>
    {/if}
    <div class="acts">
      {@render openIn(track)}
      <button class="btn outlined" onclick={() => oncopy(`${track.artist} – ${track.title}`)}><span class="ic ic-copy s12"></span>{t('result.copyArtistTitle')}</button>
      {#if subject.kind === 'track' && subject.sessionId}
        <button class="btn text" onclick={() => onopensession?.(subject.sessionId!)}>{t('action.open')}</button>
      {/if}
    </div>
  {:else if subject.kind === 'candidate'}
    {@const candidate = subject.candidate}
    <Artwork url={candidate.track.artwork_url} size={120} />
    <div class="headline center">{candidate.track.title}</div>
    <div class="callout secondary center">{candidate.track.artist}</div>
    {#if candidate.alignment}
      <div class="dsec">
        <div class="h">{t('cand.overlayTitle', candidate.track.title)}</div>
        <ContourOverlay alignment={candidate.alignment} title={candidate.track.title} height={72} />
      </div>
    {/if}
    <div class="dsec"><Facts facts={candidateFacts(candidate)} /></div>
    <div class="acts">
      {@render openIn(candidate.track)}
      {#if subject.sessionId}<button class="btn text" onclick={() => onopensession?.(subject.sessionId!)}>{t('action.open')}</button>{/if}
    </div>
  {:else if subject.kind === 'segment'}
    {@const segment = subject.segment}
    {@const site = sourceSite(subject.detail) ?? ''}
    {@const link = cueLink(subject.detail, segment)}
    {#if segment.track}
      <Artwork url={segment.track.artwork_url} size={64} />
      <div class="headline center">{segment.track.title}</div>
      <div class="callout secondary center">{segment.track.artists.join(', ')}</div>
    {:else}
      <div class="art art-64 unknown" aria-hidden="true"></div>
      <div class="headline center">{t('mix.id')}</div>
    {/if}
    <div class="cue center tnum">{clock(segment.start_ms)} – {clock(segment.end_ms)}</div>
    <div class="dsec"><Facts facts={segmentFacts(segment)} /></div>
    <div class="acts">
      {#if segment.track}{@render openIn(segment.track)}{/if}
      {#if link}<button class="btn outlined" onclick={() => onopen(link)}><span class="ic ic-play s12"></span>{t('mix.openSourceAt', site, clock(segment.start_ms))}</button>{/if}
    </div>
  {:else if subject.kind === 'session'}
    {@const detail = subject.detail}
    {@const counts = identifiedCounts(detail)}
    <Artwork url={detail.entry.artwork_url} size={(detail.source?.item_count ?? 1) > 1 ? 120 : 'video-l'} />
    <div class="headline center">{detail.entry.title}</div>
    <div class="callout secondary center">{[sourceSite(detail), t('mix.identified', counts.identified, counts.total)].filter(Boolean).join(' · ')}</div>
    <div class="acts"><button class="btn filled" onclick={() => onopensession?.(detail.entry.id)}>{t('action.open')}</button></div>
  {/if}
</aside>
