<script lang="ts">
  import type { LibraryEntry } from '../../engine/types';
  import { t } from '../../i18n';
  import { kindIcon, rowSubtitle, rowTime } from '../../models/library';
  import { text } from '../format';
  import Artwork from './Artwork.svelte';

  /**
   * One library row (DESIGN §6.8): artwork, title, the kind symbol and subtitle, the time
   * and the pin. In the app tab a checkbox leads it (on hover and while selecting); in the
   * side panel a chevron trails it. The list around it owns focus and keys (listbox).
   */
  interface Props {
    entry: LibraryEntry;
    candidateCount?: number | null;
    /** Under a day heading (the time is a clock), or in Pinned (a day for older rows). */
    inDaySection?: boolean;
    headingId?: string;
    selectable?: boolean;
    checked?: boolean;
    /** The keyboard cursor (aria-activedescendant) and the details column's row. */
    current?: boolean;
    chevron?: boolean;
    art?: 40 | 44;
    now?: number;
    onclick: (event: MouseEvent) => void;
    ontoggle?: () => void;
  }

  let {
    entry,
    candidateCount = null,
    inDaySection = true,
    headingId,
    selectable = false,
    checked = false,
    current = false,
    chevron = false,
    art = 40,
    now = Date.now(),
    onclick,
    ontoggle,
  }: Props = $props();

  const subtitle = $derived(text(rowSubtitle(entry, candidateCount)));
  const time = $derived(text(rowTime(entry, now, inDaySection)));
  const failed = $derived(entry.status === 'NoMatch' || entry.status === 'Failed');
  const title = $derived(entry.status === 'NoMatch' ? t('library.noMatch') : entry.title);
  const label = $derived([title, subtitle, time, entry.pinned ? t('library.pinnedRow') : ''].filter(Boolean).join(', '));
</script>

<!-- svelte-ignore a11y_click_events_have_key_events -->
<div
  class="row"
  class:selected={selectable ? checked : current}
  class:current
  id="row-{entry.id}"
  data-id={entry.id}
  role="option"
  aria-selected={selectable ? checked : current}
  aria-label={label}
  aria-describedby={headingId}
  tabindex="-1"
  {onclick}
>
  {#if selectable}
    <!-- The row's aria-selected carries the state; the box is for the pointer. -->
    <!-- svelte-ignore a11y_no_static_element_interactions -->
    <span class="cb" class:on={checked} aria-hidden="true" onclick={(e) => (e.stopPropagation(), ontoggle?.())}>{#if checked}<span class="ic ic-check"></span>{/if}</span>
  {/if}
  <Artwork url={entry.artwork_url} size={art} />
  <div class="text">
    <div class="truncate" class:secondary={failed}>{title}</div>
    {#if subtitle}
      <div class="callout secondary truncate"><span class="ic {kindIcon(entry)} s12 kind"></span>{subtitle}</div>
    {/if}
  </div>
  <div class="trail">
    {#if entry.status === 'Running'}<span class="spinner small" aria-hidden="true"></span>{/if}
    <span class="callout tnum">{time}</span>
    {#if entry.pinned}<span class="ic ic-pin-fill s14 pin" aria-hidden="true"></span>{/if}
    {#if chevron}<span class="ic ic-chev-right s14" aria-hidden="true"></span>{/if}
  </div>
</div>
