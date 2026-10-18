<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import type { LibraryEntry, SessionDetail } from '../../engine/types';
  import { t } from '../../i18n';
  import { LIBRARY_FILTERS, type LibraryFilter } from '../../models/library';
  import { sessionShape, sessionView, singleTrack } from '../../models/session';
  import ContextBar from '../components/ContextBar.svelte';
  import LibraryList from '../components/LibraryList.svelte';
  import Sheet from '../components/Sheet.svelte';
  import Toast from '../components/Toast.svelte';
  import { useSurface } from '../context';
  import type { DetailSubject } from '../details';
  import { heardRows } from '../format';
  import { LibraryController } from '../library.svelte';

  /**
   * The library (DESIGN §6.8, extension plan §5.7): Pinned on top, day groups, filters,
   * search, multi-select with the context bar, Undo and delete confirmation; the details
   * column shows the focused session or the batch actions.
   */
  interface Props {
    narrow: boolean;
    pinnedOnly?: boolean;
    onopen: (sessionId: string) => void;
    onlisten: () => void;
    onsettings: () => void;
    ondetails: (subject: DetailSubject | null) => void;
  }

  let { narrow, pinnedOnly = false, onopen, onlisten, onsettings, ondetails }: Props = $props();
  const { engine, platform } = useSurface();
  const library = new LibraryController(engine);
  let searchOpen = $state(false);
  let searchField = $state<HTMLInputElement | undefined>();
  let focusedDetail = $state<SessionDetail | null>(null);
  const touch = platform.capabilities.android;

  // svelte-ignore state_referenced_locally
  if (pinnedOnly) library.setFilter('pinned');

  onMount(() => void library.start());
  onDestroy(() => library.dispose());

  // The details column follows the focused row, or shows the batch actions.
  $effect(() => {
    const id = library.focusedId;
    if (!id) {
      focusedDetail = null;
      return;
    }
    void engine.session(id).then((detail) => {
      if (library.focusedId === id) focusedDetail = detail;
    });
  });

  $effect(() => {
    // While selecting, the column still shows the focused (last clicked) session; batch
    // actions live in the context bar (design spec "Details").
    const detail = focusedDetail;
    if (!detail) return ondetails(null);
    const view = sessionView(detail);
    const shape = sessionShape(view);
    if (shape === 'single') {
      const track = singleTrack(view);
      return ondetails(track ? { kind: 'track', track, heard: heardRows(detail) } : null);
    }
    if (shape === 'candidates' && detail.candidates[0]) return ondetails({ kind: 'candidate', candidate: detail.candidates[0] });
    if (shape === 'mix' || shape === 'playlist') return ondetails({ kind: 'session', detail });
    ondetails(null);
  });

  async function copy(entries: LibraryEntry[] = library.selected) {
    const text = library.copyText(entries);
    if (touch && (await platform.share({ title: t('app.name'), text }))) return;
    await platform.copyText(text);
  }

  function onkeydown(event: KeyboardEvent) {
    const mod = event.metaKey || event.ctrlKey;
    if (mod && event.key.toLowerCase() === 'f') {
      event.preventDefault();
      searchOpen = true;
      queueMicrotask(() => searchField?.focus());
    } else if (mod && event.shiftKey && event.key.toLowerCase() === 'p') {
      event.preventDefault();
      const targets = library.selecting ? library.selected : library.focused ? [library.focused] : [];
      void library.togglePinned(targets);
    }
  }

  const FILTERS = LIBRARY_FILTERS.map((id) => ({ id, label: t(`library.filter.${id}` as const) }));
  const confirmTitle = $derived.by(() => {
    const entries = library.confirmDelete;
    if (!entries) return '';
    return entries.length === 1 ? t('session.deleteConfirm.title', entries[0]!.title) : t('session.deleteConfirm.many', entries.length);
  });
</script>

<svelte:window {onkeydown} />

{#snippet filterButtons(chips: boolean)}
  {#each FILTERS as item (item.id)}
    <button class={chips ? 'fchip' : ''} class:on={library.filter === item.id} role="radio" aria-checked={library.filter === item.id} onclick={() => library.setFilter(item.id as LibraryFilter)}>{item.label}</button>
  {/each}
{/snippet}

{#snippet searchInput(width: string)}
  <label class="field small search" style:width={width}>
    <span class="ic ic-search s14 secondary"></span>
    <input bind:this={searchField} value={library.search} oninput={(e) => void library.setSearch(e.currentTarget.value)} placeholder={t('library.search')} aria-label={t('library.searchLabel')} type="search" />
  </label>
{/snippet}

{#if narrow}
  <div class="toolbar">
    {#if searchOpen}
      {@render searchInput('100%')}
      <button class="btn text" onclick={() => ((searchOpen = false), void library.setSearch(''))}>{t('action.cancel')}</button>
    {:else}
      <h1 class="wtitle">{t('library.title')}</h1>
      <span class="grow"></span>
      <button class="btn icon plain" aria-label={t('library.searchLabel')} onclick={() => ((searchOpen = true), queueMicrotask(() => searchField?.focus()))}><span class="ic ic-search"></span></button>
      <button class="btn icon plain" aria-label={t('nav.settings')} onclick={onsettings}><span class="ic ic-gear"></span></button>
    {/if}
  </div>
  {#if library.selecting}
    <ContextBar count={library.selection.size} pin={library.pinLabel} {touch} onpin={() => void library.togglePinned()} oncopy={() => void copy()} ondelete={() => void library.requestDelete()} onselectall={() => library.selectAll()} onclear={() => library.clearSelection()} />
  {:else}
    <div class="filters" role="radiogroup" aria-label={t('library.filter')}>{@render filterButtons(true)}</div>
  {/if}
{:else}
  <div class="toolbar">
    <h1 class="wtitle">{t('library.title')}</h1>
    <span class="grow"></span>
    <div class="seg" role="radiogroup" aria-label={t('library.filter')}>{@render filterButtons(false)}</div>
    <span class="grow"></span>
    {@render searchInput('200px')}
    <button class="btn filled" onclick={onlisten}><span class="ic ic-mark listen-mark"></span>{t('action.listen')}</button>
  </div>
  {#if library.selecting}
    <ContextBar count={library.selection.size} pin={library.pinLabel} onpin={() => void library.togglePinned()} oncopy={() => void copy()} ondelete={() => void library.requestDelete()} onselectall={() => library.selectAll()} onclear={() => library.clearSelection()} />
  {/if}
{/if}
<div class="pane">
  <div class="list">
    <LibraryList {library} selectable art={narrow ? 44 : 40} onopen={(entry) => onopen(entry.id)} oncopy={(text) => void platform.copyText(text)} />
  </div>
  {#if library.toast}
    <Toast toast={library.toast} onundo={() => void library.undo()} />
  {/if}
</div>
{#if library.confirmDelete}
  <Sheet title={confirmTitle} placement="center" onclose={() => (library.confirmDelete = null)}>
    <div class="secondary">{t(library.confirmDelete.some((e) => e.pinned) ? 'session.deleteConfirm.pinned' : 'session.deleteConfirm.message')}</div>
    {#snippet actions()}
      <button class="btn text" onclick={() => (library.confirmDelete = null)}>{t('action.cancel')}</button>
      <button class="btn filled danger-fill" onclick={() => void library.confirmDeletion()}>{t('session.delete')}</button>
    {/snippet}
  </Sheet>
{/if}
