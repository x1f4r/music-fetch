<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import { libraryQuery, type LibraryEntry } from '../../engine/types';
  import { t } from '../../i18n';
  import { matchesFilter, type LibraryFilter } from '../../models/library';
  import LibraryList from '../components/LibraryList.svelte';
  import { useSurface } from '../context';

  interface Props {
    onopen: (sessionId: string) => void;
  }

  let { onopen }: Props = $props();
  const { engine } = useSurface();
  let entries = $state<LibraryEntry[]>([]);
  let filter = $state<LibraryFilter>('all');
  let search = $state('');
  let stop: (() => void) | null = null;

  const FILTERS: { id: LibraryFilter; label: string }[] = (['all', 'heard', 'hummed', 'links', 'mixes'] as const).map((id) => ({
    id,
    label: t(`library.filter.${id}`),
  }));

  const visible = $derived(entries.filter((e) => matchesFilter(e, filter)));

  async function load() {
    entries = await engine.library(libraryQuery({ search: search.trim() || null, limit: 500 })).catch(() => []);
  }

  onMount(() => {
    stop = engine.subscribe({ libraryChanged: () => void load() });
    void load();
  });
  onDestroy(() => stop?.());
</script>

<div class="toolbar">
  <span class="wtitle">{t('library.title')}</span>
  <span class="grow"></span>
  <div class="seg" role="radiogroup" aria-label={t('library.title')}>
    {#each FILTERS as item (item.id)}
      <button role="radio" aria-checked={filter === item.id} class:on={filter === item.id} onclick={() => (filter = item.id)}>{item.label}</button>
    {/each}
  </div>
  <label class="field small search"><span class="ic ic-search s14 secondary"></span><input bind:value={search} oninput={() => void load()} placeholder={t('library.search')} aria-label={t('library.search')} /></label>
</div>
<div class="scroll-area">
  <div class="list">
    <LibraryList entries={visible} onopen={(entry) => onopen(entry.id)} />
  </div>
</div>
