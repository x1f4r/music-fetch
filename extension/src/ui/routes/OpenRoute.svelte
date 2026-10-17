<script lang="ts">
  import { onMount } from 'svelte';
  import { t, tm } from '../../i18n';
  import { errorLine } from '../../models/stage';
  import { EngineError } from '../../engine/client';
  import { useSurface } from '../context';

  /** `#/open?url=…` (context menus, "Open and analyze"): start the analysis, then show the session. */
  interface Props {
    url: string;
    onstarted: (sessionId: string) => void;
  }

  let { url, onstarted }: Props = $props();
  const { engine } = useSurface();
  let failure = $state<string | null>(null);

  onMount(async () => {
    try {
      onstarted(await engine.analyze(url, 'Auto', null));
    } catch (error) {
      failure = error instanceof EngineError && error.code !== 'Unknown' && error.code !== 'Disconnected' && error.code !== 'Timeout' ? t(errorLine(error.code)) : t('error.engine');
    }
  });
</script>

<div class="empty">
  {#if failure}
    <div class="headline">{failure}</div>
  {:else}
    <span class="spinner" aria-hidden="true"></span>
    <div class="callout">{tm({ key: 'state.resolving' })}</div>
  {/if}
</div>
