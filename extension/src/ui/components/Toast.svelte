<script lang="ts">
  import { t, tm } from '../../i18n';
  import type { Toast } from '../library.svelte';

  /** "Deleted 1 · Undo" (design spec "Toast"): bottom centre, inverse surface, 5 s. */
  interface Props {
    toast: Toast;
    onundo: () => void;
  }

  let { toast, onundo }: Props = $props();
</script>

<div class="toast-wrap">
  <div class="toast" role="status">
    {tm(toast.message)}
    {#if toast.undo}<button class="btn text" onclick={onundo}>{t('session.undo')}</button>{/if}
  </div>
</div>
