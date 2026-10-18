<script lang="ts">
  import { t } from '../../i18n';

  /** The welcome flow's progress: one dot per step, the current one filled. */
  interface Props {
    /** Zero-based. */
    index: number;
    count: number;
  }

  let { index, count }: Props = $props();
</script>

<div class="dots" role="img" aria-label={t('welcome.step', index + 1, count)}>
  {#each Array.from({ length: count }) as _, i (i)}<i class:on={i === index}></i>{/each}
</div>
