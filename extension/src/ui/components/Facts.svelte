<script lang="ts">
  import { t } from '../../i18n';
  import type { Fact } from '../../models/session';

  /** Key–value rows of the details block and the details column. */
  interface Props {
    facts: readonly Fact[];
  }

  let { facts }: Props = $props();
</script>

{#each facts as fact (fact.key)}
  <div class="kv"><span class="k">{t(fact.key)}</span><span class="v" class:tnum={fact.tnum}>{fact.value}</span></div>
{/each}
