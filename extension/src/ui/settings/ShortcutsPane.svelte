<script lang="ts">
  import { onMount } from 'svelte';
  import { t, type MessageKey } from '../../i18n';
  import { useSurface } from '../context';

  /** Settings › Shortcuts: the browser's commands as it has them now, and a link to change them. */
  const { platform } = useSurface();
  const COMMAND_NAMES: Record<string, MessageKey> = {
    _execute_action: 'settings.shortcuts.open',
    'identify-tab': 'settings.shortcuts.identify',
    hum: 'settings.shortcuts.hum',
  };
  let shortcuts = $state<{ name: string; shortcut: string }[]>([]);
  const rows = $derived(Object.keys(COMMAND_NAMES).map((name) => shortcuts.find((s) => s.name === name)).filter((s) => s !== undefined));

  onMount(async () => {
    shortcuts = await platform.shortcuts();
  });
</script>

<h2 class="sr-only">{t('settings.shortcuts')}</h2>
<div class="group">
  {#each rows as row (row.name)}
    <div class="frow"><div class="k">{t(COMMAND_NAMES[row.name]!)}</div><div class="v"><kbd class="tnum">{row.shortcut || t('settings.shortcuts.none')}</kbd></div></div>
  {/each}
</div>
<div class="gfoot">{t('settings.shortcuts.footer')}</div>
<div><button class="btn" onclick={() => void platform.openShortcuts()}><span class="ic ic-arrow-out s12"></span>{t('settings.shortcuts.change')}</button></div>
