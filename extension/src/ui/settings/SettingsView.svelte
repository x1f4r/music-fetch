<script lang="ts">
  import { t, type MessageKey } from '../../i18n';
  import type { ProviderSettings } from '../../settings/settings';
  import { useSurface } from '../context';
  import { SETTINGS_PANES, type SettingsPane } from '../router.svelte';
  import AboutPane from './AboutPane.svelte';
  import GeneralPane from './GeneralPane.svelte';
  import ListeningPane from './ListeningPane.svelte';
  import MelodyPane from './MelodyPane.svelte';
  import PrivacyPane from './PrivacyPane.svelte';
  import ServicesPane from './ServicesPane.svelte';
  import ShortcutsPane from './ShortcutsPane.svelte';
  import StoragePane from './StoragePane.svelte';

  /**
   * Settings in the app tab (extension plan §5.8, design `app-settings-*`): a pane list
   * and grouped rows. A setting lives in exactly one pane.
   */
  interface Props {
    pane: SettingsPane;
    providers: ProviderSettings | null;
    narrow: boolean;
    onpane: (pane: SettingsPane) => void;
  }

  let { pane, providers, narrow, onpane }: Props = $props();
  const { platform } = useSurface();
  const caps = platform.capabilities;

  const PANES: Record<SettingsPane, { icon: string; label: MessageKey }> = {
    general: { icon: 'ic-gear', label: 'settings.general' },
    listening: { icon: 'ic-mic', label: 'settings.listening' },
    services: { icon: 'ic-key', label: 'settings.services' },
    melody: { icon: 'ic-hum', label: 'settings.melody' },
    storage: { icon: 'ic-drive', label: 'settings.storage' },
    privacy: { icon: 'ic-shield', label: 'settings.privacy' },
    shortcuts: { icon: 'ic-keyboard', label: 'settings.shortcuts' },
    about: { icon: 'ic-info', label: 'settings.about' },
  };
  const panes = $derived(SETTINGS_PANES.filter((id) => id !== 'shortcuts' || caps.shortcuts));
</script>

<div class="toolbar settings-toolbar"><h1 class="wtitle">{t('nav.settings')}</h1></div>
<div class="settings-scroll">
  <div class="settings" class:narrow>
    <nav class="nav-list" aria-label={t('settings.panes')}>
      {#each panes as id (id)}
        <a class="ni" class:on={pane === id} href="#/settings/{id}" aria-current={pane === id ? 'page' : undefined} onclick={(e) => (e.preventDefault(), onpane(id))}>
          <span class="ic {PANES[id].icon}"></span>{t(PANES[id].label)}
        </a>
      {/each}
    </nav>
    <div class="form" data-pane={pane}>
      {#if pane === 'general'}
        <GeneralPane {providers} />
      {:else if pane === 'listening'}
        <ListeningPane />
      {:else if pane === 'services'}
        <ServicesPane />
      {:else if pane === 'melody'}
        <MelodyPane />
      {:else if pane === 'storage'}
        <StoragePane />
      {:else if pane === 'privacy'}
        <PrivacyPane />
      {:else if pane === 'shortcuts'}
        <ShortcutsPane />
      {:else}
        <AboutPane />
      {/if}
    </div>
  </div>
</div>
