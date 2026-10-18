<script lang="ts">
  import { onMount } from 'svelte';
  import { t } from '../../i18n';
  import { settings, type ProviderSettings } from '../../settings/settings';
  import MicGrant from '../components/MicGrant.svelte';
  import ProviderPicker from '../components/ProviderPicker.svelte';
  import StepDots from '../components/StepDots.svelte';
  import { useSurface } from '../context';

  /**
   * First run (extension plan §5.2), opened by `onInstalled`: "Where do you listen?",
   * then the microphone, then the shortcut tip (desktop) or where to find it (Android).
   * Each step is remembered, and the provider decision is stored only by Choose or Skip.
   * Firefox for Android has no microphone step: the extension can't use it there, and
   * Microphone and Hum open the Music Fetch app.
   */
  interface Props {
    providers: ProviderSettings | null;
    onfinish: () => void;
  }

  let { providers, onfinish }: Props = $props();
  const { platform } = useSurface();
  const caps = platform.capabilities;
  let step = $state<number | null>(null);
  /** Stored step numbers: 0 provider, 1 microphone, 2 shortcut or where to find it, 3 done. */
  const flow = caps.microphone === 'app' ? [0, 2] : [0, 1, 2];
  const DONE = 3;

  onMount(async () => {
    const stored = await settings.welcomeStep.getValue();
    step = flow.includes(stored) || stored >= DONE ? stored : 2;
  });

  // A decision stored elsewhere (the popup) moves past the first step.
  $effect(() => {
    if (step === 0 && providers && !providers.needsPanel) void advance(1);
  });

  async function advance(next: number) {
    if (next < DONE && !flow.includes(next)) next = flow.find((s) => s > next) ?? DONE;
    step = next;
    await settings.welcomeStep.setValue(next);
    if (next >= DONE) onfinish();
  }

  const shortcutsLabel = caps.target === 'firefox' ? 'about:addons' : 'chrome://extensions/shortcuts';
</script>

{#if step === 0 && providers}
  <div class="welcome">
    <div class="col">
      <ProviderPicker savedCustom={providers.custom} steps={flow.length} ondecided={() => void advance(1)} />
    </div>
  </div>
{:else if step === 1}
  <MicGrant step={{ index: 1, count: flow.length }} onsitesettings={() => void platform.openSiteSettings()} ondone={() => void advance(2)} />
{:else if step === 2}
  <div class="grant">
    <div class="mini-disc" aria-hidden="true"><span class="ic ic-mark"></span></div>
    {#if caps.android}
      <h1>{t('app.name')}</h1>
      <p>{t('welcome.android')}</p>
    {:else}
      <h1>{t('welcome.shortcutTitle')}</h1>
      <p><kbd>Alt</kbd>+<kbd>Shift</kbd>+<kbd>M</kbd></p>
      <p class="callout">{t('welcome.shortcut', shortcutsLabel)}</p>
    {/if}
    <div class="acts">
      {#if !caps.android && caps.shortcuts}<button class="btn text" onclick={() => void platform.openShortcuts()}>{t('settings.shortcuts.change')}</button>{/if}
      <button class="btn filled" onclick={() => void advance(DONE)}>{t('action.continue')}</button>
    </div>
    <StepDots index={flow.length - 1} count={flow.length} />
  </div>
{/if}
