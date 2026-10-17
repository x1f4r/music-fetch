<script lang="ts">
  import { onMount } from 'svelte';
  import { t } from '../../i18n';
  import { settings, type ProviderSettings } from '../../settings/settings';
  import MicGrant from '../components/MicGrant.svelte';
  import ProviderPicker from '../components/ProviderPicker.svelte';
  import { useSurface } from '../context';

  /**
   * First run (extension plan §5.2), opened by `onInstalled`: "Where do you listen?",
   * then the microphone, then the shortcut tip. Each step is remembered, and the provider
   * decision is stored only by Choose or Skip.
   */
  interface Props {
    providers: ProviderSettings | null;
    onfinish: () => void;
  }

  let { providers, onfinish }: Props = $props();
  const { platform } = useSurface();
  let step = $state(0);

  onMount(async () => {
    step = await settings.welcomeStep.getValue();
  });

  // A decision stored elsewhere (the popup) moves past the first step.
  $effect(() => {
    if (step === 0 && providers && !providers.needsPanel) void advance(1);
  });

  async function advance(next: number) {
    step = next;
    await settings.welcomeStep.setValue(next);
    if (next >= 3) onfinish();
  }

  const shortcutsUrl = platform.capabilities.target === 'firefox' ? 'about:addons' : 'chrome://extensions/shortcuts';
</script>

{#if step === 0 && providers}
  <div class="welcome">
    <div class="col">
      <ProviderPicker savedCustom={providers.custom} ondecided={() => void advance(1)} />
    </div>
  </div>
{:else if step === 1}
  <MicGrant ondone={() => void advance(platform.capabilities.shortcuts ? 2 : 3)} />
{:else if step === 2}
  <div class="grant">
    <div class="mini-disc" aria-hidden="true"><span class="ic ic-mark"></span></div>
    <h1>Alt+Shift+M</h1>
    <p>{t('welcome.shortcut', shortcutsUrl)}</p>
    <div class="acts"><button class="btn filled" onclick={() => void advance(3)}>{t('action.continue')}</button></div>
  </div>
{/if}
