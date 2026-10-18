<script lang="ts">
  import { onMount, tick } from 'svelte';
  import { t } from '../../i18n';
  import { resolveLowPower } from '../../platform/low-power';
  import { settings, type LowPowerSetting, type MicGrant, type MicPreference } from '../../settings/settings';
  import { useSurface } from '../context';
  import SwitchRow from './SwitchRow.svelte';

  /**
   * Settings › Listening (design `app-settings-listening`): the microphone's grant and
   * priority, auto-stop and low power. On Firefox for Android the microphone is the app's.
   */
  const { engine, platform } = useSurface();
  const caps = platform.capabilities;

  let autoStop = $state(true);
  let lowPower = $state<LowPowerSetting>('auto');
  let micGrant = $state<MicGrant>('unknown');
  let priority = $state<MicPreference[]>([]);
  let connected = $state<{ id: string; label: string; inUse: boolean }[]>([]);

  /** The priority list, then connected microphones not in it, then "any other". */
  const devices = $derived([
    ...priority.map((p) => {
      const device = connected.find((c) => c.id === p.id);
      return { ...p, state: device ? (device.inUse ? 'inUse' : 'connected') : 'notConnected' };
    }),
    ...connected.filter((c) => !priority.some((p) => p.id === c.id) && c.label).map((c) => ({ id: c.id, label: c.label, state: c.inUse ? 'inUse' : 'connected' })),
  ]);
  const micStatus = $derived(
    micGrant === 'granted' ? t('mic.status.granted') : micGrant === 'session' ? t('mic.status.session') : micGrant === 'denied' ? t('mic.status.denied') : t('mic.status.unknown'),
  );

  onMount(async () => {
    [autoStop, lowPower, micGrant, priority] = await Promise.all([
      settings.autoStop.getValue(),
      settings.lowPower.getValue(),
      settings.micGrant.getValue(),
      settings.micPriority.getValue(),
    ]);
    if (caps.microphone === 'extension') connected = await platform.microphones();
  });

  async function move(index: number, step: number) {
    const list = devices.filter((d) => d.label).map((d) => ({ id: d.id, label: d.label }));
    const target = index + step;
    if (target < 0 || target >= list.length) return;
    [list[index], list[target]] = [list[target]!, list[index]!];
    priority = list;
    await settings.micPriority.setValue(list);
  }

  const LOW_POWER: readonly LowPowerSetting[] = ['auto', 'on', 'off'];

  function setLowPower(value: LowPowerSetting) {
    lowPower = value;
    void settings.lowPower.setValue(value);
    // Applies at once. Auto is decided like the engine host does at start, from this
    // device's processors and memory (this page runs on the same device).
    const memory = (navigator as Navigator & { deviceMemory?: number }).deviceMemory ?? null;
    void engine.setLowPower(resolveLowPower(value, caps.android, navigator.hardwareConcurrency || 8, memory)).catch(() => undefined);
  }

  /** Arrow keys move the choice, as in every radio group. */
  function lowPowerKey(event: KeyboardEvent) {
    const step = event.key === 'ArrowRight' || event.key === 'ArrowDown' ? 1 : event.key === 'ArrowLeft' || event.key === 'ArrowUp' ? -1 : 0;
    if (!step) return;
    event.preventDefault();
    const group = event.currentTarget as HTMLElement;
    setLowPower(LOW_POWER[(LOW_POWER.indexOf(lowPower) + step + LOW_POWER.length) % LOW_POWER.length]!);
    void tick().then(() => group.querySelector<HTMLElement>('[aria-checked="true"]')?.focus());
  }
</script>

<h2 class="sr-only">{t('settings.listening')}</h2>
{#if caps.microphone === 'app'}
  <div class="group">
    <div class="frow">
      <div class="k">{t('settings.inApp')}<span class="sub">{t('settings.inApp.sub')}</span></div>
    </div>
  </div>
{:else}
  <div class="group">
    <div class="frow">
      <div class="k">{t('settings.microphone')}<span class="sub">{micStatus}</span></div>
      <div class="v">
        {#if micGrant === 'granted' || micGrant === 'denied'}
          <button class="btn small text" onclick={() => void platform.openSiteSettings()}>{t('mic.siteSettings')}</button>
        {/if}
        {#if micGrant !== 'granted'}
          <a class="btn small" href="#/grant-mic">{t('mic.allowButton')}</a>
        {/if}
      </div>
    </div>
  </div>
  <h3 class="ghead">{t('settings.micPriority')}</h3>
  <div class="group" role="list" aria-label={t('settings.micPriority.label')}>
    {#each devices as device, i (device.id)}
      <div class="dev" role="listitem">
        <span class="n tnum">{i + 1}</span>
        <span class="name truncate">{device.label}</span>
        <span class="st">{device.state === 'notConnected' ? t('settings.micPriority.notConnected') : device.state === 'inUse' ? t('settings.micPriority.inUse') : ''}</span>
        <span class="reorder">
          <button class="btn icon plain small" aria-label={t('settings.micPriority.up', device.label)} disabled={i === 0} onclick={() => void move(i, -1)}><span class="ic ic-chev-down s14 up"></span></button>
          <button class="btn icon plain small" aria-label={t('settings.micPriority.down', device.label)} disabled={i === devices.length - 1} onclick={() => void move(i, 1)}><span class="ic ic-chev-down s14"></span></button>
        </span>
      </div>
    {/each}
    <div class="dev" role="listitem"><span class="n tnum">{devices.length + 1}</span><span class="name truncate secondary">{t('settings.micPriority.any')}</span></div>
  </div>
  <div class="gfoot">{t('settings.micPriority.footer')}</div>
{/if}
<div class="group">
  <SwitchRow
    label={t('settings.autoStop')}
    value={autoStop}
    onchange={(v) => {
      autoStop = v;
      void settings.autoStop.setValue(v);
    }}
  />
</div>
<div class="group">
  <div class="frow">
    <div class="k">{t('settings.lowPower')}<span class="sub">{t('settings.lowPower.sub')}</span></div>
    <div class="v">
      <div class="seg" role="radiogroup" aria-label={t('settings.lowPower')} tabindex="-1" onkeydown={lowPowerKey}>
        {#each LOW_POWER as value (value)}
          <button role="radio" aria-checked={lowPower === value} tabindex={lowPower === value ? 0 : -1} class:on={lowPower === value} onclick={() => setLowPower(value)}>{t(`settings.lowPower.${value}`)}</button>
        {/each}
      </div>
    </div>
  </div>
</div>
<div class="gfoot">{t('settings.lowPower.footer')}</div>
