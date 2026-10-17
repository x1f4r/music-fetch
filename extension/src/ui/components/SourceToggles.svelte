<script lang="ts">
  import { t } from '../../i18n';
  import type { ListenChoice } from '../../settings/settings';

  interface Props {
    value: ListenChoice;
    /** Capture is running: the toggles dim (never hide). */
    disabled?: boolean;
    /** Sources this browser can't use right now are left out (e.g. no tab capture). */
    available?: readonly ListenChoice[];
    onchange: (value: ListenChoice) => void;
  }

  let { value, disabled = false, available = ['Microphone', 'Tab', 'Hum'], onchange }: Props = $props();

  const ALL: { id: ListenChoice; icon: string; label: string }[] = [
    { id: 'Microphone', icon: 'ic-mic', label: t('source.microphone') },
    { id: 'Tab', icon: 'ic-tab', label: t('source.tab') },
    { id: 'Hum', icon: 'ic-hum', label: t('source.hum') },
  ];
  const options = $derived(ALL.filter((o) => available.includes(o.id)));
  let buttons = $state<HTMLButtonElement[]>([]);

  function onkeydown(event: KeyboardEvent) {
    const step = event.key === 'ArrowRight' || event.key === 'ArrowDown' ? 1 : event.key === 'ArrowLeft' || event.key === 'ArrowUp' ? -1 : 0;
    if (!step) return;
    event.preventDefault();
    const index = options.findIndex((o) => o.id === value);
    const next = (index + step + options.length) % options.length;
    onchange(options[next]!.id);
    buttons[next]?.focus();
  }
</script>

<div class="sources seg" class:dim={disabled} role="radiogroup" aria-label={t('source.label')} aria-disabled={disabled}>
  {#each options as option, i (option.id)}
    <button
      bind:this={buttons[i]}
      role="radio"
      aria-checked={option.id === value}
      class:on={option.id === value}
      tabindex={option.id === value ? 0 : -1}
      onclick={() => onchange(option.id)}
      {onkeydown}
    ><span class="ic {option.icon} s14"></span>{option.label}</button>
  {/each}
</div>
