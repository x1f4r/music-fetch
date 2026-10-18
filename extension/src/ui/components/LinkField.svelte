<script lang="ts">
  import { t } from '../../i18n';

  interface Props {
    onsubmit: (text: string) => void;
    disabled?: boolean;
  }

  let { onsubmit, disabled = false }: Props = $props();
  let text = $state('');
  let focused = $state(false);
  let input = $state<HTMLInputElement | undefined>();

  /** "Type a lyric" puts the cursor here. */
  export function focus() {
    input?.focus();
  }

  function onkeydown(event: KeyboardEvent) {
    if (event.key !== 'Enter' || !text.trim()) return;
    event.preventDefault();
    onsubmit(text.trim());
    text = '';
  }
</script>

<label class="field" class:focus={focused}>
  <span class="ic ic-link s14 secondary"></span>
  <input
    bind:this={input}
    bind:value={text}
    placeholder={t('field.link')}
    aria-label={t('field.link')}
    autocomplete="off"
    spellcheck="false"
    {disabled}
    {onkeydown}
    onfocus={() => (focused = true)}
    onblur={() => (focused = false)}
  />
</label>
