<script lang="ts">
  import { t } from '../../i18n';

  /**
   * The selection's contextual bar (design spec "Context bar"): "‹n› selected" with Pin or
   * Unpin, Copy (Share on touch), Delete, Select all and ✕. A toolbar labelled by the count.
   */
  interface Props {
    count: number;
    pin: 'pin' | 'unpin';
    touch?: boolean;
    onpin: () => void;
    oncopy: () => void;
    ondelete: () => void;
    onselectall: () => void;
    onclear: () => void;
  }

  let { count, pin, touch = false, onpin, oncopy, ondelete, onselectall, onclear }: Props = $props();
  const label = $derived(t('library.selected', count));

  function onkeydown(event: KeyboardEvent) {
    // A toolbar is one tab stop; ← → move between its buttons.
    if (event.key !== 'ArrowRight' && event.key !== 'ArrowLeft') return;
    const buttons = [...(event.currentTarget as HTMLElement).querySelectorAll<HTMLButtonElement>('button')];
    const index = buttons.indexOf(document.activeElement as HTMLButtonElement);
    const next = buttons[(index + (event.key === 'ArrowRight' ? 1 : -1) + buttons.length) % buttons.length];
    event.preventDefault();
    next?.focus();
  }
</script>

<div class="ctx" role="toolbar" aria-label={label} tabindex="-1" {onkeydown}>
  <button class="btn icon plain" aria-label={t('library.clearSelection')} onclick={onclear}><span class="ic ic-x"></span></button>
  <span class="count" aria-live="polite">{label}</span>
  <button class="btn text pin" onclick={onpin}><span class="ic {pin === 'pin' ? 'ic-pin' : 'ic-pin-fill'}"></span>{t(pin === 'pin' ? 'session.pin' : 'session.unpin')}</button>
  <button class="btn text copy" onclick={oncopy}><span class="ic {touch ? 'ic-share' : 'ic-copy'}"></span>{t(touch ? 'result.share' : 'result.copy')}</button>
  <button class="btn text delete" onclick={ondelete}><span class="ic ic-trash"></span>{t('session.delete')}</button>
  <span class="grow"></span>
  <button class="btn text" onclick={onselectall}>{t('library.selectAll')}</button>
</div>
