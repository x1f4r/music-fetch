<script lang="ts">
  import type { Snippet } from 'svelte';
  import { onMount } from 'svelte';

  /**
   * A pre-prompt sheet (design spec "Sheet"): melody download, "Analyze the whole video?",
   * delete confirmations. A modal dialog labelled by its title: focus moves in and stays
   * there, Esc and the scrim close it, and focus returns to the control that opened it.
   */
  interface Props {
    title: string;
    /** Extra classes for placement (`bottom` in the popup and panel, `center` in the app tab). */
    placement?: 'bottom' | 'center';
    onclose: () => void;
    children: Snippet;
    actions: Snippet;
  }

  let { title, placement = 'bottom', onclose, children, actions }: Props = $props();
  let sheet = $state<HTMLElement | undefined>();
  const id = `sheet-${Math.random().toString(36).slice(2, 8)}`;

  onMount(() => {
    const opener = document.activeElement as HTMLElement | null;
    // The primary action (last button) gets focus, like the system's default button.
    const buttons = sheet?.querySelectorAll<HTMLElement>('.sb button');
    (buttons?.[buttons.length - 1] ?? sheet)?.focus();
    return () => {
      if (opener?.isConnected) opener.focus();
    };
  });

  function focusable(): HTMLElement[] {
    return [...(sheet?.querySelectorAll<HTMLElement>('button, [href], input, select, [tabindex]:not([tabindex="-1"])') ?? [])].filter((el) => !el.hasAttribute('disabled'));
  }

  function onkeydown(event: KeyboardEvent) {
    if (event.key === 'Escape') {
      event.preventDefault();
      event.stopPropagation();
      onclose();
    } else if (event.key === 'Tab') {
      const items = focusable();
      if (items.length === 0) return;
      const first = items[0]!;
      const last = items[items.length - 1]!;
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    }
  }
</script>

<div class="scrim" aria-hidden="true" onclick={onclose}></div>
<div class="sheet {placement}" role="dialog" aria-modal="true" aria-labelledby={id} tabindex="-1" bind:this={sheet} {onkeydown}>
  <div class="st" {id}>{title}</div>
  {@render children()}
  <div class="sb">{@render actions()}</div>
</div>
