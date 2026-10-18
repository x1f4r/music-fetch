import { fireEvent, render, screen } from '@testing-library/svelte';
import { createRawSnippet } from 'svelte';
import { describe, expect, it, vi } from 'vitest';
import Sheet from '@/src/ui/components/Sheet.svelte';

const html = (markup: string) => createRawSnippet(() => ({ render: () => markup }));

describe('sheets', () => {
  it('are modal dialogs labelled by their title that trap focus and close on Esc', async () => {
    const opener = document.createElement('button');
    document.body.append(opener);
    opener.focus();
    const onclose = vi.fn();
    const { unmount } = render(Sheet, {
      props: {
        title: 'Download the melody library?',
        onclose,
        children: html('<p>Humming needs a library.</p>'),
        actions: html('<span><button>Not now</button><button>Download</button></span>'),
      },
    });
    const dialog = screen.getByRole('dialog', { name: 'Download the melody library?' });
    expect(dialog.getAttribute('aria-modal')).toBe('true');
    const download = screen.getByRole('button', { name: 'Download' });
    expect(document.activeElement).toBe(download);
    await fireEvent.keyDown(dialog, { key: 'Tab' });
    expect(document.activeElement).toBe(screen.getByRole('button', { name: 'Not now' }));
    await fireEvent.keyDown(dialog, { key: 'Escape' });
    expect(onclose).toHaveBeenCalledOnce();
    unmount();
    expect(document.activeElement).toBe(opener);
  });
});
