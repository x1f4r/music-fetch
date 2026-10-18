import { fireEvent, render, screen, within } from '@testing-library/svelte';
import { tick } from 'svelte';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fakeBrowser } from 'wxt/testing/fake-browser';
import OpenInButton from '@/src/ui/components/OpenInButton.svelte';
import SourceToggles from '@/src/ui/components/SourceToggles.svelte';
import { TRACKS } from '@/src/ui/demo/library';

describe('the split Open in button', () => {
  beforeEach(() => fakeBrowser.reset());

  it('opens the preferred provider and lists the rest, current checked, then custom and the web', async () => {
    const onopen = vi.fn();
    render(OpenInButton, {
      props: {
        track: TRACKS.houdini,
        preferred: { kind: 'service', service: 'AppleMusic' },
        custom: { name: 'Plex', template: 'https://plex.example/search?q={query}' },
        onopen,
      },
    });
    await fireEvent.click(screen.getByRole('button', { name: 'Open in Apple Music' }));
    expect(onopen).toHaveBeenLastCalledWith(expect.stringContaining('music.apple.com'));

    const chevron = screen.getByRole('button', { name: 'Open in another service' });
    expect(chevron.getAttribute('aria-haspopup')).toBe('menu');
    await fireEvent.click(chevron);
    await tick();
    const menu = screen.getByRole('menu');
    const items = [...menu.querySelectorAll<HTMLElement>('.mi')];
    expect(items[0]!.textContent).toContain('Apple Music');
    expect(items[0]!.getAttribute('aria-checked')).toBe('true');
    expect(within(menu).getByText('Plex').parentElement!.textContent).toContain('custom');
    expect(items.at(-1)!.textContent).toContain('Search the web');
    expect(document.activeElement).toBe(items[0]);

    await fireEvent.keyDown(menu, { key: 'ArrowDown' });
    expect(document.activeElement).toBe(items[1]);
    await fireEvent.keyDown(menu, { key: 'Escape' });
    expect(screen.queryByRole('menu')).toBeNull();
    expect(document.activeElement).toBe(chevron);
  });

  it('jumps to a provider by typing its name', async () => {
    render(OpenInButton, { props: { track: TRACKS.houdini, preferred: { kind: 'service', service: 'Spotify' }, custom: null, onopen: vi.fn() } });
    await fireEvent.click(screen.getByRole('button', { name: 'Open in another service' }));
    await tick();
    await fireEvent.keyDown(screen.getByRole('menu'), { key: 't' });
    expect(document.activeElement?.textContent).toContain('Tidal');
  });
});

describe('the source toggles', () => {
  it('are a radio group moved with the arrow keys, one tab stop', async () => {
    const onchange = vi.fn();
    render(SourceToggles, { props: { value: 'Tab', onchange } });
    const radios = screen.getAllByRole('radio');
    expect(radios.map((r) => r.getAttribute('tabindex'))).toEqual(['-1', '0', '-1']);
    await fireEvent.keyDown(radios[1]!, { key: 'ArrowRight' });
    expect(onchange).toHaveBeenCalledWith('Hum');
    await fireEvent.keyDown(radios[1]!, { key: 'ArrowLeft' });
    expect(onchange).toHaveBeenLastCalledWith('Microphone');
  });

  it('keep their names for assistive technology when narrow', () => {
    render(SourceToggles, { props: { value: 'Tab', iconsOnly: true, onchange: vi.fn() } });
    expect(screen.getByRole('radio', { name: 'Microphone' }).textContent?.trim()).toBe('');
  });
});
