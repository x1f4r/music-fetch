import { fireEvent, render, screen } from '@testing-library/svelte';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fakeBrowser } from 'wxt/testing/fake-browser';
import ProviderPicker from '@/src/ui/components/ProviderPicker.svelte';

const stored = async () => (await fakeBrowser.storage.sync.get()) as Record<string, unknown>;

describe('Where do you listen?', () => {
  beforeEach(() => fakeBrowser.reset());

  it('stores nothing until Choose', async () => {
    const ondecided = vi.fn();
    render(ProviderPicker, { props: { ondecided } });
    expect(screen.getByRole('heading', { name: 'Where do you listen?' })).toBeTruthy();
    const choose = screen.getByRole('button', { name: 'Continue' }) as HTMLButtonElement;
    expect(choose.disabled).toBe(true);

    await fireEvent.click(screen.getByRole('radio', { name: /YouTube Music/ }));
    expect(screen.getByRole('radio', { name: /YouTube Music/ }).getAttribute('aria-checked')).toBe('true');
    expect(await stored()).toEqual({});

    await fireEvent.click(screen.getByRole('button', { name: 'Choose YouTube Music' }));
    await vi.waitFor(() => expect(ondecided).toHaveBeenCalledOnce());
    expect(await stored()).toEqual({ providerDecision: { provider: 'youTubeMusic', skipped: false } });
  });

  it('Skip stores Spotify', async () => {
    const ondecided = vi.fn();
    render(ProviderPicker, { props: { ondecided } });
    await fireEvent.click(screen.getByRole('button', { name: 'Skip' }));
    await vi.waitFor(() => expect(ondecided).toHaveBeenCalledOnce());
    expect(await stored()).toEqual({ providerDecision: { provider: 'spotify', skipped: true } });
  });

  it('pre-selects the streaming site in the current tab', () => {
    render(ProviderPicker, { props: { suggested: 'Tidal' } });
    const tidal = screen.getByRole('radio', { name: /Tidal/ });
    expect(tidal.getAttribute('aria-checked')).toBe('true');
    expect(tidal.textContent).toContain('This tab');
    expect(screen.getByRole('button', { name: 'Choose Tidal' })).toBeTruthy();
  });

  it('checks a custom provider before storing it', async () => {
    const ondecided = vi.fn();
    render(ProviderPicker, { props: { ondecided } });
    await fireEvent.click(screen.getByRole('button', { name: /More/ }));
    expect(screen.getByRole('radio', { name: 'Qobuz' })).toBeTruthy();
    await fireEvent.click(screen.getByRole('radio', { name: 'Another app or site' }));

    await fireEvent.input(screen.getByLabelText('Name'), { target: { value: 'Plex' } });
    await fireEvent.input(screen.getByLabelText('Link'), { target: { value: 'https://plex.example/search' } });
    await fireEvent.click(screen.getByRole('button', { name: 'Choose Plex' }));
    expect(screen.getByRole('alert').textContent).toContain('{query}');
    expect(await stored()).toEqual({});

    await fireEvent.input(screen.getByLabelText('Link'), { target: { value: 'https://plex.example/search?q={query}' } });
    expect(screen.getByText(/Opens https:\/\/plex\.example\/search\?q=a%2Dha%20Take%20On%20Me/)).toBeTruthy();
    await fireEvent.click(screen.getByRole('button', { name: 'Choose Plex' }));
    await vi.waitFor(() => expect(ondecided).toHaveBeenCalledOnce());
    expect(await stored()).toEqual({
      providerDecision: { provider: 'custom', skipped: false },
      customProvider: { name: 'Plex', template: 'https://plex.example/search?q={query}' },
    });
  });
});
