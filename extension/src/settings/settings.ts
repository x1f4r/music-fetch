import { storage } from 'wxt/utils/storage';
import {
  isValidCustomProvider,
  providerFromKey,
  providerKey,
  SPOTIFY,
  type CustomProvider,
  type Provider,
} from '../providers/links';

/**
 * Preferences (extension plan §3.5). Small choices that follow the user go to
 * `storage.sync` (Chrome Sync, Firefox Sync on desktop; local only on Android); state
 * that belongs to this browser install stays in `storage.local`. The library lives in
 * the engine, not here.
 */

/**
 * Where songs open, as decided in the first-launch panel or in Settings. It is written
 * only when the user chooses or skips (docs/APPS.md "Opening links"); until then the
 * panel shows again. `provider` is the storage key shared with the apps ("spotify",
 * "appleMusic", …, "custom").
 */
export interface ProviderDecision {
  provider: string;
  /** True when the user skipped the panel (the provider is then Spotify). */
  skipped: boolean;
}

export type LowPowerSetting = 'auto' | 'on' | 'off';

/** The stage's source toggles, in the apps' order. */
export type ListenChoice = 'Microphone' | 'Tab' | 'Hum';

/** What the browser said about the microphone, as last seen in an extension page. */
export type MicGrant = 'unknown' | 'granted' | 'denied' | 'session';

export const settings = {
  providerDecision: storage.defineItem<ProviderDecision | null>('sync:providerDecision', { fallback: null }),
  customProvider: storage.defineItem<CustomProvider | null>('sync:customProvider', { fallback: null }),
  /** `spotify:` / `music://` / … instead of the website (the browser asks once). */
  openInApp: storage.defineItem<boolean>('sync:openInApp', { fallback: false }),
  autoStop: storage.defineItem<boolean>('sync:autoStop', { fallback: true }),
  lowPower: storage.defineItem<LowPowerSetting>('sync:lowPower', { fallback: 'auto' }),
  /** Chromium: the toolbar button opens the side panel instead of the popup. */
  sidePanelOnClick: storage.defineItem<boolean>('sync:sidePanelOnClick', { fallback: false }),
  notifyInBackground: storage.defineItem<boolean>('sync:notifyInBackground', { fallback: true }),
  /** Chromium with on-device speech only. */
  searchSungLyrics: storage.defineItem<boolean>('sync:searchSungLyrics', { fallback: false }),

  /** Microphone device ids, most preferred first. */
  micPriority: storage.defineItem<string[]>('local:micPriority', { fallback: [] }),
  micGrant: storage.defineItem<MicGrant>('local:micGrant', { fallback: 'unknown' }),
  lastSource: storage.defineItem<ListenChoice | null>('local:lastSource', { fallback: null }),
  /** The welcome flow's next step (0 = provider, 1 = microphone, 2 = shortcut tip, 3 = done). */
  welcomeStep: storage.defineItem<number>('local:welcomeStep', { fallback: 0 }),
};

/** The provider Skip stores. */
export const SKIP_PROVIDER: Provider = SPOTIFY;

/** True while no usable decision is stored: show "Where do you listen?". */
export function needsProviderPanel(decision: ProviderDecision | null): boolean {
  return !decision || providerFromKey(decision.provider) === null;
}

/**
 * The provider links open in. Spotify until a decision exists; a custom provider without
 * a usable template also falls back to Spotify (as on macOS and Android).
 */
export function resolveProvider(decision: ProviderDecision | null, custom: CustomProvider | null): Provider {
  const provider = providerFromKey(decision?.provider);
  if (!provider) return SKIP_PROVIDER;
  if (provider.kind === 'custom' && !isValidCustomProvider(custom)) return SKIP_PROVIDER;
  return provider;
}

/** Store the user's choice. A custom choice needs a valid custom provider, saved with it. */
export async function chooseProvider(provider: Provider, custom?: CustomProvider): Promise<void> {
  if (provider.kind === 'custom') {
    if (!isValidCustomProvider(custom)) throw new Error('The custom provider needs a name and a link template.');
    await settings.customProvider.setValue({ name: custom.name.trim(), template: custom.template.trim() });
  }
  await settings.providerDecision.setValue({ provider: providerKey(provider), skipped: false });
}

/** "Skip": the decision is stored, with Spotify as the provider. */
export async function skipProviderPanel(): Promise<void> {
  await settings.providerDecision.setValue({ provider: providerKey(SKIP_PROVIDER), skipped: true });
}

export interface ProviderSettings {
  decision: ProviderDecision | null;
  custom: CustomProvider | null;
  provider: Provider;
  needsPanel: boolean;
}

export async function loadProviderSettings(): Promise<ProviderSettings> {
  const [decision, custom] = await Promise.all([settings.providerDecision.getValue(), settings.customProvider.getValue()]);
  return { decision, custom, provider: resolveProvider(decision, custom), needsPanel: needsProviderPanel(decision) };
}

/** Call `callback` with fresh provider settings whenever they change (in any context). */
export function watchProviderSettings(callback: (value: ProviderSettings) => void): () => void {
  const reload = () => void loadProviderSettings().then(callback);
  const stops = [settings.providerDecision.watch(reload), settings.customProvider.watch(reload)];
  return () => stops.forEach((stop) => stop());
}
