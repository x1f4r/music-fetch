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

/** A microphone in the priority list. */
export interface MicPreference {
  id: string;
  label: string;
}

/** What the browser said about the microphone, as last seen in an extension page. */
export type MicGrant = 'unknown' | 'granted' | 'denied' | 'session';

/** How to analyze a long video on a site, once the user said "Don't ask again". */
export type AnalyzeChoice = 'whole' | 'quick';

/** One stored preference. The same shape as WXT's storage items, so either backs it. */
export interface Setting<T> {
  readonly key: string;
  getValue(): Promise<T>;
  setValue(value: T): Promise<void>;
  removeValue(): Promise<void>;
  /** Called with the new value whenever it changes, in any extension context. */
  watch(callback: (value: T) => void): () => void;
}

/**
 * Demo states keep every setting in memory (like the apps' demo states), so trying a
 * state never changes the user's real choices. Set once, before the first read.
 */
let memory: { values: Map<string, unknown>; listeners: Map<string, Set<(value: unknown) => void>> } | null = null;

export function useMemorySettings(initial: Record<string, unknown> | null = {}): void {
  memory = initial === null ? null : { values: new Map(Object.entries(initial).map(([k, v]) => [k, structuredClone(v)])), listeners: new Map() };
}

function defineSetting<T>(key: `sync:${string}` | `local:${string}`, fallback: T): Setting<T> {
  const stored = storage.defineItem<T>(key, { fallback });
  const notify = (value: T) => memory?.listeners.get(key)?.forEach((listener) => listener(value));
  return {
    key,
    async getValue() {
      if (!memory) return stored.getValue();
      return memory.values.has(key) ? structuredClone(memory.values.get(key) as T) : fallback;
    },
    async setValue(value) {
      if (!memory) return stored.setValue(value);
      memory.values.set(key, structuredClone(value));
      notify(value);
    },
    async removeValue() {
      if (!memory) return stored.removeValue();
      memory.values.delete(key);
      notify(fallback);
    },
    watch(callback) {
      if (!memory) return stored.watch((value) => callback(value ?? fallback));
      const listeners = memory.listeners.get(key) ?? new Set();
      memory.listeners.set(key, listeners);
      const listener = (value: unknown) => callback(value as T);
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}

export const settings = {
  providerDecision: defineSetting<ProviderDecision | null>('sync:providerDecision', null),
  customProvider: defineSetting<CustomProvider | null>('sync:customProvider', null),
  /** `spotify:` / `music://` / … instead of the website (the browser asks once). */
  openInApp: defineSetting<boolean>('sync:openInApp', false),
  autoStop: defineSetting<boolean>('sync:autoStop', true),
  lowPower: defineSetting<LowPowerSetting>('sync:lowPower', 'auto'),
  /** Chromium: the toolbar button opens the side panel instead of the popup. */
  sidePanelOnClick: defineSetting<boolean>('sync:sidePanelOnClick', false),
  notifyInBackground: defineSetting<boolean>('sync:notifyInBackground', true),
  /** Chromium with on-device speech only. */
  searchSungLyrics: defineSetting<boolean>('sync:searchSungLyrics', false),
  /** Settings › Storage: pinned sessions keep their audio. */
  keepPinnedAudio: defineSetting<boolean>('sync:keepPinnedAudio', true),
  /** "Don't ask again for ‹site›" on the analyze sheet: the remembered choice per site. */
  analyzeChoice: defineSetting<Record<string, AnalyzeChoice>>('sync:analyzeChoice', {}),

  /**
   * Microphones by preference, most preferred first (Settings › Listening). Labels are
   * kept so a device that is not connected still shows its name. The first connected one
   * is used; any other microphone comes after the list.
   */
  micPriority: defineSetting<MicPreference[]>('local:micPriority', []),
  micGrant: defineSetting<MicGrant>('local:micGrant', 'unknown'),
  lastSource: defineSetting<ListenChoice | null>('local:lastSource', null),
  /** The welcome flow's next step (0 = provider, 1 = microphone, 2 = shortcut tip, 3 = done). */
  welcomeStep: defineSetting<number>('local:welcomeStep', 0),
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
