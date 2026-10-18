import type { EngineClient } from '../engine/client';
import { isLink } from '../engine/links';
import type { LibraryEntry, PageContext, Track } from '../engine/types';
import { t } from '../i18n';
import { isRunning } from '../models/stage';
import { isAnalyzablePage, type ActiveTab } from '../platform/surfaces';
import { primaryLink, providerLinks } from '../providers/links';
import { serviceName } from '../providers/services';
import { loadProviderSettings, settings, watchProviderSettings, type AnalyzeChoice, type ListenChoice, type ProviderSettings } from '../settings/settings';
import type { Platform } from './context';
import { ListenController } from './listen.svelte';

/** Long media gets the "Analyze the whole video?" sheet (plan §5.4). */
export const ASK_MIN_SECONDS = 10 * 60;

export interface AskRequest {
  url: string;
  title: string;
  site: string;
  durationMs: number;
  page: PageContext;
}

/**
 * What the popup, side panel and Listen route share around the stage: the provider
 * decision, the active tab and its "Analyze this page" chip with the analyze sheet, the
 * link field and the last-session row.
 */
export class ListenSurface {
  readonly listen: ListenController;
  providers: ProviderSettings | null = $state(null);
  tab: ActiveTab | null = $state(null);
  ask: AskRequest | null = $state(null);
  private stops: (() => void)[] = [];

  constructor(
    readonly engine: EngineClient,
    readonly platform: Platform,
    options: { recentLimit?: number } = {},
  ) {
    this.listen = new ListenController(engine, { recentLimit: options.recentLimit ?? 8, tabCapture: this.sources.includes('Tab') });
  }

  /** The source toggles this browser offers: "This tab" needs tab or element capture. */
  get sources(): ListenChoice[] {
    const caps = this.platform.capabilities;
    return caps.tabCapture || caps.target === 'firefox' ? ['Microphone', 'Tab', 'Hum'] : ['Microphone', 'Hum'];
  }

  get pageHost(): string | null {
    const url = this.tab?.url;
    return url && isAnalyzablePage(url) ? new URL(url).hostname.replace(/^(www|m)\./, '') : null;
  }

  /**
   * The chip shows on an analyzable page with Microphone or This tab selected; it hides
   * while Hum is selected and while a mix already runs for that page (its slot stays).
   */
  get chipVisible(): boolean {
    if (!this.pageHost || this.listen.source === 'Hum') return false;
    const running = this.listen.background;
    return !(running && running.source_url === this.tab?.url);
  }

  async start(): Promise<void> {
    this.tab = await this.platform.activeTab();
    this.listen.tabAudible = this.tab?.audible ?? null;
    this.providers = await loadProviderSettings();
    this.stops.push(watchProviderSettings((value) => (this.providers = value)));
    await this.listen.start(this.tab?.audible && this.sources.includes('Tab') ? 'Tab' : null);
  }

  stop(): void {
    this.stops.forEach((stop) => stop());
    this.listen.stop();
  }

  async refreshTab(): Promise<void> {
    this.tab = await this.platform.activeTab();
    this.listen.tabAudible = this.tab?.audible ?? null;
  }

  /** The link field: a link is analyzed, anything else searched as a lyric. */
  submit(text: string): void {
    if (isLink(text)) void this.listen.analyze(text);
    else void this.listen.searchLyrics(text);
  }

  /** "Analyze this page": long media asks first (or uses the remembered answer). */
  async analyzePage(): Promise<void> {
    const tab = this.tab;
    if (!tab?.url) return;
    const page: PageContext = { tab_id: tab.id ?? null, url: tab.url, title: tab.title ?? null, facts: null };
    const facts = await this.platform.pageFacts(tab).catch(() => null);
    if (facts) page.facts = facts as unknown as Record<string, unknown>;
    const duration = Math.max(0, ...(facts?.media ?? []).map((m) => m.duration ?? 0));
    if (duration >= ASK_MIN_SECONDS) {
      const site = this.siteName(tab.url);
      const remembered = (await settings.analyzeChoice.getValue())[site];
      if (remembered) {
        await this.analyzeLong(tab.url, page, remembered);
        return;
      }
      this.ask = { url: tab.url, title: facts?.title || tab.title || tab.url, site, durationMs: duration * 1000, page };
      return;
    }
    await this.listen.analyze(tab.url, page);
  }

  async answerAsk(choice: AnalyzeChoice, remember: boolean): Promise<void> {
    const ask = this.ask;
    this.ask = null;
    if (!ask) return;
    if (remember) await settings.analyzeChoice.setValue({ ...(await settings.analyzeChoice.getValue()), [ask.site]: choice });
    await this.analyzeLong(ask.url, ask.page, choice);
  }

  private async analyzeLong(url: string, page: PageContext, choice: AnalyzeChoice) {
    // The whole video is listened through in its tab; the stage stays free meanwhile.
    await this.listen.analyzeInBackground(url, page, choice === 'whole' ? 'Mix' : 'Auto');
  }

  siteName(url: string): string {
    try {
      const host = new URL(url).hostname.replace(/^(www|m|music)\./, '');
      const known: Record<string, string> = { 'youtube.com': 'YouTube', 'youtu.be': 'YouTube', 'soundcloud.com': 'SoundCloud', 'mixcloud.com': 'Mixcloud', 'twitch.tv': 'Twitch' };
      return known[host] ?? host;
    } catch {
      return url;
    }
  }

  /** "Open in ‹provider›" for a track in the preferred provider. */
  openInFor(track: Track | null): { label: string; url: string } | null {
    if (!track || !this.providers) return null;
    const link = primaryLink(providerLinks(track, this.providers.custom), this.providers.provider);
    if (!link) return null;
    const name = link.provider.kind === 'custom' ? this.providers.custom?.name ?? t('provider.customDefault') : serviceName(link.provider.service);
    return { label: t('result.openIn', name), url: link.url };
  }

  /** The row under the stage: a mix running in the background, else the newest session. */
  get recentRow(): LibraryEntry | null {
    return this.listen.background ?? this.listen.last;
  }

  /** ⌘/Ctrl+V on the stage with no field focused: identify the pasted link at once. */
  pasted(text: string): boolean {
    const trimmed = text.trim();
    if (!isLink(trimmed) || isRunning(this.listen.stage)) return false;
    void this.listen.analyze(trimmed);
    return true;
  }

  /** Where the stage's result was heard: "From this tab · youtube.com". */
  get resultContext(): string | null {
    const stage = this.listen.stage;
    if (stage.source === 'Tab' && this.pageHost) return t('result.fromTab', this.pageHost);
    return null;
  }
}
