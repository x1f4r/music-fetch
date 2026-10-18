import type { EngineClient } from '../engine/client';
import { isLink } from '../engine/links';
import type { LibraryEntry, PageContext, Track } from '../engine/types';
import { t } from '../i18n';
import { isRunning, promptStage } from '../models/stage';
import { nativeListenUrl } from '../platform/native-app';
import { isAnalyzablePage, type ActiveTab } from '../platform/surfaces';
import { primaryLink, providerLinks } from '../providers/links';
import { serviceName } from '../providers/services';
import { loadProviderSettings, settings, watchProviderSettings, type AnalyzeChoice, type ListenChoice, type ProviderSettings } from '../settings/settings';
import type { Platform } from './context';
import { ListenController } from './listen.svelte';

/** Long media gets the "Analyze the whole video?" sheet (plan §5.4). */
export const ASK_MIN_SECONDS = 10 * 60;

export interface AskRequest {
  /** `long`: whole video or a quick check; `short`: check 3 short parts or just listen. */
  kind: 'long' | 'short';
  url: string;
  title: string;
  site: string;
  durationMs: number;
  page: PageContext;
}

const MEDIA_HOSTS = /(^|\.)(youtube\.com|youtu\.be|soundcloud\.com|mixcloud\.com|twitch\.tv|vimeo\.com|tiktok\.com)$/;

/** A link to a video or audio page (not a streaming catalog): its sound only plays on the page. */
export function isMediaPageLink(text: string): boolean {
  try {
    const url = new URL(text.trim());
    const host = url.hostname.replace(/^(www|m)\./, '');
    if (host === 'music.youtube.com') return false;
    if (host === 'soundcloud.com' && url.pathname.includes('/sets/')) return false;
    if ((host === 'youtube.com' || host === 'youtu.be') && url.searchParams.has('list') && !url.searchParams.has('v')) return false;
    return MEDIA_HOSTS.test(host);
  } catch {
    return false;
  }
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
    options: { recentLimit?: number; closeOnHandOff?: boolean } = {},
  ) {
    const close = options.closeOnHandOff ?? false;
    this.listen = new ListenController(engine, {
      recentLimit: options.recentLimit ?? 8,
      tabCapture: this.sources.includes('Tab'),
      handOff: platform.capabilities.microphone === 'app' ? (source) => this.handOff(source, close) : undefined,
    });
  }

  /**
   * Microphone and Hum where the extension can't use the microphone (Firefox for Android):
   * the app's Listen stage, through a tab Firefox offers to open in the app. The popup
   * closes so that tab shows.
   */
  private async handOff(source: 'Microphone' | 'Hum', close: boolean): Promise<void> {
    await this.platform.openUrl(nativeListenUrl(source === 'Hum' ? 'hum' : 'mic'));
    if (close) this.platform.close();
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

  /**
   * The link field: a link is analyzed, anything else searched as a lyric. A pasted video
   * or audio page link (not this tab's) only plays on its page: offer to open it.
   */
  submit(text: string): void {
    if (!isLink(text)) {
      void this.listen.searchLyrics(text);
      return;
    }
    if (isMediaPageLink(text) && text.trim() !== this.tab?.url) {
      this.listen.stage = promptStage({ key: 'page.playsOnPage' }, text.trim());
      return;
    }
    void this.listen.analyze(text);
  }

  /** "Open and analyze": the page opens in a tab; the capture coordinator (X4) takes over there. */
  async openAndAnalyze(url: string): Promise<void> {
    this.listen.reset();
    await this.platform.openUrl(url);
  }

  /**
   * "Analyze this page": a site the engine may not read asks for access first; a known
   * streaming page answers from its own metadata; long media asks whole or quick, a short
   * video asks to check 3 parts or just listen (or uses the remembered answer).
   */
  async analyzePage(): Promise<void> {
    const tab = this.tab;
    if (!tab?.url) return;
    if ((await this.platform.hostAccess(tab.url)) === 'needed') {
      this.listen.stage = promptStage({ key: 'page.allowHost', args: [this.pageHost ?? tab.url] }, tab.url);
      return;
    }
    const page: PageContext = { tab_id: tab.id ?? null, url: tab.url, title: tab.title ?? null, facts: null };
    const facts = await this.platform.pageFacts(tab).catch(() => null);
    if (facts) page.facts = facts as unknown as Record<string, unknown>;
    const duration = Math.max(0, ...(facts?.media ?? []).map((m) => m.duration ?? 0));
    const video = facts?.media.some((m) => m.tag === 'video') ?? false;
    const kind: AskRequest['kind'] | null = duration >= ASK_MIN_SECONDS ? 'long' : video && duration > 0 && !facts?.nowPlaying ? 'short' : null;
    if (!kind) {
      await this.listen.analyze(tab.url, page);
      return;
    }
    const site = this.siteName(tab.url);
    const remembered = (await settings.analyzeChoice.getValue())[kind === 'long' ? site : `${site}:short`];
    if (remembered) {
      await this.answer(tab.url, page, remembered);
      return;
    }
    this.ask = { kind, url: tab.url, title: facts?.title || tab.title || tab.url, site, durationMs: duration * 1000, page };
  }

  /** "Allow": the optional host permission, then the analysis it was for. */
  async allowHost(url: string): Promise<void> {
    const granted = await this.platform.requestHost(url);
    this.listen.reset();
    if (granted && url === this.tab?.url) await this.analyzePage();
  }

  async answerAsk(choice: AnalyzeChoice, remember: boolean): Promise<void> {
    const ask = this.ask;
    this.ask = null;
    if (!ask) return;
    const key = ask.kind === 'long' ? ask.site : `${ask.site}:short`;
    if (remember) await settings.analyzeChoice.setValue({ ...(await settings.analyzeChoice.getValue()), [key]: choice });
    await this.answer(ask.url, ask.page, choice);
  }

  private async answer(url: string, page: PageContext, choice: AnalyzeChoice) {
    if (choice === 'listen') {
      await this.listen.listenWith('Tab');
      return;
    }
    if (choice === 'check') {
      await this.listen.analyze(url, page);
      return;
    }
    // A long video is listened through in its tab; the stage stays free meanwhile.
    await this.listen.analyzeInBackground(url, page, choice === 'whole' ? 'Mix' : 'Auto');
  }

  /** The stage's result came from this page's own metadata (plan §5.3 step 1). */
  get resultFromPage(): boolean {
    const stage = this.listen.stage;
    return stage.phase === 'matched' && stage.kind === 'Link' && stage.input !== null && stage.input === this.tab?.url;
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
    if (this.resultFromPage && this.pageHost) return t('result.fromPage', this.pageHost);
    if (stage.source === 'Tab' && this.pageHost) return t('result.fromTab', this.pageHost);
    return null;
  }
}
