import { siteAdapter } from './adapters';
import { servePage, type PageEvent } from './bridge';
import { readPageFacts, type PageFacts } from './facts';
import { emptyFacts, finite, type SiteFacts } from './site';

/** The attribute the page script puts on the player element so the agent can find it. */
export const PLAYER_ATTR = 'data-mf-player';

/** What `mark` tells the agent about the player it marked. */
export interface MarkedPlayer {
  found: boolean;
  durationMs: number | null;
  live: boolean;
  protected: boolean;
  title: string | null;
  /** The player's own seek, play and pause exist (YouTube). */
  controls: boolean;
}

interface PageGlobal {
  played: HTMLMediaElement[];
  stop: (() => void) | null;
}

const GLOBAL = '__mfPageScript';

/**
 * The MAIN-world page script (extension plan §3.3). Injected on demand after the agent,
 * it answers the agent's requests with the site adapter: facts from the page's own
 * objects, the player element (marked with `data-mf-player`), seek/play/pause and video
 * quality through the site's player API, and events for in-page navigation and ads.
 *
 * It also remembers every element that starts playing from now on, including detached
 * ones (`new Audio()`, SoundCloud), which the agent can't otherwise find.
 */
export function startPageScript(win: Window & typeof globalThis, doc: Document, nonce: string): () => void {
  const holder = win as unknown as Record<string, PageGlobal | undefined>;
  let global = holder[GLOBAL];
  if (!global) {
    global = { played: [], stop: null };
    holder[GLOBAL] = global;
    const played = global.played;
    const remember = (element: HTMLMediaElement) => {
      if (!played.includes(element)) played.push(element);
      if (played.length > 20) played.shift();
    };
    // Installed once per page, even when the script is injected again.
    const proto = win.HTMLMediaElement.prototype;
    const play = proto.play;
    proto.play = function (this: HTMLMediaElement, ...args: []) {
      remember(this);
      return play.apply(this, args);
    };
    doc.addEventListener(
      'play',
      (event) => {
        if (event.target instanceof win.HTMLMediaElement) remember(event.target);
      },
      true,
    );
  }
  global.stop?.();
  const played = global.played;
  const adapter = siteAdapter(win.location.hostname, doc);
  let marked: HTMLMediaElement | null = null;

  const element = (): HTMLMediaElement | null => adapter.element(win, doc, played);

  const describe = (el: HTMLMediaElement): MarkedPlayer => {
    const facts = adapter.facts(win, doc);
    // During an ad the element holds the ad: trust the player API, then the page's facts.
    const ad = adapter.ad?.(win, doc) ?? false;
    const element = (finite(el.duration) ?? 0) * 1000 || null;
    const fromPage = facts.durationMs ?? null;
    const duration = adapter.durationMs?.(win) ?? (ad ? fromPage : (element ?? fromPage));
    return {
      found: true,
      durationMs: duration,
      live: Boolean(facts.live) || el.duration === Infinity,
      protected: Boolean(el.mediaKeys),
      title: facts.title ?? null,
      controls: Boolean(adapter.seek),
    };
  };

  const server = servePage(win, nonce, {
    hello: () => ({ adapter: adapter.id, version: adapter.version }),
    facts: (): PageFacts & { site: SiteFacts } => {
      const page = readPageFacts(doc, win.navigator.mediaSession);
      const site = { ...emptyFacts(adapter.id, adapter.version), ...adapter.facts(win, doc) };
      // During an ad the element holds the ad: give it the content's length the site knows.
      if (site.durationMs && (adapter.ad?.(win, doc) ?? false)) {
        const player = adapter.element(win, doc, played);
        page.media = page.media.map((m, i) => (doc.querySelectorAll('audio, video')[i] === player ? { ...m, duration: site.durationMs! / 1000 } : m));
      }
      return { ...page, site, nowPlaying: page.nowPlaying ?? site.nowPlaying };
    },
    mark: async (): Promise<MarkedPlayer> => {
      let el = element();
      if (!el && adapter.reveal?.(win, doc)) {
        await new Promise((resolve) => setTimeout(resolve, 400));
        el = element();
      }
      if (!el) return { found: false, durationMs: null, live: false, protected: false, title: null, controls: false };
      if (!el.isConnected) {
        // A detached player: put it in the document (invisible, playback unchanged) so the agent can reach it.
        el.hidden = true;
        doc.documentElement.appendChild(el);
      }
      marked?.removeAttribute(PLAYER_ATTR);
      el.setAttribute(PLAYER_ATTR, nonce);
      marked = el;
      return describe(el);
    },
    seek: (ms) => adapter.seek?.(win, Number(ms)) ?? false,
    play: () => adapter.play?.(win, doc) ?? false,
    pause: () => adapter.pause?.(win, doc) ?? false,
    quality: (args) => {
      const { on, restore } = (args ?? {}) as { on?: boolean; restore?: string | null };
      return adapter.lowQuality?.(win, Boolean(on), restore ?? null) ?? null;
    },
    state: () => ({ ad: adapter.ad?.(win, doc) ?? false, durationMs: adapter.durationMs?.(win) ?? null }),
    reveal: () => adapter.reveal?.(win, doc) ?? false,
  });

  const emit = (event: PageEvent) => server.emit(event);
  const onNavigate = () => emit({ type: 'navigated', url: win.location.href });
  for (const name of adapter.navigationEvents ?? []) win.addEventListener(name, onNavigate);

  // Ads (YouTube): watch the player's own class, and look again every second in case the
  // site replaced the player element.
  let ad = adapter.ad?.(win, doc) ?? false;
  const checkAd = () => {
    const now = adapter.ad!(win, doc);
    if (now !== ad) {
      ad = now;
      emit({ type: 'ad', showing: now });
    }
  };
  const observer = adapter.ad ? new win.MutationObserver(checkAd) : null;
  let observed: Element | null = null;
  const observePlayer = () => {
    const player = doc.getElementById('movie_player') ?? doc.querySelector('.html5-video-player');
    if (!observer || player === observed) return;
    observer.disconnect();
    observed = player;
    if (player) observer.observe(player, { attributes: true, attributeFilter: ['class'] });
  };
  observePlayer();
  const poll = adapter.ad
    ? win.setInterval(() => {
        observePlayer();
        checkAd();
      }, 1000)
    : null;

  const stop = () => {
    server.close();
    observer?.disconnect();
    if (poll !== null) win.clearInterval(poll);
    for (const name of adapter.navigationEvents ?? []) win.removeEventListener(name, onNavigate);
  };
  global.stop = stop;
  return stop;
}
