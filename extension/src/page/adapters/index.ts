import { adapterFor, type AdapterId } from '../facts';
import type { SiteAdapter } from '../site';
import { bandcamp } from './bandcamp';
import { generic } from './generic';
import { mixcloud } from './mixcloud';
import { soundcloud } from './soundcloud';
import { streaming } from './streaming';
import { twitch } from './twitch';
import { youtube } from './youtube';

export const ADAPTERS: Record<AdapterId, SiteAdapter> = { youtube, soundcloud, mixcloud, twitch, bandcamp, streaming, generic };

/**
 * The adapter for a page, by host (YouTube, SoundCloud, …), else the generic one. The
 * e2e build also honours `<meta name="mf-test-adapter">`, so local fixture pages on
 * 127.0.0.1 can stand in for the sites; release builds don't contain that branch.
 */
export function siteAdapter(hostname: string, doc?: Document): SiteAdapter {
  if (import.meta.env.MODE === 'e2e' && doc) {
    const forced = doc.querySelector<HTMLMetaElement>('meta[name="mf-test-adapter"]')?.content as AdapterId | undefined;
    if (forced && forced in ADAPTERS) return ADAPTERS[forced];
  }
  return ADAPTERS[adapterFor(hostname)];
}
