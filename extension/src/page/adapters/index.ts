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

/** The adapter for a page, by host (YouTube, SoundCloud, …), else the generic one. */
export function siteAdapter(hostname: string): SiteAdapter {
  return ADAPTERS[adapterFor(hostname)];
}
