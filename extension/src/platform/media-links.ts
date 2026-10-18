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
