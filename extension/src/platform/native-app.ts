/**
 * Links into the Music Fetch app on Android (`musicfetch://`, see the app's AppIntents).
 *
 * Firefox for Android only offers an app for a link in a visible tab, so the surface opens
 * the link in a new tab and closes the popup. The `intent:` form without a package lets
 * Firefox ask "Open this link in Music Fetch app?" when the app is installed and go to the
 * download page when it isn't (a `musicfetch:` link would leave a blank tab, and naming the
 * package sends people to a store listing that doesn't exist).
 */

/** Where the app is downloaded (the project's releases). */
export const APP_DOWNLOAD_URL = 'https://github.com/x1f4r/music-fetch/releases/latest';

/** The app's Listen stage, started with a source: the microphone or humming. */
export type NativeListenSource = 'mic' | 'hum';

export function nativeListenUrl(source: NativeListenSource, fallback: string = APP_DOWNLOAD_URL): string {
  return `intent://listen?source=${source}#Intent;scheme=musicfetch;S.browser_fallback_url=${encodeURIComponent(fallback)};end`;
}
