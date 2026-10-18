import TAP_URL from './tap-worklet.js?url&no-inline';

/**
 * The packaged tap worklet, for extension pages (offscreen document, popup, side panel,
 * app tab), whose CSP allows scripts from the extension only. Content scripts load the
 * same source from a Blob URL instead (`tap.ts`), so don't import this there.
 */
export const TAP_WORKLET_URL: string = TAP_URL;
