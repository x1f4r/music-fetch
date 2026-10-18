// jsdom ships no types; the adapter tests use only this much of it.
declare module 'jsdom' {
  export class JSDOM {
    constructor(html: string, options?: { url?: string; runScripts?: 'dangerously' | 'outside-only'; pretendToBeVisual?: boolean });
    readonly window: Window & typeof globalThis;
  }
}
