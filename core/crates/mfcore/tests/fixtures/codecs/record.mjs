// node record.mjs: records a synthetic signal with MediaRecorder in headless
// Chromium (WebM/Opus) and Firefox (Ogg/Opus), as the extension's microphone
// and tab captures will. Nothing is played: the oscillators feed only the
// recorder. Needs puppeteer-core; writes mediarecorder-*.{webm,ogg}.
import { writeFile } from 'node:fs/promises';
import puppeteer from 'puppeteer-core';

const browsers = [
  { name: 'chrome', file: 'mediarecorder-chrome.webm', opts: { executablePath: process.env.CHROME ?? '/usr/bin/chromium', headless: true, args: ['--no-sandbox', '--disable-audio-output', '--autoplay-policy=no-user-gesture-required'] } },
  { name: 'firefox', file: 'mediarecorder-firefox.ogg', opts: { browser: 'firefox', executablePath: process.env.FIREFOX ?? '/usr/bin/firefox', headless: true, extraPrefsFirefox: { 'media.autoplay.default': 0, 'media.volume_scale': '0.0' } } },
];

for (const { name, file, opts } of browsers) {
  const browser = await puppeteer.launch(opts);
  try {
    const page = await browser.newPage();
    const bytes = await page.evaluate(async () => {
      const ctx = new AudioContext({ sampleRate: 48000 });
      await ctx.resume();
      const dest = ctx.createMediaStreamDestination();
      const gain = ctx.createGain();
      gain.gain.value = 0.25;
      gain.connect(dest);
      for (const [f, at] of [[220, 0], [330, 0], [330, 0.5], [495, 0.5]]) {
        const o = ctx.createOscillator();
        o.frequency.value = f;
        o.connect(gain);
        o.start(ctx.currentTime + at);
        o.stop(ctx.currentTime + at + (at ? 1.0 : 0.5));
      }
      const rec = new MediaRecorder(dest.stream);
      const chunks = [];
      rec.ondataavailable = (e) => chunks.push(e.data);
      const done = new Promise((r) => (rec.onstop = r));
      rec.start(250);
      // Headless audio clocks can lag the wall clock; wait for 1.6 s of audio.
      const end = ctx.currentTime + 1.6;
      while (ctx.currentTime < end) await new Promise((r) => setTimeout(r, 50));
      rec.stop();
      await done;
      const blob = new Blob(chunks, { type: rec.mimeType });
      return { type: rec.mimeType, data: Array.from(new Uint8Array(await blob.arrayBuffer())) };
    });
    await writeFile(new URL(file, import.meta.url), Buffer.from(bytes.data));
    console.log(name, bytes.type, bytes.data.length, 'bytes');
  } finally {
    await browser.close();
  }
}
