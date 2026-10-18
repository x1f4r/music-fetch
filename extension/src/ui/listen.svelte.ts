import type { EngineClient } from '../engine/client';
import { EngineError, isCaptureErrorCode } from '../engine/client';
import type { LibraryEntry, MelodyIndexStatus, PageContext, SessionKind } from '../engine/types';
import { libraryQuery } from '../engine/types';
import {
  adoptSession,
  downloadStage,
  failedStage,
  idleStage,
  isRunning,
  reduceStage,
  startingStage,
  type StageState,
} from '../models/stage';
import { settings, type ListenChoice } from '../settings/settings';

/** The melody library's size before the engine has said (the 1.1 index). */
export const MELODY_DOWNLOAD_BYTES = 55_200_000;

/**
 * The listen stage shared by the popup, side panel and app tab: starts and stops
 * sessions on the engine, follows the stage session's events and keeps the newest
 * library entries for the "last session" row. Surfaces render `stage` and call the actions.
 */
export class ListenController {
  stage: StageState = $state(idleStage());
  source: ListenChoice = $state('Tab');
  recent: LibraryEntry[] = $state([]);
  /** The engine host could not be reached. */
  offline = $state(false);
  /** Whether the tab "This tab" would capture is playing sound (null: unknown). */
  tabAudible: boolean | null = $state(null);

  private unsubscribe: (() => void) | null = null;
  private melody: MelodyIndexStatus | null = null;

  constructor(
    private readonly engine: EngineClient,
    private readonly options: { recentLimit?: number; tabCapture?: boolean } = {},
  ) {}

  /** Connect, restore a session that is still running (the popup was reopened) and load the library. */
  async start(defaultSource: ListenChoice | null): Promise<void> {
    this.unsubscribe = this.engine.subscribe({
      sessionEvent: (sessionId, event) => {
        this.stage = reduceStage(this.stage, sessionId, event);
      },
      libraryChanged: () => void this.loadRecent(),
      connectionChanged: (connected) => {
        if (connected) this.offline = false;
      },
    });
    const last = await settings.lastSource.getValue();
    this.source = defaultSource ?? last ?? (this.options.tabCapture === false ? 'Microphone' : 'Tab');
    if (this.source === 'Tab' && this.options.tabCapture === false) this.source = 'Microphone';
    if (this.stage.phase === 'idle') this.stage = idleStage(this.source);
    try {
      const [active] = await this.engine.activeSessions();
      if (active && !isRunning(this.stage)) {
        const source: ListenChoice | null = active.kind === 'Hum' ? 'Hum' : active.source === 'Microphone' ? 'Microphone' : active.source ? 'Tab' : null;
        if (source) this.source = source;
        this.stage = { ...startingStage(source, active.kind), sessionId: active.id, phase: active.source ? 'listening' : 'working' };
      }
      await this.loadRecent();
    } catch {
      this.offline = true;
    }
  }

  stop(): void {
    this.unsubscribe?.();
    this.unsubscribe = null;
  }

  async loadRecent(): Promise<void> {
    try {
      this.recent = await this.engine.library(libraryQuery({ limit: this.options.recentLimit ?? 1 }));
    } catch {
      this.offline = true;
    }
  }

  /**
   * A source toggle: chooses the method without listening. After a no-match, an error
   * or a prompt it returns the stage to idle for the new method (DESIGN §5.2).
   */
  async chooseSource(source: ListenChoice): Promise<void> {
    if (isRunning(this.stage) || this.stage.download) return;
    this.source = source;
    if (this.stage.phase !== 'matched' && this.stage.phase !== 'candidates') this.stage = idleStage(source);
    await settings.lastSource.setValue(source);
  }

  /** The listen button: start with the selected source, or stop the running session. */
  async toggle(): Promise<void> {
    if (this.stage.download) return;
    if (isRunning(this.stage)) {
      await this.finish();
      return;
    }
    const source = this.source;
    if (source === 'Hum' && !(await this.melodyInstalled())) {
      // Humming needs the melody library: ask first (plan §5.5).
      this.stage = { ...idleStage('Hum'), melodyMissing: true };
      return;
    }
    const kind: SessionKind = source === 'Hum' ? 'Hum' : source === 'Microphone' ? 'Listen' : 'SystemAudio';
    this.stage = startingStage(source, kind, { tabAudible: this.tabAudible });
    await this.run(async () => {
      const autoStop = await settings.autoStop.getValue();
      return this.engine.startListening(source === 'Tab' ? 'Tab' : 'Microphone', source === 'Hum' ? 'Hum' : 'Auto', { auto_stop: autoStop });
    });
  }

  /** Paste a link, or "Analyze this page". `mode` is the analyze sheet's choice. */
  async analyze(input: string, page: PageContext | null = null, mode: 'Auto' | 'Mix' = 'Auto'): Promise<void> {
    if (isRunning(this.stage)) await this.cancel();
    this.stage = startingStage(null, 'Link', { input });
    await this.run(() => this.engine.analyze(input, mode, page));
  }

  async searchLyrics(text: string): Promise<void> {
    if (isRunning(this.stage)) await this.cancel();
    this.stage = startingStage(null, 'Lyrics', { input: text });
    await this.run(() => this.engine.searchLyrics(text));
  }

  /** "Try again": the same method, or the same link or lyric again. */
  async tryAgain(): Promise<void> {
    const { kind, input } = this.stage;
    if (input && kind === 'Link') return this.analyze(input);
    if (input && kind === 'Lyrics') return this.searchLyrics(input);
    this.reset();
    await this.toggle();
  }

  /** Switch to `source` and listen at once ("Hum it", "Use microphone"). */
  async listenWith(source: ListenChoice): Promise<void> {
    this.stage = idleStage(source);
    await this.chooseSource(source);
    await this.toggle();
  }

  /** The user stopped capture: identify what was heard. */
  async finish(): Promise<void> {
    const id = this.stage.sessionId;
    if (id) await this.engine.finishSession(id).catch(() => undefined);
  }

  async cancel(): Promise<void> {
    const id = this.stage.sessionId;
    this.stage = idleStage(this.source);
    if (id) await this.engine.cancelSession(id).catch(() => undefined);
  }

  /** Back to the button from a result, no match, error or prompt. */
  reset(): void {
    if (!isRunning(this.stage) && !this.stage.download) this.stage = idleStage(this.source);
  }

  /** "Download" on the melody prompt: the stage shows the download, then Hum is ready. */
  async installMelody(): Promise<void> {
    const total = this.melody?.download_bytes ?? MELODY_DOWNLOAD_BYTES;
    this.stage = downloadStage(total);
    try {
      const id = await this.engine.installMelodyIndex();
      this.stage = adoptSession(this.stage, id);
      this.melody = null;
    } catch (error) {
      this.stage = failedStage(idleStage('Hum'), 'Network', error instanceof Error ? error.message : String(error));
    }
  }

  async cancelDownload(): Promise<void> {
    const id = this.stage.download?.sessionId;
    if (id) await this.engine.cancelSession(id).catch(() => undefined);
    this.stage = idleStage('Hum');
  }

  private async melodyInstalled(): Promise<boolean> {
    if (this.melody?.installed) return true;
    try {
      this.melody = await this.engine.melodyIndexStatus();
      return this.melody.installed;
    } catch {
      // Let the engine say what is missing (Notice::MelodyIndexMissing).
      return true;
    }
  }

  private async run(start: () => Promise<string>): Promise<void> {
    try {
      const id = await start();
      this.stage = adoptSession(this.stage, id);
    } catch (error) {
      const raw = error instanceof EngineError ? error.code : null;
      const known = raw !== null && raw !== 'Disconnected' && raw !== 'Timeout' && raw !== 'Unknown';
      if (!known) this.offline = true;
      const message = error instanceof Error ? error.message : String(error);
      if (known && raw) {
        this.stage = failedStage(this.stage, raw, message);
        if (isCaptureErrorCode(raw) && raw === 'MicDenied') void settings.micGrant.setValue('denied');
      } else {
        this.stage = { ...failedStage(this.stage, 'Internal', message), line: { key: 'error.engine' } };
      }
    }
  }
}
