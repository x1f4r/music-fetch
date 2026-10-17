import type { EngineClient } from '../engine/client';
import { EngineError } from '../engine/client';
import type { LibraryEntry, PageContext, SessionKind } from '../engine/types';
import { libraryQuery } from '../engine/types';
import { adoptSession, errorLine, idleStage, isRunning, reduceStage, startingStage, type StageState } from '../models/stage';
import { settings, type ListenChoice } from '../settings/settings';

/**
 * The listen stage shared by the popup, side panel and app tab: starts and stops
 * sessions on the engine, follows the stage session's events and keeps the newest
 * library entry for the "last session" row. Surfaces render `stage` and call the actions.
 */
export class ListenController {
  stage: StageState = $state(idleStage());
  source: ListenChoice = $state('Tab');
  recent: LibraryEntry[] = $state([]);
  /** The engine host could not be reached. */
  offline = $state(false);

  private unsubscribe: (() => void) | null = null;

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
    try {
      const [active] = await this.engine.activeSessions();
      if (active && !isRunning(this.stage)) {
        const source: ListenChoice | null = active.kind === 'Hum' ? 'Hum' : active.source === 'Microphone' ? 'Microphone' : active.source ? 'Tab' : null;
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

  async chooseSource(source: ListenChoice): Promise<void> {
    if (isRunning(this.stage)) return;
    this.source = source;
    await settings.lastSource.setValue(source);
  }

  /** The listen button: start with the selected source, or stop the running session. */
  async toggle(): Promise<void> {
    if (isRunning(this.stage)) {
      await this.finish();
      return;
    }
    const source = this.source;
    const kind: SessionKind = source === 'Hum' ? 'Hum' : source === 'Microphone' ? 'Listen' : 'SystemAudio';
    this.stage = startingStage(source, kind);
    await this.run(async () => {
      const autoStop = await settings.autoStop.getValue();
      return this.engine.startListening(source === 'Tab' ? 'Tab' : 'Microphone', source === 'Hum' ? 'Hum' : 'Auto', { auto_stop: autoStop });
    });
  }

  /** Paste a link, or "Analyze this page". */
  async analyze(input: string, page: PageContext | null = null): Promise<void> {
    if (isRunning(this.stage)) await this.cancel();
    this.stage = startingStage(null, 'Link');
    await this.run(() => this.engine.analyze(input, 'Auto', page));
  }

  async searchLyrics(text: string): Promise<void> {
    this.stage = startingStage(null, 'Lyrics');
    await this.run(() => this.engine.searchLyrics(text));
  }

  /** The user stopped capture: identify what was heard. */
  async finish(): Promise<void> {
    const id = this.stage.sessionId;
    if (id) await this.engine.finishSession(id).catch(() => undefined);
  }

  async cancel(): Promise<void> {
    const id = this.stage.sessionId;
    this.stage = idleStage();
    if (id) await this.engine.cancelSession(id).catch(() => undefined);
  }

  /** Back to the button from a result, no match or error. */
  reset(): void {
    if (!isRunning(this.stage)) this.stage = idleStage();
  }

  private async run(start: () => Promise<string>): Promise<void> {
    try {
      const id = await start();
      this.stage = adoptSession(this.stage, id);
    } catch (error) {
      const code = error instanceof EngineError && error.code !== 'Disconnected' && error.code !== 'Timeout' && error.code !== 'Unknown' ? error.code : null;
      if (!code) this.offline = true;
      this.stage = {
        ...idleStage(),
        phase: 'failed',
        error: { code: code ?? 'Internal', message: error instanceof Error ? error.message : String(error) },
        line: { key: code ? errorLine(code) : 'error.engine' },
        lineActive: true,
      };
    }
  }
}
