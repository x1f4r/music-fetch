import { StreamSource } from './stream-source';
import type { TapOptions } from './tap';

export type MicPermission = 'granted' | 'prompt' | 'denied' | 'unknown';

/** Why the microphone can't be used. */
export class MicError extends Error {
  constructor(
    readonly reason: 'MicNotGranted' | 'MicUnavailable',
    message: string,
  ) {
    super(message);
    this.name = 'MicError';
  }
}

/** `permissions.query({name: 'microphone'})`, which both browsers answer for extension pages. */
export async function micPermission(): Promise<MicPermission> {
  try {
    const status = await navigator.permissions.query({ name: 'microphone' as PermissionName });
    return status.state;
  } catch {
    return 'unknown';
  }
}

/** Raw input: no echo cancellation, noise suppression or gain control (extension plan §3.2). */
export const RAW_AUDIO: MediaTrackConstraints = { echoCancellation: false, noiseSuppression: false, autoGainControl: false };

/**
 * The device to record from: the first connected entry of the user's priority list, as
 * in the apps (Settings → Listening). Null means the system default. `default` and
 * `communications` are Chromium's aliases, not devices.
 */
export function chooseMic(devices: readonly Pick<MediaDeviceInfo, 'deviceId' | 'kind'>[], priority: readonly string[]): string | null {
  const present = new Set(devices.filter((d) => d.kind === 'audioinput').map((d) => d.deviceId));
  return priority.find((id) => present.has(id) && id !== 'default' && id !== 'communications') ?? null;
}

export interface MicOptions {
  /** Device ids, most preferred first. */
  priority?: string[];
  /**
   * Fail fast unless the permission is granted. Off only on the grant page, which may
   * prompt. Elsewhere a prompt can't be shown (offscreen document) or hangs (a Firefox
   * sidebar before the grant), so the host sends the user to `#/grant-mic` instead (S2).
   */
  requireGrant?: boolean;
  tap?: TapOptions;
}

/** The microphone, after the one-time grant in an extension tab (extension plan §3.2, §5.5). */
export class MicSource extends StreamSource {
  readonly kind = 'mic' as const;
  private deviceId: string | null = null;
  private readonly onDeviceChange = () => void this.followDevices();

  constructor(private readonly options: MicOptions = {}) {
    super(options.tap ?? {});
  }

  /** The device in use (null: the system default). */
  get device(): string | null {
    return this.deviceId;
  }

  async start(): Promise<void> {
    if (!navigator.mediaDevices?.getUserMedia) throw new MicError('MicUnavailable', 'This browser has no microphone access here.');
    if (this.options.requireGrant !== false) {
      const permission = await micPermission();
      if (permission === 'denied' || permission === 'prompt') throw new MicError('MicNotGranted', 'Allow the microphone first.');
    }
    const stream = await this.open();
    await this.attach(stream, stream.getAudioTracks()[0]?.label ?? 'microphone');
    navigator.mediaDevices.addEventListener('devicechange', this.onDeviceChange);
  }

  override stop(): void {
    navigator.mediaDevices?.removeEventListener('devicechange', this.onDeviceChange);
    super.stop();
  }

  private async open(): Promise<MediaStream> {
    const devices = await navigator.mediaDevices.enumerateDevices().catch(() => []);
    const preferred = chooseMic(devices, this.options.priority ?? []);
    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        audio: preferred ? { ...RAW_AUDIO, deviceId: { exact: preferred } } : RAW_AUDIO,
        video: false,
      });
      this.deviceId = stream.getAudioTracks()[0]?.getSettings().deviceId ?? preferred;
      return stream;
    } catch (error) {
      if (error instanceof DOMException && error.name === 'NotAllowedError') throw new MicError('MicNotGranted', 'Allow the microphone first.');
      if (error instanceof DOMException && (error.name === 'NotFoundError' || error.name === 'OverconstrainedError')) {
        if (preferred) {
          this.options.priority = (this.options.priority ?? []).filter((id) => id !== preferred);
          return this.open();
        }
        throw new MicError('MicUnavailable', 'No microphone is connected.');
      }
      throw error;
    }
  }

  /** A device came or went: keep recording from the first connected one in the list. */
  private async followDevices(): Promise<void> {
    if (this.ended) return;
    const devices = await navigator.mediaDevices.enumerateDevices().catch(() => []);
    const want = chooseMic(devices, this.options.priority ?? []);
    const live = this.stream?.getAudioTracks().some((t) => t.readyState === 'live') ?? false;
    if (live && (want === null || want === this.deviceId)) return;
    try {
      const stream = await this.open();
      if (this.ended) {
        stream.getTracks().forEach((t) => t.stop());
        return;
      }
      await this.replace(stream, stream.getAudioTracks()[0]?.label ?? 'microphone');
      this.events.emit({ type: 'device', deviceId: this.deviceId ?? 'default', label: stream.getAudioTracks()[0]?.label ?? '' });
    } catch (error) {
      this.end('error', error instanceof Error ? error.message : String(error));
    }
  }
}
