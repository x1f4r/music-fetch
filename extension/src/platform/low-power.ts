import type { LowPowerSetting } from '../settings/settings';

/**
 * Low power (extension plan §2.7): the setting, or on "auto" the a05-1 thresholds
 * (≤ 4 CPUs or < 4 GiB). Firefox has no `deviceMemory`, so Firefox for Android counts
 * as low-end at ≤ 8 CPUs. Used by the engine host at start and by Settings when the
 * user changes it (same device, same answer).
 */
export function resolveLowPower(setting: LowPowerSetting, android: boolean, cpus: number, memoryGiB: number | null): boolean {
  if (setting === 'on') return true;
  if (setting === 'off') return false;
  if (cpus <= 4) return true;
  if (memoryGiB !== null && memoryGiB < 4) return true;
  return android && memoryGiB === null && cpus <= 8;
}
