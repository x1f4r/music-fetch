#!/usr/bin/env python3
"""Synthetic replay files for the microphone dev hook (DevFiles): no recordings needed.

    python3 -I tools/android-ux/replays.py <out-dir>

hum.wav: 14 s of a hummed melody (a voiced tone with harmonics, vibrato and breaths), enough
for the pitch trace and the hum ring. quiet.wav: 8 s of faint noise, which ends a listen with
"Too quiet to hear" without asking any recognizer.
"""
import math
import os
import random
import struct
import sys
import wave

RATE = 22050


def write(path, samples):
    with wave.open(path, "wb") as f:
        f.setnchannels(1)
        f.setsampwidth(2)
        f.setframerate(RATE)
        f.writeframes(b"".join(struct.pack("<h", max(-32767, min(32767, int(s * 32767)))) for s in samples))


def hum():
    notes = [0, 0, 2, 4, 4, 2, 0, -1, -3, -3, -1, 0, 2, 0, -1, -3]
    base = 220.0
    out, phase = [], 0.0
    for i, n in enumerate(notes):
        dur = 0.75 if i % 4 != 3 else 0.95
        f0 = base * 2 ** (n / 12)
        count = int(dur * RATE)
        for k in range(count):
            t = k / RATE
            vib = 1 + 0.006 * math.sin(2 * math.pi * 5.5 * t)
            phase += 2 * math.pi * f0 * vib / RATE
            env = min(1.0, t / 0.05) * min(1.0, (dur - t) / 0.08)
            voice = math.sin(phase) + 0.45 * math.sin(2 * phase) + 0.2 * math.sin(3 * phase)
            out.append(0.18 * env * voice + random.uniform(-0.002, 0.002))
        out += [random.uniform(-0.002, 0.002) for _ in range(int(0.06 * RATE))]
    return out


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    out = sys.argv[1]
    os.makedirs(out, exist_ok=True)
    random.seed(3)
    write(os.path.join(out, "hum.wav"), hum())
    write(os.path.join(out, "quiet.wav"), [random.uniform(-0.0015, 0.0015) for _ in range(8 * RATE)])
    print(out)


if __name__ == "__main__":
    main()
