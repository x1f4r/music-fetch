"""Offline builder for the Music Fetch melody index.

Stages (each resumable, state in SQLite under ``$MFIDX_ROOT/state``):

1. ``universe``  chart sources -> deduplicated songs with a popularity prior
2. ``resolve``   Deezer track per song (id, ISRC, duration, preview)
3. ``extract``   preview -> RMVPE f0 -> notes -> phrases (GPU)
4. ``midi``      Lakh MIDI melody tracks -> phrases
5. ``export``    JSONL for ``mf index build`` + stats.json
6. ``eval-*``    synthetic hums and the CHAD humming manifest
"""

__version__ = "1.0.0"
