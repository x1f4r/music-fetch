"""Vocal stems for every WAV in a directory (runs in the separate `sepenv` with audio-separator).

    sepenv/bin/python scripts/separate_vocals.py <in_dir> <out_dir> [model]

Prints one JSON line per file with the separation time.
"""

import json
import os
import sys
import time
from pathlib import Path

from audio_separator.separator import Separator

DEFAULT_MODEL = "vocals_mel_band_roformer.ckpt"


def main() -> int:
    in_dir, out_dir = Path(sys.argv[1]), Path(sys.argv[2])
    model = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_MODEL
    out_dir.mkdir(parents=True, exist_ok=True)
    overlap = int(os.environ.get("MFIDX_SEP_OVERLAP", "2"))
    batch = int(os.environ.get("MFIDX_SEP_BATCH", "4"))
    sep = Separator(
        output_dir=str(out_dir),
        output_format="WAV",
        output_single_stem="Vocals",
        model_file_dir=str(Path.home() / "melody-build" / "data" / "models" / "separator"),
        mdxc_params={
            "segment_size": 256,
            "override_model_segment_size": False,
            "batch_size": batch,
            "overlap": overlap,
            "pitch_shift": 0,
        },
    )
    t = time.time()
    sep.load_model(model_filename=model)
    print(
        json.dumps({"event": "loaded", "model": model, "seconds": round(time.time() - t, 2)}),
        flush=True,
    )
    for wav in sorted(in_dir.glob("*.wav")):
        t = time.time()
        outputs = sep.separate(str(wav), {"Vocals": f"{wav.stem}.vocals"})
        print(
            json.dumps(
                {
                    "event": "separated",
                    "file": wav.name,
                    "outputs": outputs,
                    "seconds": round(time.time() - t, 2),
                }
            ),
            flush=True,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
