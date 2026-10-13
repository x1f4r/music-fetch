import os
from pathlib import Path

import numpy as np
import pytest

torch = pytest.importorskip("torch")

from melody_index.rmvpe import E2E, Rmvpe, mel_filterbank  # noqa: E402

WEIGHTS = (
    Path(os.environ.get("MFIDX_ROOT", Path.home() / "melody-build"))
    / "data"
    / "models"
    / "rmvpe.pt"
)


def test_mel_filterbank_matches_librosa():
    librosa = pytest.importorskip("librosa")
    ref = librosa.filters.mel(sr=16000, n_fft=1024, n_mels=128, fmin=30, fmax=8000, htk=True)
    assert np.allclose(mel_filterbank(), ref, atol=1e-6)


def test_model_shapes():
    model = E2E(4, 1, (2, 2)).eval()
    with torch.no_grad():
        out = model(torch.zeros(2, 128, 64))
    assert out.shape == (2, 64, 360)


@pytest.mark.skipif(not WEIGHTS.exists(), reason="rmvpe.pt not downloaded")
def test_weights_load_strict_and_track_a_tone():
    model = Rmvpe(WEIGHTS)
    t = np.arange(16000 * 2) / 16000
    f = 220.0
    tone = (
        sum(0.7**h * np.sin(2 * np.pi * f * h * t) for h in range(1, 10)).astype(np.float32) * 0.2
    )
    ((f0, conf),) = model.f0([tone], threshold=0.03)
    voiced = f0[conf > 0.5]
    assert voiced.size > 100
    assert abs(np.median(1200 * np.log2(voiced / f))) < 20
