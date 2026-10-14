"""RMVPE vocal pitch estimator (Wei et al., Interspeech 2023, arXiv 2306.15412).

The network definition mirrors the reference implementation (Dream-High/RMVPE,
Apache-2.0; also shipped in RVC-Project, MIT) so that the published ``rmvpe.pt``
weights from ``lj1995/VoiceConversionWebUI`` load with ``strict=True``. Input is
16 kHz mono audio; output is f0 at a 10 ms hop (160 samples) plus the peak
salience per frame, which serves as voicing confidence.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import torch
import torch.nn.functional as F
from torch import nn

SAMPLE_RATE = 16000
HOP = 160
N_FFT = 1024
N_MELS = 128
N_CLASS = 360
MEL_FMIN = 30.0
MEL_FMAX = 8000.0
CENTS_OFFSET = 1997.3794084376191


class BiGRU(nn.Module):
    def __init__(self, input_features: int, hidden_features: int, num_layers: int) -> None:
        super().__init__()
        self.gru = nn.GRU(
            input_features,
            hidden_features,
            num_layers=num_layers,
            batch_first=True,
            bidirectional=True,
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.gru(x)[0]


class ConvBlockRes(nn.Module):
    def __init__(self, in_channels: int, out_channels: int, momentum: float = 0.01) -> None:
        super().__init__()
        self.conv = nn.Sequential(
            nn.Conv2d(in_channels, out_channels, (3, 3), (1, 1), (1, 1), bias=False),
            nn.BatchNorm2d(out_channels, momentum=momentum),
            nn.ReLU(),
            nn.Conv2d(out_channels, out_channels, (3, 3), (1, 1), (1, 1), bias=False),
            nn.BatchNorm2d(out_channels, momentum=momentum),
            nn.ReLU(),
        )
        self.is_shortcut = in_channels != out_channels
        if self.is_shortcut:
            self.shortcut = nn.Conv2d(in_channels, out_channels, (1, 1))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        if self.is_shortcut:
            return self.conv(x) + self.shortcut(x)
        return self.conv(x) + x


class ResEncoderBlock(nn.Module):
    def __init__(
        self,
        in_channels: int,
        out_channels: int,
        kernel_size,
        n_blocks: int = 1,
        momentum: float = 0.01,
    ) -> None:
        super().__init__()
        self.n_blocks = n_blocks
        self.conv = nn.ModuleList([ConvBlockRes(in_channels, out_channels, momentum)])
        for _ in range(n_blocks - 1):
            self.conv.append(ConvBlockRes(out_channels, out_channels, momentum))
        self.kernel_size = kernel_size
        if kernel_size is not None:
            self.pool = nn.AvgPool2d(kernel_size=kernel_size)

    def forward(self, x: torch.Tensor):
        for block in self.conv:
            x = block(x)
        if self.kernel_size is not None:
            return x, self.pool(x)
        return x


class Encoder(nn.Module):
    def __init__(
        self,
        in_channels: int,
        in_size: int,
        n_encoders: int,
        kernel_size,
        n_blocks: int,
        out_channels: int = 16,
        momentum: float = 0.01,
    ) -> None:
        super().__init__()
        self.n_encoders = n_encoders
        self.bn = nn.BatchNorm2d(in_channels, momentum=momentum)
        self.layers = nn.ModuleList()
        for _ in range(n_encoders):
            self.layers.append(
                ResEncoderBlock(in_channels, out_channels, kernel_size, n_blocks, momentum)
            )
            in_channels = out_channels
            out_channels *= 2
            in_size //= 2
        self.out_size = in_size
        self.out_channel = out_channels

    def forward(self, x: torch.Tensor):
        concat = []
        x = self.bn(x)
        for layer in self.layers:
            skip, x = layer(x)
            concat.append(skip)
        return x, concat


class Intermediate(nn.Module):
    def __init__(
        self,
        in_channels: int,
        out_channels: int,
        n_inters: int,
        n_blocks: int,
        momentum: float = 0.01,
    ) -> None:
        super().__init__()
        self.layers = nn.ModuleList(
            [ResEncoderBlock(in_channels, out_channels, None, n_blocks, momentum)]
        )
        for _ in range(n_inters - 1):
            self.layers.append(
                ResEncoderBlock(out_channels, out_channels, None, n_blocks, momentum)
            )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        for layer in self.layers:
            x = layer(x)
        return x


class ResDecoderBlock(nn.Module):
    def __init__(
        self, in_channels: int, out_channels: int, stride, n_blocks: int = 1, momentum: float = 0.01
    ) -> None:
        super().__init__()
        out_padding = (0, 1) if stride == (1, 2) else (1, 1)
        self.n_blocks = n_blocks
        self.conv1 = nn.Sequential(
            nn.ConvTranspose2d(
                in_channels,
                out_channels,
                (3, 3),
                stride,
                (1, 1),
                output_padding=out_padding,
                bias=False,
            ),
            nn.BatchNorm2d(out_channels, momentum=momentum),
            nn.ReLU(),
        )
        self.conv2 = nn.ModuleList([ConvBlockRes(out_channels * 2, out_channels, momentum)])
        for _ in range(n_blocks - 1):
            self.conv2.append(ConvBlockRes(out_channels, out_channels, momentum))

    def forward(self, x: torch.Tensor, concat: torch.Tensor) -> torch.Tensor:
        x = self.conv1(x)
        x = torch.cat((x, concat), dim=1)
        for block in self.conv2:
            x = block(x)
        return x


class Decoder(nn.Module):
    def __init__(
        self, in_channels: int, n_decoders: int, stride, n_blocks: int, momentum: float = 0.01
    ) -> None:
        super().__init__()
        self.layers = nn.ModuleList()
        for _ in range(n_decoders):
            out_channels = in_channels // 2
            self.layers.append(
                ResDecoderBlock(in_channels, out_channels, stride, n_blocks, momentum)
            )
            in_channels = out_channels

    def forward(self, x: torch.Tensor, concat: list[torch.Tensor]) -> torch.Tensor:
        for i, layer in enumerate(self.layers):
            x = layer(x, concat[-1 - i])
        return x


class DeepUnet(nn.Module):
    def __init__(
        self,
        kernel_size,
        n_blocks: int,
        en_de_layers: int = 5,
        inter_layers: int = 4,
        in_channels: int = 1,
        en_out_channels: int = 16,
    ) -> None:
        super().__init__()
        self.encoder = Encoder(
            in_channels, N_MELS, en_de_layers, kernel_size, n_blocks, en_out_channels
        )
        self.intermediate = Intermediate(
            self.encoder.out_channel // 2, self.encoder.out_channel, inter_layers, n_blocks
        )
        self.decoder = Decoder(self.encoder.out_channel, en_de_layers, kernel_size, n_blocks)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x, concat = self.encoder(x)
        x = self.intermediate(x)
        return self.decoder(x, concat)


class E2E(nn.Module):
    def __init__(
        self,
        n_blocks: int,
        n_gru: int,
        kernel_size,
        en_de_layers: int = 5,
        inter_layers: int = 4,
        in_channels: int = 1,
        en_out_channels: int = 16,
    ) -> None:
        super().__init__()
        self.unet = DeepUnet(
            kernel_size, n_blocks, en_de_layers, inter_layers, in_channels, en_out_channels
        )
        self.cnn = nn.Conv2d(en_out_channels, 3, (3, 3), padding=(1, 1))
        if n_gru:
            self.fc = nn.Sequential(
                BiGRU(3 * N_MELS, 256, n_gru),
                nn.Linear(512, N_CLASS),
                nn.Dropout(0.25),
                nn.Sigmoid(),
            )
        else:
            self.fc = nn.Sequential(nn.Linear(3 * N_MELS, N_CLASS), nn.Dropout(0.25), nn.Sigmoid())

    def forward(self, mel: torch.Tensor) -> torch.Tensor:
        # mel: [B, n_mels, T] -> salience [B, T, 360]
        x = mel.transpose(-1, -2).unsqueeze(1)
        x = self.cnn(self.unet(x)).transpose(1, 2).flatten(-2)
        return self.fc(x)


def mel_filterbank(
    sr: int = SAMPLE_RATE,
    n_fft: int = N_FFT,
    n_mels: int = N_MELS,
    fmin: float = MEL_FMIN,
    fmax: float = MEL_FMAX,
) -> np.ndarray:
    """HTK-scale triangular filters with Slaney area normalization
    (identical to ``librosa.filters.mel(..., htk=True)``)."""

    def hz_to_mel(f):
        return 2595.0 * np.log10(1.0 + np.asarray(f, dtype=np.float64) / 700.0)

    def mel_to_hz(m):
        return 700.0 * (10.0 ** (np.asarray(m, dtype=np.float64) / 2595.0) - 1.0)

    fftfreqs = np.fft.rfftfreq(n=n_fft, d=1.0 / sr)
    mel_f = mel_to_hz(np.linspace(hz_to_mel(fmin), hz_to_mel(fmax), n_mels + 2))
    fdiff = np.diff(mel_f)
    ramps = np.subtract.outer(mel_f, fftfreqs)
    weights = np.zeros((n_mels, len(fftfreqs)), dtype=np.float64)
    for i in range(n_mels):
        lower = -ramps[i] / fdiff[i]
        upper = ramps[i + 2] / fdiff[i + 1]
        weights[i] = np.maximum(0.0, np.minimum(lower, upper))
    enorm = 2.0 / (mel_f[2 : n_mels + 2] - mel_f[:n_mels])
    weights *= enorm[:, None]
    return weights.astype(np.float32)


class Rmvpe:
    """Batched RMVPE inference."""

    def __init__(self, weights: Path, device: str | None = None, half: bool = False) -> None:
        self.device = torch.device(device or ("cuda" if torch.cuda.is_available() else "cpu"))
        model = E2E(4, 1, (2, 2))
        state = torch.load(weights, map_location="cpu", weights_only=True)
        model.load_state_dict(state, strict=True)
        model.eval()
        self.half = half and self.device.type == "cuda"
        self.model = (model.half() if self.half else model).to(self.device)
        self.mel_basis = torch.from_numpy(mel_filterbank()).to(self.device)
        self.window = torch.hann_window(N_FFT).to(self.device)
        cents = CENTS_OFFSET + 20.0 * np.arange(N_CLASS)
        self.cents_mapping = np.pad(cents, (4, 4))

    @torch.no_grad()
    def mel(self, audio: torch.Tensor) -> torch.Tensor:
        """[B, N] float32 at 16 kHz -> log-mel [B, 128, N // 160 + 1]."""
        spec = torch.stft(
            audio,
            n_fft=N_FFT,
            hop_length=HOP,
            win_length=N_FFT,
            window=self.window,
            center=True,
            return_complex=True,
        )
        mag = spec.abs()
        mel = torch.matmul(self.mel_basis, mag)
        return torch.log(torch.clamp(mel, min=1e-5))

    @torch.no_grad()
    def salience(self, audios: list[np.ndarray]) -> list[np.ndarray]:
        """Per-input salience maps [T_i, 360] (T_i = len_i // 160 + 1)."""
        if not audios:
            return []
        n = max(len(a) for a in audios)
        batch = np.zeros((len(audios), n), dtype=np.float32)
        for i, a in enumerate(audios):
            batch[i, : len(a)] = a
        x = torch.from_numpy(batch).to(self.device)
        mel = self.mel(x)
        frames = mel.shape[-1]
        n_pad = 32 * ((frames - 1) // 32 + 1) - frames
        if n_pad > 0:
            mel = F.pad(mel, (0, n_pad), mode="reflect")
        if self.half:
            mel = mel.half()
        hidden = self.model(mel)[:, :frames].float().cpu().numpy()
        return [hidden[i, : len(a) // HOP + 1] for i, a in enumerate(audios)]

    def decode(self, salience: np.ndarray, threshold: float) -> tuple[np.ndarray, np.ndarray]:
        """Local weighted average of cents around the peak bin -> (f0 Hz, 0 = unvoiced; confidence)."""
        if salience.shape[0] == 0:
            return np.zeros(0, np.float32), np.zeros(0, np.float32)
        center = np.argmax(salience, axis=1) + 4
        padded = np.pad(salience, ((0, 0), (4, 4)))
        idx = center[:, None] + np.arange(-4, 5)[None, :]
        weights = np.take_along_axis(padded, idx, axis=1)
        cents = (weights * self.cents_mapping[idx]).sum(1) / np.maximum(weights.sum(1), 1e-9)
        conf = salience.max(axis=1)
        f0 = 10.0 * 2.0 ** (cents / 1200.0)
        f0[conf <= threshold] = 0.0
        return f0.astype(np.float32), conf.astype(np.float32)

    def f0(
        self, audios: list[np.ndarray], threshold: float = 0.03
    ) -> list[tuple[np.ndarray, np.ndarray]]:
        return [self.decode(s, threshold) for s in self.salience(audios)]
