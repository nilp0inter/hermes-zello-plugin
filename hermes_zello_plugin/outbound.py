"""Outbound audio pipeline: arbitrary audio file → Zello channel PTT.

Pipeline:

  audio_path (mp3/wav/ogg from elevenlabs)
        │
        ▼ ffmpeg -i ... -f s16le -ac 1 -ar 16000 -
        ▼
  PCM int16 mono @ 16 kHz
        │
        ▼ chunk into 60 ms frames (960 samples = 1920 bytes)
        ▼
  opuslib.Encoder(16000, 1, APPLICATION_VOIP).encode(frame, 960)
        │
        ▼ async with app.outbound_stream(codec_header, packet_duration_ms=60):
        ▼    await stream.send(opus_bytes)   # one packet per 60 ms wall clock
        ▼
  Zello channel

Notes
-----

* The plan §4.7 prescribes ``frames_per_packet=2, frame_size_ms=60`` (120 ms
  packets).  libopus' high-level encoder API produces single-frame packets;
  building Opus compound packets (TOC byte code-1) by hand for marginal
  bandwidth savings is not worth the complexity at the WAN bit-rates Zello
  serves.  We use ``frames_per_packet=1, frame_size_ms=60`` for a one-frame-
  per-packet wire shape.  Both shapes are valid Opus and Zello-spec
  compliant.  See ``DELTAS.md``.

* Pacing uses a drift-forward scheme: ``next_send_at += packet_duration_s``
  on every send, not ``now + packet_duration_s``.  A slow loop iteration
  catches up by sending the next packet with reduced (or zero) sleep,
  rather than accumulating real-time skew.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
from pathlib import Path
from typing import Optional

import opuslib  # type: ignore[import-untyped]
from aiozello.codec import encode_codec_header

logger = logging.getLogger(__name__)


# ── Tunables ──────────────────────────────────────────────────────────────

OUTBOUND_SAMPLE_RATE_HZ = 16000
OUTBOUND_FRAME_SIZE_MS = 60
OUTBOUND_FRAMES_PER_PACKET = 1  # see module docstring

# Derived constants
_SAMPLES_PER_FRAME = OUTBOUND_SAMPLE_RATE_HZ // 1000 * OUTBOUND_FRAME_SIZE_MS
_BYTES_PER_SAMPLE = 2  # int16
_BYTES_PER_FRAME = _SAMPLES_PER_FRAME * _BYTES_PER_SAMPLE
_PACKET_DURATION_MS = OUTBOUND_FRAME_SIZE_MS * OUTBOUND_FRAMES_PER_PACKET
_PACKET_DURATION_S = _PACKET_DURATION_MS / 1000.0


# ── ffmpeg decode ─────────────────────────────────────────────────────────


class FfmpegDecodeError(RuntimeError):
    """ffmpeg subprocess exited non-zero or wrote no audio."""


async def decode_to_pcm16(
    audio_path: str | Path,
    *,
    sample_rate_hz: int = OUTBOUND_SAMPLE_RATE_HZ,
    ffmpeg_bin: Optional[str] = None,
) -> bytes:
    """Decode ``audio_path`` to PCM int16 mono at *sample_rate_hz*.

    Subprocess: ``ffmpeg -i <path> -f s16le -acodec pcm_s16le -ac 1
    -ar <rate> pipe:1``.  ffmpeg is resolved via ``shutil.which`` if
    *ffmpeg_bin* is not provided.

    Raises :class:`FfmpegDecodeError` on failure (non-zero exit or empty
    output).
    """
    ffmpeg = ffmpeg_bin or shutil.which("ffmpeg")
    if not ffmpeg:
        raise FfmpegDecodeError("ffmpeg binary not found on PATH")

    proc = await asyncio.create_subprocess_exec(
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-i",
        str(audio_path),
        "-f",
        "s16le",
        "-acodec",
        "pcm_s16le",
        "-ac",
        "1",
        "-ar",
        str(sample_rate_hz),
        "pipe:1",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise FfmpegDecodeError(
            f"ffmpeg exited {proc.returncode}: {stderr.decode('utf-8', 'replace').strip()}"
        )
    if not stdout:
        raise FfmpegDecodeError("ffmpeg produced empty PCM output")
    return stdout


# ── PCM chunking ──────────────────────────────────────────────────────────


def chunk_pcm(pcm: bytes, *, bytes_per_frame: int = _BYTES_PER_FRAME) -> list[bytes]:
    """Split *pcm* into fixed-size frames; zero-pad the last short frame.

    Padding (rather than dropping) preserves the trailing audio at the cost
    of <60 ms of silence at the very end — much less than the next PTT-start
    gap on the listener side.
    """
    if not pcm:
        return []
    out: list[bytes] = []
    for i in range(0, len(pcm), bytes_per_frame):
        frame = pcm[i : i + bytes_per_frame]
        if len(frame) < bytes_per_frame:
            frame = frame + b"\x00" * (bytes_per_frame - len(frame))
        out.append(frame)
    return out


# ── Pacing primitive (testable in isolation) ──────────────────────────────


class DriftForwardPacer:
    """Drift-forward pacer for fixed-duration packet emission.

    Call :meth:`reset` once before sending the first packet; call
    :meth:`tick` after each ``send`` to wait for the next emission slot.
    A slow iteration auto-catches by reducing the next sleep.

    Time and sleep are injected so tests can run without real wall-clock.
    """

    def __init__(
        self,
        packet_duration_s: float,
        *,
        time_fn=None,
        sleep_fn=None,
    ):
        self.packet_duration_s = packet_duration_s
        # Don't bind the running loop at __init__ — defer to first reset()/tick()
        # so the pacer is safe to construct outside a running event loop
        # (avoids the get_event_loop_policy().get_event_loop() deprecation
        # warning in Python 3.12+).
        self._time_fn_override = time_fn
        self._sleep_fn_override = sleep_fn
        self._time = None  # resolved lazily
        self._sleep = None
        self._next_send_at: float = 0.0

    def _resolve(self) -> None:
        if self._time is not None:
            return
        if self._time_fn_override is not None:
            self._time = self._time_fn_override
        else:
            self._time = asyncio.get_running_loop().time
        self._sleep = self._sleep_fn_override or asyncio.sleep

    def reset(self) -> None:
        self._resolve()
        self._next_send_at = self._time()

    async def tick(self) -> None:
        self._resolve()
        self._next_send_at += self.packet_duration_s
        delta = self._next_send_at - self._time()
        if delta > 0:
            await self._sleep(delta)


# ── End-to-end pipeline ───────────────────────────────────────────────────


async def stream_audio_to_zello(
    app,
    audio_path: str | Path,
    *,
    sample_rate_hz: int = OUTBOUND_SAMPLE_RATE_HZ,
    ffmpeg_bin: Optional[str] = None,
) -> int:
    """Decode, encode, and stream *audio_path* to the connected Zello channel.

    *app* is an ``aiozello.Application`` whose WebSocket is connected.
    Returns the number of opus packets sent.

    Raises :class:`FfmpegDecodeError` on decode failure; any aiozello /
    network error propagates from the ``async with`` block.
    """
    pcm = await decode_to_pcm16(audio_path, sample_rate_hz=sample_rate_hz, ffmpeg_bin=ffmpeg_bin)
    frames = chunk_pcm(pcm)
    if not frames:
        logger.warning("outbound: no PCM frames after decode (audio_path=%s)", audio_path)
        return 0

    encoder = opuslib.Encoder(sample_rate_hz, 1, opuslib.APPLICATION_VOIP)
    codec_header = encode_codec_header(
        sample_rate_hz, OUTBOUND_FRAMES_PER_PACKET, OUTBOUND_FRAME_SIZE_MS
    )

    pacer = DriftForwardPacer(_PACKET_DURATION_S)
    packets_sent = 0

    async with app.outbound_stream(codec_header, _PACKET_DURATION_MS) as stream:
        pacer.reset()
        last = len(frames) - 1
        for i, frame in enumerate(frames):
            opus_bytes = encoder.encode(frame, _SAMPLES_PER_FRAME)
            await stream.send(opus_bytes)
            packets_sent += 1
            # Skip the sleep after the FINAL packet — nothing follows it, and
            # waiting another packet_duration_s before sending stop_stream
            # serves no purpose.
            if i < last:
                await pacer.tick()

    logger.info("outbound: streamed %d opus packets (%.2fs of audio)",
                packets_sent, packets_sent * _PACKET_DURATION_S)
    return packets_sent
