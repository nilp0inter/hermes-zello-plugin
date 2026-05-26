"""Tests for ``hermes_zello_plugin.aggregator.UtteranceAggregator``."""

from __future__ import annotations

import asyncio
from typing import Iterable

import pytest

from aiozello.codec import encode_codec_header
from aiozello.protocol import StreamStart, StreamStop

from hermes_zello_plugin.aggregator import UtteranceAggregator


# ── Fixtures: fake aiozello.IncomingAudioStream / Application ─────────────


class _FakeStream:
    """Stand-in for ``aiozello.stream.IncomingAudioStream``.

    Exposes ``decode()`` as an async generator yielding the supplied
    pre-decoded PCM chunks, and ``drain()`` as a no-op coroutine.
    """

    def __init__(self, pcm_chunks: Iterable[bytes]):
        self._chunks = list(pcm_chunks)
        self.drained = False

    async def decode(self):
        for chunk in self._chunks:
            await asyncio.sleep(0)  # yield to loop so tests stay reactive
            yield chunk

    async def drain(self) -> None:
        self.drained = True


class _FakeApp:
    def __init__(self):
        self.streams: dict[int, _FakeStream] = {}


# 16 kHz mono = 2 bytes/sample → 32_000 bytes/sec.
_SR = 16000
_CODEC_HEADER_16K_60MS = encode_codec_header(_SR, 1, 60)
_HALF_SEC_PCM = b"\x10\x20" * (_SR // 2)   # 0.5 s at 16k mono s16le
_TWO_SEC_PCM = b"\x30\x40" * (_SR * 2)     # 2.0 s


def _stream_start(stream_id: int, sender: str) -> StreamStart:
    return StreamStart(
        type="audio",
        codec="opus",
        packet_duration=60,
        stream_id=stream_id,
        channel="pichufletos",
        sender=sender,
        key="",
        codec_header=_CODEC_HEADER_16K_60MS,
    )


# ── Flush callback recorder ──────────────────────────────────────────────


class _Recorder:
    def __init__(self):
        self.calls: list[tuple[str, bytes, int]] = []

    async def __call__(self, sender: str, pcm: bytes, sample_rate_hz: int) -> None:
        self.calls.append((sender, pcm, sample_rate_hz))


# ── Tests ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_single_allowed_stream_flushes_after_window():
    app = _FakeApp()
    app.streams[1] = _FakeStream([_HALF_SEC_PCM])
    flush = _Recorder()

    agg = UtteranceAggregator(
        app,
        flush,
        allowed_users={"nilp0inter_dev"},
        allow_all=False,
        window_s=0.05,
        max_utterance_s=300.0,
    )

    await agg.on_stream_start(_stream_start(1, "nilp0inter_dev"))
    # Wait for the consumer task to drain + flush timer to fire.
    await asyncio.sleep(0.20)

    assert len(flush.calls) == 1
    sender, pcm, sr = flush.calls[0]
    assert sender == "nilp0inter_dev"
    assert pcm == _HALF_SEC_PCM
    assert sr == _SR


@pytest.mark.asyncio
async def test_two_consecutive_streams_same_user_concatenate():
    app = _FakeApp()
    app.streams[1] = _FakeStream([_HALF_SEC_PCM])
    app.streams[2] = _FakeStream([_HALF_SEC_PCM])
    flush = _Recorder()

    agg = UtteranceAggregator(
        app, flush,
        allowed_users={"nilp0inter_dev"},
        allow_all=False,
        window_s=0.20,
        max_utterance_s=300.0,
    )

    await agg.on_stream_start(_stream_start(1, "nilp0inter_dev"))
    # Let stream 1 drain fully (it's a fast generator); start stream 2
    # before the 0.20s flush window expires.
    await asyncio.sleep(0.05)
    await agg.on_stream_start(_stream_start(2, "nilp0inter_dev"))
    await asyncio.sleep(0.40)  # past the window

    assert len(flush.calls) == 1
    _, pcm, _ = flush.calls[0]
    assert pcm == _HALF_SEC_PCM + _HALF_SEC_PCM


@pytest.mark.asyncio
async def test_different_users_get_independent_flushes():
    app = _FakeApp()
    app.streams[1] = _FakeStream([_HALF_SEC_PCM])
    app.streams[2] = _FakeStream([_HALF_SEC_PCM])
    flush = _Recorder()

    agg = UtteranceAggregator(
        app, flush,
        allowed_users={"alice", "bob"},
        allow_all=False,
        window_s=0.05,
        max_utterance_s=300.0,
    )

    await agg.on_stream_start(_stream_start(1, "alice"))
    await agg.on_stream_start(_stream_start(2, "bob"))
    await asyncio.sleep(0.20)

    senders = sorted(c[0] for c in flush.calls)
    assert senders == ["alice", "bob"]


@pytest.mark.asyncio
async def test_denied_user_drains_stream_without_flushing():
    app = _FakeApp()
    fake_stream = _FakeStream([_HALF_SEC_PCM])
    app.streams[1] = fake_stream
    flush = _Recorder()

    agg = UtteranceAggregator(
        app, flush,
        allowed_users={"someone_else"},
        allow_all=False,
        window_s=0.05,
        max_utterance_s=300.0,
    )

    await agg.on_stream_start(_stream_start(1, "intruder"))
    await asyncio.sleep(0.15)

    assert flush.calls == []
    assert fake_stream.drained is True


@pytest.mark.asyncio
async def test_allow_all_bypasses_allowlist():
    app = _FakeApp()
    app.streams[1] = _FakeStream([_HALF_SEC_PCM])
    flush = _Recorder()

    agg = UtteranceAggregator(
        app, flush,
        allowed_users=set(),  # empty allow-list
        allow_all=True,
        window_s=0.05,
        max_utterance_s=300.0,
    )

    await agg.on_stream_start(_stream_start(1, "anyone"))
    await asyncio.sleep(0.15)

    assert len(flush.calls) == 1
    assert flush.calls[0][0] == "anyone"


@pytest.mark.asyncio
async def test_max_utterance_ceiling_triggers_early_flush():
    app = _FakeApp()
    # 2s of PCM in a single stream, window is 5s but max is 1s.
    app.streams[1] = _FakeStream([_TWO_SEC_PCM])
    flush = _Recorder()

    agg = UtteranceAggregator(
        app, flush,
        allowed_users={"nilp0inter_dev"},
        allow_all=False,
        window_s=5.0,
        max_utterance_s=1.0,
    )

    await agg.on_stream_start(_stream_start(1, "nilp0inter_dev"))
    # Consumer task pushes 2s of PCM in one shot → ceiling crosses → flush.
    await asyncio.sleep(0.10)

    assert len(flush.calls) == 1
    _, pcm, _ = flush.calls[0]
    assert pcm == _TWO_SEC_PCM


@pytest.mark.asyncio
async def test_flush_all_drains_pending_state():
    app = _FakeApp()
    app.streams[1] = _FakeStream([_HALF_SEC_PCM])
    flush = _Recorder()

    agg = UtteranceAggregator(
        app, flush,
        allowed_users={"nilp0inter_dev"},
        allow_all=False,
        window_s=10.0,
        max_utterance_s=300.0,
    )

    await agg.on_stream_start(_stream_start(1, "nilp0inter_dev"))
    # Don't wait for the window — call flush_all() to force the pending
    # utterance out.
    await asyncio.sleep(0.05)  # let consumer drain into buffer
    await agg.flush_all()

    assert len(flush.calls) == 1
    _, pcm, _ = flush.calls[0]
    assert pcm == _HALF_SEC_PCM


@pytest.mark.asyncio
async def test_unknown_stream_id_is_no_op():
    app = _FakeApp()
    # Note: NO entry in app.streams for stream_id=99.
    flush = _Recorder()

    agg = UtteranceAggregator(
        app, flush,
        allowed_users={"nilp0inter_dev"},
        allow_all=False,
        window_s=0.05,
        max_utterance_s=300.0,
    )

    await agg.on_stream_start(_stream_start(99, "nilp0inter_dev"))
    await asyncio.sleep(0.20)

    assert flush.calls == []


@pytest.mark.asyncio
async def test_on_stream_stop_is_a_no_op():
    """``on_stream_stop`` should not crash and not produce a flush by itself
    (decode() exhaustion drives the flush)."""
    app = _FakeApp()
    flush = _Recorder()

    agg = UtteranceAggregator(
        app, flush,
        allowed_users={"nilp0inter_dev"},
        allow_all=False,
        window_s=0.05,
        max_utterance_s=300.0,
    )

    await agg.on_stream_stop(StreamStop(stream_id=42))
    await asyncio.sleep(0.10)

    assert flush.calls == []
