"""Tests for ``hermes_zello_plugin.outbound`` — pacing + chunking only.

The full pipeline (ffmpeg subprocess, opuslib encode, aiozello outbound
stream) is exercised by the smoke test; here we cover the deterministic
units.
"""

from __future__ import annotations

import pytest

from hermes_zello_plugin.outbound import (
    DriftForwardPacer,
    _BYTES_PER_FRAME,
    chunk_pcm,
)


# ── chunk_pcm ────────────────────────────────────────────────────────────


def test_chunk_pcm_empty_returns_empty_list():
    assert chunk_pcm(b"") == []


def test_chunk_pcm_exact_multiple_no_padding():
    pcm = b"\x01\x02" * (_BYTES_PER_FRAME * 3 // 2)
    assert len(pcm) == _BYTES_PER_FRAME * 3
    frames = chunk_pcm(pcm)
    assert len(frames) == 3
    for f in frames:
        assert len(f) == _BYTES_PER_FRAME


def test_chunk_pcm_short_tail_is_zero_padded():
    short = b"\xFF" * (_BYTES_PER_FRAME + 100)
    frames = chunk_pcm(short)
    assert len(frames) == 2
    assert frames[0] == b"\xFF" * _BYTES_PER_FRAME
    assert frames[1].startswith(b"\xFF" * 100)
    assert frames[1].endswith(b"\x00" * (_BYTES_PER_FRAME - 100))
    assert len(frames[1]) == _BYTES_PER_FRAME


# ── DriftForwardPacer ────────────────────────────────────────────────────


class _FakeClock:
    """Deterministic time + sleep stand-in for the pacer.

    ``time()`` returns the simulated 'now'.  ``sleep(d)`` advances the
    clock by *d* and records the sleep duration.  Tests can also pre-bump
    the clock between ticks via :meth:`advance` to simulate a slow loop
    iteration.
    """

    def __init__(self):
        self.now = 0.0
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    async def sleep(self, d: float) -> None:
        self.sleeps.append(d)
        self.now += d

    def advance(self, d: float) -> None:
        self.now += d


@pytest.mark.asyncio
async def test_pacer_steady_state_sleeps_full_packet_duration():
    clk = _FakeClock()
    pacer = DriftForwardPacer(0.12, time_fn=clk.time, sleep_fn=clk.sleep)
    pacer.reset()

    for _ in range(5):
        await pacer.tick()

    # Each tick should sleep 0.12 s when no other work consumed time.
    assert clk.sleeps == [pytest.approx(0.12)] * 5
    assert clk.now == pytest.approx(0.60)


@pytest.mark.asyncio
async def test_pacer_drift_forward_catches_up_after_slow_iteration():
    clk = _FakeClock()
    pacer = DriftForwardPacer(0.12, time_fn=clk.time, sleep_fn=clk.sleep)
    pacer.reset()

    await pacer.tick()  # 0 → 0.12
    assert clk.sleeps == [pytest.approx(0.12)]

    # Simulate a slow iteration: 0.18s of work between ticks.
    clk.advance(0.18)
    await pacer.tick()  # next_send_at=0.24, now=0.30 → delta=-0.06, pacer skips sleep
    assert clk.sleeps == [pytest.approx(0.12)]  # no new sleep recorded

    # Next tick: next_send_at=0.36, now=0.30 → sleeps 0.06 to catch up.
    await pacer.tick()
    assert clk.sleeps == [pytest.approx(0.12), pytest.approx(0.06)]


@pytest.mark.asyncio
async def test_pacer_drift_forward_does_not_skew_long_term():
    """After N ticks, total wall time should approach N * packet_duration_s.

    A naive ``sleep(packet_duration_s)`` after each send accumulates real
    skew if the sends themselves take time; drift-forward pacing converges.
    """
    clk = _FakeClock()
    pacer = DriftForwardPacer(0.12, time_fn=clk.time, sleep_fn=clk.sleep)
    pacer.reset()

    n_ticks = 50
    per_tick_work_s = 0.02  # the "send" itself costs 20 ms
    for _ in range(n_ticks):
        clk.advance(per_tick_work_s)
        await pacer.tick()

    # Total wall time should be exactly n_ticks * packet_duration_s, because
    # the pacer pegs to schedule, not "sleep + work" cycles.
    expected = n_ticks * 0.12
    assert clk.now == pytest.approx(expected, rel=1e-9)
