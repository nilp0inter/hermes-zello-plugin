"""Per-user utterance aggregator.

Zello phone apps cap a single PTT at ~60 seconds.  A user dictating a
30 s – 1.5 min memo therefore arrives as 1–3 back-to-back streams.  This
module groups consecutive streams from the same sender into one logical
utterance and emits the full concatenated PCM via a supplied async
``flush_cb``.

Lifecycle per user:

  on_stream_start(sender)
      │ — allow-list check; deny → drain & drop
      │ — find IncomingAudioStream by stream_id in app.streams
      │ — spawn consumer task: async for pcm in stream.decode(): buf += pcm
      ▼
  consumer task exits (stream_stop drains decode() generator)
      │ — append pcm chunk to per-user buffer
      │ — cancel pending flush timer, arm a new one for window_s
      │ — if buffered duration ≥ max_utterance_s → flush now
      ▼
  flush timer fires (or max-utterance ceiling, or .flush_all() at shutdown)
      │ — pop per-user state
      │ — invoke flush_cb(sender, pcm_bytes, sample_rate_hz)
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

from aiozello.codec import decode_codec_header
from aiozello.protocol import StreamStart, StreamStop

from .allowlist import is_user_authorized

logger = logging.getLogger(__name__)


FlushCallback = Callable[[str, bytes, int], Awaitable[None]]


# Bound for ``flush_all`` waiting on in-flight consumer tasks during
# shutdown.  Each consumer is blocked on ``IncomingAudioStream.decode()``
# which only exits when ``None`` is pushed into the per-stream queue.
# When the WS dies mid-PTT (deploy restart, network blip), aiozello never
# delivers ``on_stream_stop`` so no ``None`` is ever pushed — leaving the
# decode loop blocked forever.  We push the None sentinels ourselves and
# bound the wait so a misbehaving stream cannot wedge shutdown.
_FLUSH_ALL_CONSUMER_TIMEOUT_S = 2.0


@dataclass
class _UserState:
    sender: str
    sample_rate_hz: int
    pcm: bytearray = field(default_factory=bytearray)
    started_at: float = 0.0
    timer_handle: Optional[asyncio.TimerHandle] = None


class UtteranceAggregator:
    """Groups same-user Zello streams into one logical utterance.

    The aggregator does NOT touch the network — it consumes ``app.streams``
    (the dict aiozello populates on ``on_stream_start``) and emits via a
    caller-supplied ``flush_cb``.  Tests can drive it without a live WS.
    """

    def __init__(
        self,
        app,
        flush_cb: FlushCallback,
        *,
        allowed_users,
        allow_all: bool,
        window_s: float,
        max_utterance_s: float,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self._app = app
        self._flush_cb = flush_cb
        self._allowed_users = allowed_users
        self._allow_all = allow_all
        self._window_s = float(window_s)
        self._max_utterance_s = float(max_utterance_s)
        self._loop = loop  # resolved lazily
        self._states: dict[str, _UserState] = {}
        self._consumer_tasks: set[asyncio.Task] = set()

    # ── aiozello callback handlers ────────────────────────────────────────

    async def on_stream_start(self, event: StreamStart) -> None:
        """Called by aiozello when any user keys their mic on the channel."""
        sender = event.sender or ""
        if not is_user_authorized(
            sender, allowed_users=self._allowed_users, allow_all=self._allow_all
        ):
            logger.warning(
                "zello: dropping stream from unauthorized sender=%r stream_id=%s",
                sender,
                event.stream_id,
            )
            await self._drain_silently(event.stream_id)
            return

        stream = self._app.streams.get(event.stream_id)
        if stream is None:
            logger.warning(
                "zello: on_stream_start for unknown stream_id=%s sender=%r",
                event.stream_id,
                sender,
            )
            return

        sample_rate_hz, _, _ = decode_codec_header(event.codec_header)
        task = self._spawn(self._consume_stream(sender, stream, sample_rate_hz))
        self._consumer_tasks.add(task)
        task.add_done_callback(self._consumer_tasks.discard)

    async def on_stream_stop(self, event: StreamStop) -> None:
        """No-op: the consumer task discovers EOF via decode()'s None sentinel.

        aiozello dispatches stream_stop and ALSO sends ``None`` into the
        per-stream queue (see ``Application._handle_message``), so the
        decode() async generator exits naturally.  We don't need to do
        anything here — keeping the method around lets the plugin wire it
        as a callback for completeness / future hooks.
        """
        return

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def flush_all(self, *, consumer_timeout_s: float = _FLUSH_ALL_CONSUMER_TIMEOUT_S) -> None:
        """Force-flush every pending user state.  Called on disconnect.

        Robust against mid-PTT WS death: aiozello pushes ``None`` into a
        stream's queue only when it dispatches ``on_stream_stop``; if the
        WS closes first, that dispatch never happens and any consumer
        blocked on ``decode()`` would hang forever.  We push ``None``
        into every open ``IncomingAudioStream.incoming`` queue ourselves,
        then bound the gather so a misbehaving consumer cannot wedge
        shutdown — surviving partial PCM still lands via ``_append`` when
        the consumer drains; truly-stuck consumers are cancelled.
        """
        # 1. Unblock all consumers — push the EOF sentinel into every open
        #    inbound stream's queue.  Safe to call even if the stream already
        #    received a real on_stream_stop (the queue just gets two Nones;
        #    decode() exits on the first).
        for stream in list(getattr(self._app, "streams", {}).values()):
            try:
                await stream.put(None)
            except Exception:
                logger.debug("zello: stream.put(None) during flush_all raised", exc_info=True)

        # 2. Wait for consumers to finish so per-user PCM is fully drained
        #    into the aggregator state, with a hard ceiling.
        pending = list(self._consumer_tasks)
        if pending:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*pending, return_exceptions=True),
                    timeout=consumer_timeout_s,
                )
            except asyncio.TimeoutError:
                stuck = [t for t in pending if not t.done()]
                logger.warning(
                    "zello: flush_all consumer timeout after %.1fs — cancelling %d stuck task(s)",
                    consumer_timeout_s,
                    len(stuck),
                )
                for t in stuck:
                    t.cancel()
                # One last gather to absorb the cancellations; bounded too.
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*stuck, return_exceptions=True),
                        timeout=0.5,
                    )
                except asyncio.TimeoutError:
                    logger.warning("zello: stuck consumers ignored their cancel()")

        # 3. Flush remaining per-user state.
        senders = list(self._states.keys())
        for sender in senders:
            await self._flush_now(sender, reason="shutdown")

    # ── Consumer / flush internals ────────────────────────────────────────

    async def _consume_stream(
        self,
        sender: str,
        stream,
        sample_rate_hz: int,
    ) -> None:
        """Drain one IncomingAudioStream into the per-user buffer."""
        local_pcm = bytearray()
        try:
            async for pcm_chunk in stream.decode():
                local_pcm.extend(pcm_chunk)
        except Exception:
            logger.exception(
                "zello: decode error draining stream from sender=%r (kept %d bytes)",
                sender,
                len(local_pcm),
            )
        if not local_pcm:
            logger.debug("zello: stream from sender=%r produced no PCM", sender)
            return

        await self._append(sender, bytes(local_pcm), sample_rate_hz)

    async def _drain_silently(self, stream_id) -> None:
        """For denied streams: drain the queue so it doesn't fill memory."""
        stream = self._app.streams.get(stream_id)
        if stream is None:
            return
        try:
            await stream.drain()
        except Exception:
            logger.debug("zello: drain of denied stream raised", exc_info=True)

    async def _append(self, sender: str, pcm: bytes, sample_rate_hz: int) -> None:
        st = self._states.get(sender)
        if st is None:
            st = _UserState(
                sender=sender,
                sample_rate_hz=sample_rate_hz,
                started_at=self._now(),
            )
            self._states[sender] = st
        elif st.sample_rate_hz != sample_rate_hz:
            # Sample-rate change mid-aggregation is an aiozello / codec-
            # header edge case; safest to flush the old and start fresh
            # rather than concatenate at the wrong rate.
            logger.warning(
                "zello: sample-rate change for sender=%r (%d → %d); "
                "flushing buffer before appending",
                sender,
                st.sample_rate_hz,
                sample_rate_hz,
            )
            await self._flush_now(sender, reason="sample-rate-change")
            st = _UserState(
                sender=sender,
                sample_rate_hz=sample_rate_hz,
                started_at=self._now(),
            )
            self._states[sender] = st

        st.pcm.extend(pcm)

        # Reset / arm flush timer
        if st.timer_handle is not None:
            st.timer_handle.cancel()
            st.timer_handle = None

        # Max-utterance ceiling check
        seconds_buffered = len(st.pcm) / 2 / max(1, st.sample_rate_hz)
        if seconds_buffered >= self._max_utterance_s:
            logger.info(
                "zello: sender=%r hit max_utterance_s=%.1fs (have %.1fs); flushing",
                sender,
                self._max_utterance_s,
                seconds_buffered,
            )
            await self._flush_now(sender, reason="max-utterance")
            return

        loop = self._get_loop()
        st.timer_handle = loop.call_later(
            self._window_s, self._spawn_flush, sender
        )

    def _spawn_flush(self, sender: str) -> None:
        """call_later callback shim — schedules the async flush coroutine."""
        self._spawn(self._flush_now(sender, reason="window"))

    async def _flush_now(self, sender: str, *, reason: str) -> None:
        st = self._states.pop(sender, None)
        if st is None:
            return
        if st.timer_handle is not None:
            st.timer_handle.cancel()
        if not st.pcm:
            return
        seconds = len(st.pcm) / 2 / max(1, st.sample_rate_hz)
        logger.info(
            "zello: flushing utterance from sender=%r (%.1fs, %d bytes, reason=%s)",
            sender,
            seconds,
            len(st.pcm),
            reason,
        )
        try:
            await self._flush_cb(sender, bytes(st.pcm), st.sample_rate_hz)
        except Exception:
            logger.exception(
                "zello: flush_cb raised for sender=%r (utterance dropped)",
                sender,
            )

    # ── Loop / time helpers (overridable in tests) ────────────────────────

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        return self._loop

    def _now(self) -> float:
        return self._get_loop().time()

    def _spawn(self, coro) -> asyncio.Task:
        loop = self._get_loop()
        return loop.create_task(coro)
