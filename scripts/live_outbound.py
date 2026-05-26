#!/usr/bin/env python3
"""Live outbound PTT smoke test — sends a short tone to the Zello channel.

Exercises the full outbound pipeline:
ffmpeg-generated test tone → ``stream_audio_to_zello`` →
``opuslib`` encode → ``app.outbound_stream(...)`` → realtime pacing →
audible 0.5s beep on the channel.

YES, this WILL beep on the channel.  Use with discretion.

Run from the repo root inside ``nix develop``::

    uv run python scripts/live_outbound.py [--duration 0.5] [--freq 440]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import tempfile
from pathlib import Path


async def _generate_tone(path: Path, duration_s: float, freq_hz: float) -> None:
    """Synthesize a sine-wave WAV via ffmpeg."""
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"sine=frequency={freq_hz}:duration={duration_s}:sample_rate=16000",
        "-ac",
        "1",
        "-ar",
        "16000",
        "-c:a",
        "pcm_s16le",
        str(path),
    )
    rc = await proc.wait()
    if rc != 0:
        raise RuntimeError(f"ffmpeg tone generation exited {rc}")


async def main(duration_s: float, freq_hz: float, connect_timeout_s: float) -> int:
    # Reuse the dotenv loader from live_smoke.py
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from live_smoke import _load_dotenv  # type: ignore[import-not-found]

    _load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    from aiozello.__main__ import Application
    from aiozello.auth import LocalTokenManager
    from aiozello.protocol import ChannelStatus

    issuer = os.environ.get("ZELLO_ISSUER", "").strip()
    pk_path = os.environ.get("ZELLO_PRIVATE_KEY_PATH", "./private.key").strip()
    username = os.environ.get("ZELLO_USERNAME", "").strip()
    password = os.environ.get("ZELLO_PASSWORD", "")
    channel = os.environ.get("ZELLO_CHANNEL", "").strip()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    log = logging.getLogger("live_outbound")

    if not (issuer and username and password and channel and Path(pk_path).exists()):
        print("FAIL: env vars or private.key missing", file=sys.stderr)
        return 2

    # 1. Generate test tone
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
        tone_path = Path(f.name)
    try:
        await _generate_tone(tone_path, duration_s, freq_hz)
        log.info("generated %.2fs %.0fHz tone at %s (%d bytes)",
                 duration_s, freq_hz, tone_path, tone_path.stat().st_size)

        # 2. Connect
        connected = asyncio.Event()

        async def on_status(event: ChannelStatus) -> None:
            log.info("channel_status status=%s users_online=%s error=%s",
                     event.status, event.users_online, event.error)
            if not event.error and event.status.lower() == "online":
                connected.set()

        ltm = LocalTokenManager(issuer, pk_path)
        app = Application(
            token=None,
            token_loader=ltm.issue,
            token_refresh_interval_s=3000.0,
            username=username,
            password=password,
            channels=[channel],
            callbacks={"on_channel_status": on_status},
        )

        run_task = asyncio.create_task(app.run(), name="zello-live-outbound")
        try:
            try:
                await asyncio.wait_for(connected.wait(), timeout=connect_timeout_s)
            except asyncio.TimeoutError:
                log.error("FAIL — no channel-online within %.1fs", connect_timeout_s)
                return 1

            # 3. Stream the tone
            from hermes_zello_plugin.outbound import stream_audio_to_zello

            log.info("streaming tone to channel=%s", channel)
            packets = await stream_audio_to_zello(app, tone_path)
            log.info("OK — sent %d opus packets", packets)
            # Brief pause so the listener side has time to drain before we close.
            await asyncio.sleep(0.5)
            return 0
        finally:
            try:
                await app.disconnect()
            except Exception:
                pass
            if not run_task.done():
                run_task.cancel()
                try:
                    await run_task
                except (asyncio.CancelledError, Exception):
                    pass
    finally:
        try:
            tone_path.unlink()
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=0.5, help="tone duration seconds")
    ap.add_argument("--freq", type=float, default=440.0, help="tone frequency Hz")
    ap.add_argument("--connect-timeout", type=float, default=20.0)
    args = ap.parse_args()
    sys.exit(asyncio.run(main(args.duration, args.freq, args.connect_timeout)))
