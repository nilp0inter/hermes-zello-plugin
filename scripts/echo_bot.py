#!/usr/bin/env python3
"""End-to-end echo bot — proves the plugin's full inbound + outbound
pipeline without requiring hermes-agent.

Pipeline per PTT::

   phone PTT → UtteranceAggregator → PCM
       → ffmpeg → Ogg-Opus → Groq Whisper (transcribe)
       → ElevenLabs Flash v2.5 (synthesize transcript)
       → mp3 → stream_audio_to_zello (outbound)
       → audible echo back on the channel

Env (in addition to the standard ZELLO_* set)::

   GROQ_API_KEY=...           # https://console.groq.com
   ELEVENLABS_API_KEY=...     # https://elevenlabs.io
   ELEVENLABS_VOICE_ID=...    # optional — defaults to Rachel
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import tempfile
from pathlib import Path


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        s = raw.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        k, v = k.strip(), v.strip()
        if len(v) >= 2 and v[0] == v[-1] and v[0] in {"'", '"'}:
            v = v[1:-1]
        if k and k not in os.environ:
            os.environ[k] = v


# Default Groq Orpheus TTS voice (playai-tts deprecated 2025-12-31).
# Voices: troy, hannah, austin (English-only).
_DEFAULT_VOICE = "troy"
_TTS_MODEL = "canopylabs/orpheus-v1-english"


async def transcribe_groq(ogg_bytes: bytes, api_key: str) -> str:
    """Submit Ogg-Opus to Groq Whisper for transcription."""
    import aiohttp

    url = "https://api.groq.com/openai/v1/audio/transcriptions"
    form = aiohttp.FormData()
    form.add_field("file", ogg_bytes, filename="utterance.ogg", content_type="audio/ogg")
    form.add_field("model", "whisper-large-v3-turbo")
    form.add_field("response_format", "json")
    async with aiohttp.ClientSession() as s:
        async with s.post(url, headers={"Authorization": f"Bearer {api_key}"}, data=form, timeout=aiohttp.ClientTimeout(total=60)) as r:
            body = await r.json()
            if r.status != 200:
                raise RuntimeError(f"Groq STT {r.status}: {body}")
            return (body.get("text") or "").strip()


async def synthesize_groq(text: str, api_key: str, voice: str, out_path: Path) -> None:
    """Synthesize *text* to WAV at *out_path* via Groq PlayAI TTS."""
    import aiohttp

    url = "https://api.groq.com/openai/v1/audio/speech"
    payload = {
        "model": _TTS_MODEL,
        "voice": voice,
        "input": text,
        "response_format": "wav",
    }
    async with aiohttp.ClientSession() as s:
        async with s.post(url, headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}, json=payload, timeout=aiohttp.ClientTimeout(total=60)) as r:
            if r.status != 200:
                body = await r.text()
                raise RuntimeError(f"Groq TTS {r.status}: {body[:200]}")
            data = await r.read()
            out_path.write_bytes(data)


async def main(speak_prefix: str) -> int:
    _load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    # Local imports so dotenv-loaded env is in place
    from aiozello.__main__ import Application
    from aiozello.auth import LocalTokenManager
    from aiozello.protocol import ChannelStatus

    from hermes_zello_plugin.aggregator import UtteranceAggregator
    from hermes_zello_plugin.outbound import stream_audio_to_zello

    log = logging.getLogger("echo_bot")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    # Quiet aiozello's noisy callback debug
    logging.getLogger("aiozello.__main__").setLevel(logging.INFO)

    # ── Config ──────────────────────────────────────────────────────────
    cfg = {
        "issuer": os.environ.get("ZELLO_ISSUER", "").strip(),
        "pk_path": os.environ.get("ZELLO_PRIVATE_KEY_PATH", "./private.key").strip(),
        "username": os.environ.get("ZELLO_USERNAME", "").strip(),
        "password": os.environ.get("ZELLO_PASSWORD", ""),
        "channel": os.environ.get("ZELLO_CHANNEL", "").strip(),
        "allowed": frozenset(
            u.strip() for u in os.environ.get("ZELLO_ALLOWED_USERS", "").split(",") if u.strip()
        ),
        "allow_all": os.environ.get("ZELLO_ALLOW_ALL_USERS", "").lower() in {"1", "true", "yes"},
        "window_s": float(os.environ.get("ZELLO_AGGREGATOR_WINDOW_S", "2") or "2"),
        "max_utt_s": float(os.environ.get("ZELLO_MAX_UTTERANCE_S", "300") or "300"),
        "groq_key": os.environ.get("GROQ_API_KEY", "").strip(),
        "tts_voice": os.environ.get("GROQ_TTS_VOICE", _DEFAULT_VOICE).strip(),
        "tts_speed": float(os.environ.get("TTS_SPEED", "1.3") or "1.3"),
    }

    missing = [k for k, v in {
        "ZELLO_ISSUER": cfg["issuer"],
        "ZELLO_USERNAME": cfg["username"],
        "ZELLO_PASSWORD": cfg["password"],
        "ZELLO_CHANNEL": cfg["channel"],
        "GROQ_API_KEY": cfg["groq_key"],
    }.items() if not v]
    if missing:
        log.error("missing env: %s", ", ".join(missing))
        return 2
    if not Path(cfg["pk_path"]).exists():
        log.error("private key not found at %s", cfg["pk_path"])
        return 2

    # ── aiozello Application ───────────────────────────────────────────
    ltm = LocalTokenManager(cfg["issuer"], cfg["pk_path"])
    app = Application(
        token=None,
        token_loader=ltm.issue,
        token_refresh_interval_s=3000.0,
        username=cfg["username"],
        password=cfg["password"],
        channels=[cfg["channel"]],
        callbacks={},  # populated below before run()
    )

    connected = asyncio.Event()

    async def on_channel_status(event: ChannelStatus) -> None:
        log.info("channel_status status=%s users_online=%s error=%s",
                 event.status, event.users_online, event.error)
        if not event.error and event.status.lower() == "online":
            connected.set()

    # ── Echo flush callback ────────────────────────────────────────────
    async def on_utterance(sender: str, pcm: bytes, sample_rate_hz: int) -> None:
        seconds = len(pcm) / 2 / max(1, sample_rate_hz)
        log.info("utterance from %s (%.2fs, %d bytes)", sender, seconds, len(pcm))

        # PCM → Ogg-Opus
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin",
            "-f", "s16le", "-ar", str(sample_rate_hz), "-ac", "1", "-i", "pipe:0",
            "-c:a", "libopus", "-b:a", "24k", "-f", "ogg", "pipe:1",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        ogg_bytes, stderr = await proc.communicate(input=pcm)
        if proc.returncode != 0:
            log.error("ffmpeg PCM→ogg failed: %s", stderr.decode("utf-8", "replace"))
            return

        # STT
        try:
            transcript = await transcribe_groq(ogg_bytes, cfg["groq_key"])
        except Exception as e:
            log.exception("Groq STT failed: %s", e)
            return
        if not transcript:
            log.info("STT returned empty transcript; nothing to echo")
            return
        log.info("transcript: %r", transcript)

        # TTS
        reply_text = f"{speak_prefix} {transcript}".strip()
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
            tts_path = Path(f.name)
        try:
            try:
                await synthesize_groq(reply_text, cfg["groq_key"], cfg["tts_voice"], tts_path)
            except Exception as e:
                log.exception("Groq TTS failed: %s", e)
                return
            log.info("TTS synthesized %d bytes at %s", tts_path.stat().st_size, tts_path)

            # Speed up via ffmpeg atempo (preserves pitch).
            speed = cfg["tts_speed"]
            sped_path = tts_path
            if abs(speed - 1.0) > 0.01:
                with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as sf:
                    sped_path = Path(sf.name)
                proc = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
                    "-i", str(tts_path), "-filter:a", f"atempo={speed}",
                    "-f", "wav", str(sped_path),
                )
                if await proc.wait() != 0:
                    log.warning("atempo speedup failed; streaming original")
                    sped_path = tts_path
                else:
                    log.info("sped up TTS by %.2fx → %s", speed, sped_path)

            # Outbound stream
            try:
                packets = await stream_audio_to_zello(app, sped_path)
                log.info("OK — streamed %d opus packets", packets)
            except Exception as e:
                log.exception("outbound stream failed: %s", e)
        finally:
            for p in {tts_path, sped_path}:
                try:
                    p.unlink()
                except FileNotFoundError:
                    pass

    aggregator = UtteranceAggregator(
        app, on_utterance,
        allowed_users=cfg["allowed"],
        allow_all=cfg["allow_all"],
        window_s=cfg["window_s"],
        max_utterance_s=cfg["max_utt_s"],
    )

    # ── Wire callbacks (must match KNOWN_CALLBACKS exactly) ────────────
    app.callbacks = app.callbacks  # placeholder
    # aiozello's Application.__init__ has already run fix_callbacks on whatever
    # we passed in callbacks=.  To wire ours, we reach into the dict directly
    # using the SAME decoration aiozello applies (log_callback wrap).  Simpler:
    # construct a fresh Application now that we have aggregator.
    from aiozello.__main__ import fix_callbacks
    app.callbacks = fix_callbacks({
        "on_channel_status": on_channel_status,
        "on_stream_start": aggregator.on_stream_start,
        "on_stream_stop": aggregator.on_stream_stop,
    })

    log.info("connecting (channel=%s username=%s, allowed=%s)",
             cfg["channel"], cfg["username"], sorted(cfg["allowed"]) if not cfg["allow_all"] else "ALL")

    stop = asyncio.Event()

    def _on_sigint(*_args):
        log.info("SIGINT — stopping")
        stop.set()

    asyncio.get_event_loop().add_signal_handler(signal.SIGINT, _on_sigint)
    asyncio.get_event_loop().add_signal_handler(signal.SIGTERM, _on_sigint)

    run_task = asyncio.create_task(app.run(), name="zello-echo-run")
    try:
        try:
            await asyncio.wait_for(connected.wait(), timeout=20.0)
        except asyncio.TimeoutError:
            log.error("connect timeout")
            return 1
        log.info("READY — PTT into channel '%s' from your phone (allowed: %s)",
                 cfg["channel"], sorted(cfg["allowed"]))
        log.info("Ctrl-C to stop.")
        await stop.wait()
        return 0
    finally:
        log.info("disconnecting")
        try:
            await aggregator.flush_all()
        except Exception:
            log.exception("aggregator flush_all raised")
        try:
            await app.disconnect()
        except Exception:
            log.exception("aiozello disconnect raised")
        if not run_task.done():
            run_task.cancel()
            try:
                await run_task
            except (asyncio.CancelledError, Exception):
                pass


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", default="You said:",
                    help="String prepended to the transcript before TTS")
    args = ap.parse_args()
    sys.exit(asyncio.run(main(args.prefix)))
