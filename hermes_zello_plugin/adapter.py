"""ZelloAdapter — Hermes platform adapter for the Zello Channel API.

Plugin entry point: :func:`register`.  Mirrors
``plugins/platforms/irc/adapter.py:927-969`` of hermes-agent.

Voice-only in v1.  Inbound: aiozello stream → :class:`UtteranceAggregator`
→ ffmpeg-wrapped Ogg-Opus → ``MessageEvent(message_type=VOICE)``.
Outbound: ``send_voice`` → :func:`stream_audio_to_zello`.

See ``HERMES-ZELLO-PLAN.md`` §4 for the design and ``DELTAS.md`` for the
field/API drift discovered against the live hermes-agent / aiozello tip.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
from typing import Any, Dict, Optional

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_audio_from_bytes,
    resolve_channel_prompt,
)
from gateway.config import Platform, PlatformConfig  # noqa: F401 - PlatformConfig for typing only
from gateway.session import SessionSource  # noqa: F401 - re-exported for completeness

# Heavy runtime deps (aiozello, opuslib) and modules that import them
# (.aggregator, .outbound) are NOT imported at module level so this
# module loads cleanly in processes that don't have those Python
# packages on PYTHONPATH — notably the hermes-dashboard service, which
# enumerates plugins to compute "is this platform connected" but never
# actually runs the adapter.  Lazy-imported inside the methods that
# need them.  See HERMES-ZELLO-PLAN.md follow-up audit.
from .config import load_config, missing_required, ZelloConfig
from .platform_hint import PLATFORM_HINT

logger = logging.getLogger(__name__)


_CONNECT_TIMEOUT_S = 30.0
_JWT_REFRESH_INTERVAL_S = 3000.0  # 50 min, inside the 60 min token TTL


# ── Inbound utterance → MessageEvent helper ──────────────────────────────


async def _pcm_to_ogg_opus(
    pcm: bytes,
    sample_rate_hz: int,
    *,
    ffmpeg_bin: Optional[str] = None,
) -> bytes:
    """Wrap PCM s16le mono *pcm* as Ogg-Opus via ``ffmpeg``.

    Used on the inbound side to package an aggregated utterance into a
    container Hermes' STT layer can read.  Subprocess in / out via pipes;
    no temp files.
    """
    from .outbound import FfmpegDecodeError  # lazy: outbound pulls opuslib

    ffmpeg = ffmpeg_bin or shutil.which("ffmpeg")
    if not ffmpeg:
        raise FfmpegDecodeError("ffmpeg binary not found on PATH")

    proc = await asyncio.create_subprocess_exec(
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-f",
        "s16le",
        "-ar",
        str(sample_rate_hz),
        "-ac",
        "1",
        "-i",
        "pipe:0",
        "-c:a",
        "libopus",
        "-b:a",
        "24k",
        "-f",
        "ogg",
        "pipe:1",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate(input=pcm)
    if proc.returncode != 0:
        raise FfmpegDecodeError(
            f"ffmpeg(ogg-opus) exited {proc.returncode}: "
            f"{stderr.decode('utf-8', 'replace').strip()}"
        )
    if not stdout:
        raise FfmpegDecodeError("ffmpeg(ogg-opus) produced empty output")
    return stdout


# ── Adapter ───────────────────────────────────────────────────────────────


class ZelloAdapter(BasePlatformAdapter):
    """Async Zello Channel API adapter — voice-only.

    Constructed by :func:`register`'s ``adapter_factory`` lambda; configured
    entirely from env vars (no YAML schema in v1).
    """

    def __init__(self, config, **_kwargs):
        super().__init__(config=config, platform=Platform("zello"))

        # Env-driven config (plan §6.2 — sops mounts the env file into the
        # container; PlatformConfig.extra stays empty in v1).
        self._zello_cfg: ZelloConfig = load_config()

        self._app: Optional[Application] = None
        self._aggregator: Optional[UtteranceAggregator] = None
        self._run_task: Optional[asyncio.Task] = None
        self._connected_event: asyncio.Event = asyncio.Event()

    @property
    def name(self) -> str:
        return "Zello"

    # ── Connection lifecycle ──────────────────────────────────────────────

    async def connect(self) -> bool:
        # Lazy imports so adapter.py loads in processes that don't have
        # aiozello / opuslib on PYTHONPATH (e.g. hermes-dashboard).
        from aiozello.__main__ import Application
        from aiozello.auth import LocalTokenManager
        from .aggregator import UtteranceAggregator

        cfg = self._zello_cfg

        if not os.path.exists(cfg.private_key_path):
            logger.error(
                "[%s] private key not found at %s", self.name, cfg.private_key_path
            )
            self._set_fatal_error(
                "config_missing",
                f"ZELLO_PRIVATE_KEY_PATH does not exist: {cfg.private_key_path}",
                retryable=False,
            )
            return False

        try:
            token_manager = LocalTokenManager(cfg.issuer, cfg.private_key_path)
        except Exception as e:
            logger.exception("[%s] LocalTokenManager init failed", self.name)
            self._set_fatal_error("auth_init_failed", str(e), retryable=False)
            return False

        self._app = Application(
            token=None,
            token_loader=token_manager.issue,
            token_refresh_interval_s=_JWT_REFRESH_INTERVAL_S,
            username=cfg.username,
            password=cfg.password,
            channels=[cfg.channel],
            callbacks={
                "on_channel_status": self._on_channel_status,
                "on_stream_start": self._on_stream_start,
                "on_stream_stop": self._on_stream_stop,
                "on_text_message": self._on_text_message,
                "on_ws_closed": self._on_ws_closed,
            },
        )

        self._aggregator = UtteranceAggregator(
            self._app,
            self._handle_utterance,
            allowed_users=cfg.allowed_users,
            allow_all=cfg.allow_all_users,
            window_s=cfg.aggregator_window_s,
            max_utterance_s=cfg.max_utterance_s,
        )

        self._connected_event.clear()
        self._run_task = asyncio.create_task(
            self._app.run(), name=f"zello-run-{cfg.username}"
        )

        try:
            await asyncio.wait_for(self._connected_event.wait(), timeout=_CONNECT_TIMEOUT_S)
        except asyncio.TimeoutError:
            logger.error(
                "[%s] did not receive channel-online ChannelStatus within %.1fs",
                self.name,
                _CONNECT_TIMEOUT_S,
            )
            self._set_fatal_error(
                "connect_timeout",
                f"No channel_status from Zello within {_CONNECT_TIMEOUT_S:.0f}s",
                retryable=True,
            )
            await self._cancel_run_task()
            return False

        self._mark_connected()
        logger.info(
            "[%s] connected — channel=%s username=%s allowed=%s",
            self.name,
            cfg.channel,
            cfg.username,
            "all" if cfg.allow_all_users else sorted(cfg.allowed_users),
        )
        return True

    async def disconnect(self) -> None:
        logger.info("[%s] disconnecting", self.name)
        if self._aggregator is not None:
            try:
                await self._aggregator.flush_all()
            except Exception:
                logger.exception("[%s] aggregator flush_all raised", self.name)
        if self._app is not None:
            try:
                await self._app.disconnect()
            except Exception:
                logger.exception("[%s] aiozello disconnect raised", self.name)
        await self._cancel_run_task()
        self._app = None
        self._aggregator = None
        self._mark_disconnected()

    async def _cancel_run_task(self) -> None:
        if self._run_task is None:
            return
        if not self._run_task.done():
            self._run_task.cancel()
            try:
                await self._run_task
            except (asyncio.CancelledError, Exception):
                pass
        self._run_task = None

    # ── aiozello callbacks ────────────────────────────────────────────────

    async def _on_channel_status(self, event: ChannelStatus) -> None:
        logger.info(
            "[%s] channel_status channel=%s status=%s users_online=%s",
            self.name,
            event.channel,
            event.status,
            event.users_online,
        )
        if event.error:
            logger.error(
                "[%s] channel_status error=%s error_type=%s",
                self.name,
                event.error,
                event.error_type,
            )
            return
        if event.status.lower() == "online":
            self._connected_event.set()

    async def _on_stream_start(self, event: StreamStart) -> None:
        if self._aggregator is not None:
            await self._aggregator.on_stream_start(event)

    async def _on_stream_stop(self, event: StreamStop) -> None:
        if self._aggregator is not None:
            await self._aggregator.on_stream_stop(event)

    async def _on_text_message(self, event: TextMessage) -> None:
        # v1 is voice-only; ack and discard per plan §7.
        logger.info(
            "[%s] discarding text message from sender=%s (voice-only in v1)",
            self.name,
            event.sender,
        )

    async def _on_ws_closed(self, _event) -> None:
        logger.info("[%s] websocket closed (aiozello will reconnect)", self.name)

    # ── Flush callback: PCM → Ogg-Opus → MessageEvent ─────────────────────

    async def _handle_utterance(
        self, sender: str, pcm: bytes, sample_rate_hz: int
    ) -> None:
        from .outbound import FfmpegDecodeError  # lazy: outbound pulls opuslib

        try:
            ogg_bytes = await _pcm_to_ogg_opus(pcm, sample_rate_hz)
        except FfmpegDecodeError as e:
            logger.error("[%s] failed to package utterance as Ogg-Opus: %s", self.name, e)
            return

        path = cache_audio_from_bytes(ogg_bytes, ext=".ogg")
        source = self.build_source(
            chat_id=self._zello_cfg.channel,
            chat_name=self._zello_cfg.channel,
            chat_type="channel",
            user_id=sender,
            user_name=sender,
        )
        # Per-channel ephemeral system prompt.  ``config.extra["channel_prompts"]``
        # is seeded from top-level ``zello.channel_prompts.<chat_id>:`` in
        # ``~/.hermes/config.yaml`` by :func:`_env_enablement` — required
        # on hermes ≤ ``faa13e49`` whose ``gateway/config.py`` bridging
        # loop iterates ``list(Platform)`` only (built-ins).  On newer
        # hermes versions the generic loop covers plugin platforms too;
        # the env-enablement seed is harmless there (same key/value).
        # resolve_channel_prompt returns None if nothing is configured,
        # leaving only the static PLATFORM_HINT in effect.
        channel_prompt = resolve_channel_prompt(
            self.config.extra, self._zello_cfg.channel
        )
        event = MessageEvent(
            text="",  # transcript will be filled in by hermes' STT pipeline
            message_type=MessageType.VOICE,
            source=source,
            media_urls=[path],
            media_types=["audio/ogg"],
            channel_prompt=channel_prompt,
        )
        logger.info(
            "[%s] dispatching VOICE MessageEvent sender=%s bytes=%d path=%s",
            self.name,
            sender,
            len(ogg_bytes),
            path,
        )
        await self.handle_message(event)

    # ── BasePlatformAdapter overrides ─────────────────────────────────────

    def _should_auto_tts_for_chat(self, chat_id: str) -> bool:
        """Plugin owns the TTS path via ``send()``; disable base auto-TTS.

        Returning False here prevents the base adapter's auto-TTS block
        (gateway/platforms/base.py:3607) from firing.  Without this, a
        future configuration change that successfully enables auto-TTS
        would deliver the same reply twice — once via play_tts and once
        via our send() (which now also synthesizes).  Belt-and-braces
        even though the current deployments don't trigger the base path.
        """
        return False

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Synthesize text via hermes' configured TTS and stream as opus.

        Zello has no native text delivery.  Earlier versions of the plugin
        no-op'd send() and relied on the base adapter's auto-TTS path to
        intercept text replies before they reached us — but that path
        depends on per-chat voice-mode state that turned out fragile to
        bring up declaratively (the _sync_voice_mode_state_to_adapter
        runner method clears _auto_tts_enabled_chats every reconnect,
        and load_config() inside its try-block silently returns False
        in some environments leaving _auto_tts_default at False too).

        Instead, own the path here: call hermes' text_to_speech_tool
        directly (honors settings.tts.provider / settings.tts.providers.*
        from config.yaml — ElevenLabs Flash v2.5 in our deployment),
        then deliver the produced file via send_voice.  No dependency on
        voice-mode state, no double-delivery risk (auto-TTS is force-
        disabled by _should_auto_tts_for_chat above).
        """
        # Inline markdown-strip + length-cap.  Mirrors the
        # base-adapter helper (gateway/platforms/base.py:2264 in
        # upstream main) but kept local because our pinned hermes
        # commit predates the helper landing on BasePlatformAdapter.
        import re as _re
        speech_text = _re.sub(r'[*_`#\[\]()]', '', content or '')[:4000].strip()
        if not speech_text:
            logger.info(
                "[%s] send: empty text after markdown cleanup; nothing to speak",
                self.name,
            )
            return SendResult(success=True, message_id="zello-noop")

        try:
            from tools.tts_tool import text_to_speech_tool, check_tts_requirements
        except ImportError as e:
            logger.error("[%s] send: tts_tool import failed: %s", self.name, e)
            return SendResult(success=False, error=f"tts_tool unavailable: {e}", retryable=False)

        if not check_tts_requirements():
            logger.error(
                "[%s] send: TTS not configured (settings.tts.provider missing or API key absent)",
                self.name,
            )
            return SendResult(
                success=False,
                error="TTS not configured; set settings.tts.provider + provider API key",
                retryable=False,
            )

        import json as _json
        try:
            result_str = await asyncio.to_thread(text_to_speech_tool, text=speech_text)
            tts_data = _json.loads(result_str)
        except Exception as e:
            logger.exception("[%s] send: TTS synthesis failed", self.name)
            return SendResult(success=False, error=f"TTS error: {e}", retryable=True)

        audio_path = tts_data.get("file_path")
        if not audio_path or not os.path.exists(audio_path):
            err = tts_data.get("error") or "TTS produced no file"
            logger.error("[%s] send: %s (result=%r)", self.name, err, tts_data)
            return SendResult(success=False, error=str(err), retryable=False)

        try:
            return await self.send_voice(
                chat_id=chat_id, audio_path=audio_path, metadata=metadata
            )
        finally:
            try:
                os.remove(audio_path)
            except OSError:
                pass

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        logger.info(
            "[%s] send_image NO-OP (Zello v1 is voice-only) chat_id=%s url=%s",
            self.name,
            chat_id,
            image_url,
        )
        return SendResult(success=False, error="Zello v1 is voice-only")

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        # One chat — the dedicated channel.  chat_id is the channel name.
        return {
            "name": self._zello_cfg.channel,
            "type": "channel",
            "chat_id": self._zello_cfg.channel,
        }

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **_kwargs,
    ) -> SendResult:
        from .outbound import stream_audio_to_zello, FfmpegDecodeError  # lazy: opuslib

        if self._app is None:
            return SendResult(success=False, error="Zello adapter not connected")
        try:
            packets = await stream_audio_to_zello(self._app, audio_path)
        except FfmpegDecodeError as e:
            logger.error("[%s] send_voice ffmpeg decode failed: %s", self.name, e)
            return SendResult(success=False, error=f"ffmpeg decode: {e}", retryable=False)
        except Exception as e:
            logger.exception("[%s] send_voice failed", self.name)
            return SendResult(success=False, error=str(e), retryable=True)
        return SendResult(success=True, message_id=f"zello-{packets}p")

    async def play_tts(
        self,
        chat_id: str,
        audio_path: str,
        **kwargs,
    ) -> SendResult:
        return await self.send_voice(chat_id=chat_id, audio_path=audio_path, **kwargs)


# ── Registry hooks (plan §4.4) ────────────────────────────────────────────


def check_requirements() -> bool:
    """Verify lightweight deps + required env vars are present.

    ``check_fn`` is the dashboard's "is this platform configurable on this
    host?" gate (see ``hermes_cli/web_server.py::status_payload`` →
    ``GatewayConfig.get_connected_platforms``).  Heavy runtime deps
    (``aiozello``, ``opuslib``, ``ffmpeg``) are NOT checked here —
    they're required only when the adapter actually runs, which happens
    in the gateway process where they're available; gating dashboard
    visibility on those would silently hide a correctly-configured
    plugin in deployments where the dashboard service has a slimmer
    PYTHONPATH than the gateway service.  Mirrors the Mattermost
    plugin's lightweight ``check_mattermost_requirements`` (just
    ``import aiohttp``).  Adapter ``connect()`` surfaces missing heavy
    deps via ``_set_fatal_error``.
    """
    try:
        import jwt  # noqa: F401
    except ImportError as e:
        logger.warning("zello: pyjwt import failed: %s", e)
        return False
    return not missing_required()


def validate_config(_config) -> bool:
    """Validate that the required env vars are non-blank."""
    return not missing_required()


def is_connected(_config) -> bool:
    """Treated as 'configured' here — same heuristic IRC uses."""
    return not missing_required()


def _env_enablement() -> Optional[dict]:
    """Seed PlatformConfig.extra from env vars + config.yaml.

    Mirrors IRC's hook (``plugins/platforms/irc/adapter.py:651``).  Two
    responsibilities:

    1. Seed ``channel`` and ``home_channel`` from env vars so the
       plugin shows up in ``hermes gateway status`` without instantiating
       the adapter.
    2. **Seed ``channel_prompts`` from ``~/.hermes/config.yaml``.**
       On hermes-agent ≤ ``faa13e49`` the shared YAML→extra bridging
       loop in ``gateway/config.py`` iterates ``list(Platform)`` only
       (built-ins), so plugin platforms never get their
       ``channel_prompts`` key bridged into ``PlatformConfig.extra``.
       Without this seed, ``resolve_channel_prompt(self.config.extra,
       chat_id)`` in the adapter returns ``None`` and per-channel
       ephemeral prompts silently never reach the agent.  On newer
       hermes versions (after the shared loop learned to enumerate
       plugin entries) the same key is populated by the generic loop
       too; ``extra.update(seed)`` preserves it either way.
    """
    channel = os.getenv("ZELLO_CHANNEL", "").strip()
    if not channel:
        return None
    seed: dict[str, Any] = {"channel": channel}

    # Read channel_prompts from config.yaml (production hermes
    # workaround — see docstring).
    try:
        import yaml  # transitive hermes dep
        from pathlib import Path

        home_str = os.getenv("HERMES_HOME", "").strip()
        hermes_home = Path(home_str) if home_str else (Path.home() / ".hermes")
        cfg_path = hermes_home / "config.yaml"
        if cfg_path.exists():
            with cfg_path.open(encoding="utf-8") as f:
                ycfg = yaml.safe_load(f) or {}
            zello_block = ycfg.get("zello") or {}
            cp = zello_block.get("channel_prompts")
            if isinstance(cp, dict):
                seed["channel_prompts"] = {str(k): v for k, v in cp.items()}
    except Exception as e:  # noqa: BLE001
        logger.debug("zello: channel_prompts seed from config.yaml skipped: %s", e)

    home_channel = os.getenv("ZELLO_HOME_CHANNEL", "").strip() or channel
    seed["home_channel"] = {
        "platform": "zello",
        "chat_id": home_channel,
        "name": home_channel,
    }
    return seed


def _apply_yaml_config(
    yaml_cfg: dict, zello_cfg: dict
) -> Optional[dict]:
    """Translate ``config.yaml`` ``zello:`` keys into ``ZELLO_*`` env vars.

    Mirrors ``plugins/platforms/mattermost/adapter.py::_apply_yaml_config``
    (PlatformEntry hook landed in hermes #24836 / #25443).  Older hermes
    versions (e.g. the user's deployed ``faa13e49``) don't have
    ``apply_yaml_config_fn`` on ``PlatformEntry``; the compat shim in
    :func:`register` drops the kwarg silently in that case — this hook
    activates automatically once hermes-agent is upgraded.

    Env vars take precedence over YAML — every assignment is guarded by
    ``not os.getenv(...)``.  Returns ``None`` because everything flows
    through env vars; nothing extra is seeded into ``PlatformConfig.extra``
    directly (``channel_prompts`` continues to flow via
    :func:`_env_enablement` so the production fallback path stays
    intact).
    """
    # allowed_users: comma-separated string or list of strings
    au = zello_cfg.get("allowed_users")
    if au is not None and not os.getenv("ZELLO_ALLOWED_USERS"):
        if isinstance(au, list):
            au = ",".join(str(v) for v in au)
        os.environ["ZELLO_ALLOWED_USERS"] = str(au)

    if "allow_all_users" in zello_cfg and not os.getenv("ZELLO_ALLOW_ALL_USERS"):
        os.environ["ZELLO_ALLOW_ALL_USERS"] = str(
            zello_cfg["allow_all_users"]
        ).lower()

    if "aggregator_window_s" in zello_cfg and not os.getenv("ZELLO_AGGREGATOR_WINDOW_S"):
        os.environ["ZELLO_AGGREGATOR_WINDOW_S"] = str(
            zello_cfg["aggregator_window_s"]
        )

    if "max_utterance_s" in zello_cfg and not os.getenv("ZELLO_MAX_UTTERANCE_S"):
        os.environ["ZELLO_MAX_UTTERANCE_S"] = str(zello_cfg["max_utterance_s"])

    if "home_channel" in zello_cfg and not os.getenv("ZELLO_HOME_CHANNEL"):
        os.environ["ZELLO_HOME_CHANNEL"] = str(zello_cfg["home_channel"])

    return None  # all settings flow through env; channel_prompts flows via env_enablement


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[list] = None,
    force_document: bool = False,
) -> dict:
    """Out-of-process delivery for cron jobs that run outside the gateway.

    v1: voice-only.  Cron text bodies are dropped with a warning; if
    ``media_files`` contains an audio path, open a one-shot aiozello
    session and stream it.

    This path is rarely exercised — most cron deliveries route through
    the live adapter — but the hook must exist for
    ``deliver=zello`` to be a valid cronjob target.
    """
    from aiozello.__main__ import Application  # lazy: aiozello
    from aiozello.auth import LocalTokenManager
    from aiozello.protocol import ChannelStatus
    from .outbound import stream_audio_to_zello  # lazy: opuslib

    if not media_files:
        return {"error": "zello standalone send: v1 is voice-only; no media_files"}

    audio_path = next((p for p in media_files if str(p).lower().endswith((".ogg", ".mp3", ".wav", ".opus", ".m4a"))), None)
    if audio_path is None:
        return {"error": "zello standalone send: no audio file in media_files"}

    try:
        cfg = load_config()
    except ValueError as e:
        return {"error": f"zello standalone send: {e}"}

    if not os.path.exists(cfg.private_key_path):
        return {"error": f"zello standalone send: private key missing at {cfg.private_key_path}"}

    connected = asyncio.Event()

    async def _on_status(event) -> None:
        if event.error:
            return
        if event.status.lower() == "online":
            connected.set()

    token_manager = LocalTokenManager(cfg.issuer, cfg.private_key_path)
    app = Application(
        token=None,
        token_loader=token_manager.issue,
        token_refresh_interval_s=0,  # one-shot; no refresh loop
        username=cfg.username,
        password=cfg.password,
        channels=[cfg.channel],
        callbacks={"on_channel_status": _on_status},
    )

    run_task = asyncio.create_task(app.run(), name="zello-standalone-run")
    try:
        try:
            await asyncio.wait_for(connected.wait(), timeout=_CONNECT_TIMEOUT_S)
        except asyncio.TimeoutError:
            return {"error": "zello standalone send: connect timeout"}

        packets = await stream_audio_to_zello(app, audio_path)
        return {"success": True, "message_id": f"zello-standalone-{packets}p"}
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


def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin loader.

    Hermes' ``ctx.register_platform(...)`` forwards ``**entry_kwargs`` straight
    into ``PlatformEntry(...)`` (see ``hermes_cli/plugins.py:736``), so any
    kwarg not declared on that dataclass raises ``TypeError`` at registration
    time.  Older hermes-agent pins miss newer fields (e.g.
    ``standalone_sender_fn`` was added after ``faa13e49``); rather than
    couple the plugin to a specific hermes commit, we introspect
    ``PlatformEntry``'s signature and drop kwargs the local install doesn't
    accept.  The skipped kwarg is logged once for visibility.
    """
    import inspect

    kwargs: Dict[str, Any] = dict(
        name="zello",
        label="Zello",
        adapter_factory=lambda cfg: ZelloAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=[
            "ZELLO_ISSUER",
            "ZELLO_PRIVATE_KEY_PATH",
            "ZELLO_USERNAME",
            "ZELLO_PASSWORD",
            "ZELLO_CHANNEL",
        ],
        install_hint=(
            "uv sync (provides aiozello, opuslib, pyjwt); "
            "ensure ffmpeg + libopus are on PATH/LD_LIBRARY_PATH"
        ),
        setup_fn=None,  # no interactive wizard in v1
        env_enablement_fn=_env_enablement,
        # YAML→env bridge.  Newer hermes (#24836) consumes this hook
        # automatically; older hermes lacks the field and the compat
        # filter below drops it silently.
        apply_yaml_config_fn=_apply_yaml_config,
        cron_deliver_env_var="ZELLO_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="ZELLO_ALLOWED_USERS",
        allow_all_env="ZELLO_ALLOW_ALL_USERS",
        max_message_length=0,  # voice-only; no text size relevant
        emoji="📻",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=PLATFORM_HINT,
    )

    try:
        from gateway.platform_registry import PlatformEntry
        accepted = set(inspect.signature(PlatformEntry).parameters)
    except Exception:  # noqa: BLE001
        # Couldn't introspect — pass through unchanged and let
        # register_platform raise if there's a mismatch.
        accepted = set(kwargs)

    dropped = sorted(k for k in kwargs if k not in accepted)
    filtered = {k: v for k, v in kwargs.items() if k in accepted}
    if dropped:
        logger.info(
            "zello: skipping unsupported PlatformEntry kwargs on this "
            "hermes version: %s",
            dropped,
        )
    ctx.register_platform(**filtered)

    from .end_session_tool import (
        END_SESSION_NAME,
        END_SESSION_SCHEMA,
        END_SESSION_TOOLSET,
        end_session_handler,
    )

    ctx.register_tool(
        name=END_SESSION_NAME,
        toolset=END_SESSION_TOOLSET,
        schema=END_SESSION_SCHEMA,
        handler=end_session_handler,
        is_async=True,
        description="End the current Zello voice session (full /new parity).",
        emoji="🛑",
    )
