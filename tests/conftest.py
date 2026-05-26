"""Test fixtures + ``gateway.*`` stubs.

``hermes_zello_plugin.adapter`` imports from ``gateway.platforms.base``,
``gateway.config``, and ``gateway.session`` (all owned by hermes-agent,
not declared as a dependency of this plugin).  We register minimal stub
modules in ``sys.modules`` *before* any plugin module is imported, so the
plugin tree is importable in CI without a hermes checkout.

The stubs implement just enough surface for ``adapter.py`` to import and
for the smoke-test in ``test_smoke.py`` to exercise registration.  They
are intentionally NOT a behavioural compatibility shim — adapter
behaviour is tested via the module-level helpers (config, aggregator,
outbound) which do not touch hermes APIs.
"""

from __future__ import annotations

import sys
import types
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


# ── Stub: gateway.config ──────────────────────────────────────────────────

_gw_config = types.ModuleType("gateway.config")


class _Platform:
    """Minimal Platform stand-in.

    Supports ``Platform("zello")`` returning a singleton-ish object whose
    ``.value`` is the supplied string, so adapter code that does
    ``self.platform.value`` works.
    """

    _cache: dict[str, "_Platform"] = {}

    def __init__(self, value: str):
        self.value = value

    def __new__(cls, value: str):  # type: ignore[override]
        cached = cls._cache.get(value)
        if cached is not None:
            return cached
        instance = super().__new__(cls)
        cls._cache[value] = instance
        return instance

    def __repr__(self) -> str:
        return f"Platform({self.value!r})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Platform) and other.value == self.value

    def __hash__(self) -> int:
        return hash(self.value)


@dataclass
class _PlatformConfig:
    enabled: bool = True
    token: Optional[str] = None
    extra: dict = field(default_factory=dict)


_gw_config.Platform = _Platform
_gw_config.PlatformConfig = _PlatformConfig


# ── Stub: gateway.session ─────────────────────────────────────────────────

_gw_session = types.ModuleType("gateway.session")


@dataclass
class _SessionSource:
    platform: Any
    chat_id: str
    chat_name: Optional[str] = None
    chat_type: str = "dm"
    user_id: Optional[str] = None
    user_name: Optional[str] = None
    thread_id: Optional[str] = None
    chat_topic: Optional[str] = None
    user_id_alt: Optional[str] = None
    chat_id_alt: Optional[str] = None
    is_bot: bool = False
    guild_id: Optional[str] = None
    parent_chat_id: Optional[str] = None
    message_id: Optional[str] = None


_gw_session.SessionSource = _SessionSource


# ── Stub: gateway.platforms.base ──────────────────────────────────────────

_gw_platforms = types.ModuleType("gateway.platforms")
_gw_platforms_base = types.ModuleType("gateway.platforms.base")


class _MessageType(Enum):
    TEXT = "text"
    VOICE = "voice"
    PHOTO = "photo"


@dataclass
class _MessageEvent:
    text: str
    message_type: _MessageType = _MessageType.TEXT
    source: Any = None
    media_urls: list = field(default_factory=list)
    media_types: list = field(default_factory=list)
    raw_message: Any = None
    message_id: Optional[str] = None


@dataclass
class _SendResult:
    success: bool
    message_id: Optional[str] = None
    error: Optional[str] = None
    raw_response: Any = None
    retryable: bool = False


_CACHE_CALLS: list[tuple[bytes, str]] = []


def _cache_audio_from_bytes(data: bytes, ext: str = ".ogg") -> str:
    """Stub: record the call and return a fake path."""
    _CACHE_CALLS.append((data, ext))
    return f"/tmp/test-cache-{len(_CACHE_CALLS)}{ext}"


class _BasePlatformAdapter:
    """Stub base class — provides just the surface adapter.py touches.

    Not an ABC; tests instantiate the real adapter directly.
    """

    def __init__(self, config, platform):
        self.config = config
        self.platform = platform
        self._running = False
        self._fatal_error_code: Optional[str] = None
        self._fatal_error_message: Optional[str] = None
        self._fatal_error_retryable: bool = True
        self.handle_message_calls: list = []

    def build_source(
        self,
        chat_id: str,
        chat_name: Optional[str] = None,
        chat_type: str = "dm",
        user_id: Optional[str] = None,
        user_name: Optional[str] = None,
        **kwargs,
    ):
        return _SessionSource(
            platform=self.platform,
            chat_id=chat_id,
            chat_name=chat_name,
            chat_type=chat_type,
            user_id=user_id,
            user_name=user_name,
        )

    def _set_fatal_error(self, code: str, message: str, *, retryable: bool) -> None:
        self._running = False
        self._fatal_error_code = code
        self._fatal_error_message = message
        self._fatal_error_retryable = retryable

    def _mark_connected(self) -> None:
        self._running = True

    def _mark_disconnected(self) -> None:
        self._running = False

    async def handle_message(self, event) -> None:
        self.handle_message_calls.append(event)


_gw_platforms_base.BasePlatformAdapter = _BasePlatformAdapter
_gw_platforms_base.MessageEvent = _MessageEvent
_gw_platforms_base.MessageType = _MessageType
_gw_platforms_base.SendResult = _SendResult
_gw_platforms_base.cache_audio_from_bytes = _cache_audio_from_bytes


# ── Install stubs ────────────────────────────────────────────────────────

_gw_root = types.ModuleType("gateway")
sys.modules.setdefault("gateway", _gw_root)
sys.modules.setdefault("gateway.config", _gw_config)
sys.modules.setdefault("gateway.session", _gw_session)
sys.modules.setdefault("gateway.platforms", _gw_platforms)
sys.modules.setdefault("gateway.platforms.base", _gw_platforms_base)
