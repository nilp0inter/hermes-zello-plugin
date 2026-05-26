"""Env-var parsing for the Zello platform plugin.

Pure module: no I/O beyond reading from a supplied ``Mapping`` (defaults to
``os.environ``).  Used both at plugin-load (validation hooks) and at adapter
construction (runtime config).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Mapping, Optional

logger = logging.getLogger(__name__)


# Defaults matching plan §4.2.
_DEFAULT_AGGREGATOR_WINDOW_S = 2.0
_DEFAULT_MAX_UTTERANCE_S = 300.0


@dataclass(frozen=True)
class ZelloConfig:
    """Parsed Zello plugin configuration.

    Construct via :func:`load_config`; do not instantiate directly so the
    validation rules stay in one place.
    """

    issuer: str
    private_key_path: str
    username: str
    password: str
    channel: str

    # Optional
    allowed_users: frozenset[str] = field(default_factory=frozenset)
    allow_all_users: bool = False
    home_channel: Optional[str] = None
    aggregator_window_s: float = _DEFAULT_AGGREGATOR_WINDOW_S
    max_utterance_s: float = _DEFAULT_MAX_UTTERANCE_S

    @property
    def effective_home_channel(self) -> str:
        return self.home_channel or self.channel


REQUIRED_ENV = (
    "ZELLO_ISSUER",
    "ZELLO_PRIVATE_KEY_PATH",
    "ZELLO_USERNAME",
    "ZELLO_PASSWORD",
    "ZELLO_CHANNEL",
)


def _parse_bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _parse_csv(raw: str) -> frozenset[str]:
    return frozenset(s.strip() for s in raw.split(",") if s.strip())


def _parse_float(raw: str, default: float, *, name: str = "") -> float:
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        if name:
            logger.warning(
                "zello: %s=%r is not a valid float; falling back to default %r",
                name, raw, default,
            )
        else:
            logger.warning(
                "zello: %r is not a valid float; falling back to default %r",
                raw, default,
            )
        return default


def missing_required(env: Optional[Mapping[str, str]] = None) -> list[str]:
    """Return the list of required env vars that are missing or blank.

    Used by the registry's ``check_fn`` and ``validate_config`` hooks.
    """
    e = env if env is not None else os.environ
    return [k for k in REQUIRED_ENV if not (e.get(k) or "").strip()]


def load_config(env: Optional[Mapping[str, str]] = None) -> ZelloConfig:
    """Parse env vars into a :class:`ZelloConfig`.

    Raises ``ValueError`` if any required key is missing or blank.
    """
    e = env if env is not None else os.environ

    missing = missing_required(e)
    if missing:
        raise ValueError(f"Missing required env vars: {', '.join(missing)}")

    return ZelloConfig(
        issuer=e["ZELLO_ISSUER"].strip(),
        private_key_path=e["ZELLO_PRIVATE_KEY_PATH"].strip(),
        username=e["ZELLO_USERNAME"].strip(),
        password=e["ZELLO_PASSWORD"],  # don't strip — password may have edges
        channel=e["ZELLO_CHANNEL"].strip(),
        allowed_users=_parse_csv(e.get("ZELLO_ALLOWED_USERS", "")),
        allow_all_users=_parse_bool(e.get("ZELLO_ALLOW_ALL_USERS", "")),
        home_channel=(e.get("ZELLO_HOME_CHANNEL") or "").strip() or None,
        aggregator_window_s=_parse_float(
            e.get("ZELLO_AGGREGATOR_WINDOW_S", ""),
            _DEFAULT_AGGREGATOR_WINDOW_S,
            name="ZELLO_AGGREGATOR_WINDOW_S",
        ),
        max_utterance_s=_parse_float(
            e.get("ZELLO_MAX_UTTERANCE_S", ""),
            _DEFAULT_MAX_UTTERANCE_S,
            name="ZELLO_MAX_UTTERANCE_S",
        ),
    )
