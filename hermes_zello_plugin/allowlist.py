"""Allow-list check for inbound Zello senders.

Two-tier: ``allow_all`` (dev-only override) defeats the allow-list entirely;
otherwise the sender's lowercased username must appear in ``allowed_users``.

Zello usernames are case-insensitive on the wire — normalise both sides.
"""

from __future__ import annotations

from typing import Iterable


def is_user_authorized(
    sender: str,
    *,
    allowed_users: Iterable[str],
    allow_all: bool,
) -> bool:
    """True iff *sender* may address the bot.

    Empty / blank *sender* is always denied.  An empty allow-list with
    ``allow_all=False`` denies everyone — this is the safe default when the
    user forgot to configure ``ZELLO_ALLOWED_USERS``.
    """
    if not sender or not sender.strip():
        return False
    if allow_all:
        return True
    needle = sender.strip().lower()
    return any(needle == u.strip().lower() for u in allowed_users if u)
