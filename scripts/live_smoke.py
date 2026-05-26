#!/usr/bin/env python3
"""Live Zello connection smoke test (non-invasive — no outbound PTT).

Loads ``.env`` from the repo root, instantiates an ``aiozello.Application``
with the real credentials, waits for the channel-online ``ChannelStatus``,
then disconnects cleanly.  No audio is streamed; the developer-console
live log will show only the logon/logout pair.

Run from the repo root inside ``nix develop``::

    uv run python scripts/live_smoke.py [--timeout 15]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path


def _load_dotenv(path: Path) -> None:
    """Minimal .env reader — sets `os.environ` for unset keys."""
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        if "=" not in s:
            continue
        k, v = s.split("=", 1)
        k = k.strip()
        v = v.strip()
        # Strip surrounding quotes if any
        if len(v) >= 2 and v[0] == v[-1] and v[0] in {"'", '"'}:
            v = v[1:-1]
        if k and k not in os.environ:
            os.environ[k] = v


async def main(timeout_s: float) -> int:
    _load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    from aiozello.__main__ import Application
    from aiozello.auth import LocalTokenManager
    from aiozello.protocol import ChannelStatus

    issuer = os.environ.get("ZELLO_ISSUER", "").strip()
    pk_path = os.environ.get("ZELLO_PRIVATE_KEY_PATH", "./private.key").strip()
    username = os.environ.get("ZELLO_USERNAME", "").strip()
    password = os.environ.get("ZELLO_PASSWORD", "")
    channel = os.environ.get("ZELLO_CHANNEL", "").strip()

    missing = [
        k
        for k, v in {
            "ZELLO_ISSUER": issuer,
            "ZELLO_PRIVATE_KEY_PATH": pk_path,
            "ZELLO_USERNAME": username,
            "ZELLO_PASSWORD": password,
            "ZELLO_CHANNEL": channel,
        }.items()
        if not v
    ]
    if missing:
        print(f"FAIL: missing env: {', '.join(missing)}", file=sys.stderr)
        return 2
    if not Path(pk_path).exists():
        print(f"FAIL: private key not found at {pk_path}", file=sys.stderr)
        return 2

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    log = logging.getLogger("live_smoke")

    connected = asyncio.Event()

    async def on_status(event: ChannelStatus) -> None:
        log.info("channel_status channel=%s status=%s users_online=%s error=%s",
                 event.channel, event.status, event.users_online, event.error)
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

    log.info("connecting (channel=%s username=%s)", channel, username)
    run_task = asyncio.create_task(app.run(), name="zello-live-smoke")

    try:
        try:
            await asyncio.wait_for(connected.wait(), timeout=timeout_s)
            log.info("OK — channel is online")
            return 0
        except asyncio.TimeoutError:
            log.error("FAIL — no channel-online status within %.1fs", timeout_s)
            return 1
    finally:
        log.info("disconnecting")
        try:
            await app.disconnect()
        except Exception:
            log.exception("disconnect raised")
        if not run_task.done():
            run_task.cancel()
            try:
                await run_task
            except (asyncio.CancelledError, Exception):
                pass


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--timeout", type=float, default=20.0)
    args = ap.parse_args()
    sys.exit(asyncio.run(main(args.timeout)))
