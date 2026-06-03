"""Session-control tool for Zello conversations."""

from __future__ import annotations

from typing import Any


END_SESSION_NAME = "end_session"
END_SESSION_TOOLSET = "session-control"
END_SESSION_SCHEMA = {
    "name": END_SESSION_NAME,
    "description": "End the current Zello voice session with full /new parity.",
    "parameters": {
        "type": "object",
        "properties": {
            "reason": {
                "type": "string",
                "description": "Optional reason for logs or audit trails.",
            },
        },
        "required": [],
        "additionalProperties": False,
    },
}


async def end_session_handler(args: dict[str, Any], **_kwargs: Any) -> str:
    """Reset the live Zello session via the gateway's /new implementation."""
    from gateway.config import Platform
    from gateway.platforms.base import MessageEvent
    from gateway.run import _gateway_runner_ref
    from gateway.session import SessionSource
    from gateway.session_context import get_session_env
    from tools.registry import tool_error, tool_result

    runner = _gateway_runner_ref()
    if runner is None:
        return tool_error("Gateway not running")

    platform_str = get_session_env("HERMES_SESSION_PLATFORM")
    chat_id = get_session_env("HERMES_SESSION_CHAT_ID")
    user_id = get_session_env("HERMES_SESSION_USER_ID") or None

    if platform_str != "zello" or not chat_id:
        return tool_error("end_session only valid in a live zello session")

    source = SessionSource(
        platform=Platform("zello"),
        chat_id=chat_id,
        user_id=user_id,
        chat_type="channel",
    )
    event = MessageEvent(text="/new", source=source)
    await runner._handle_reset_command(event)
    return tool_result(ok=True, reason=(args or {}).get("reason", ""))
