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


def _mapping_get(obj: Any, name: str, key: str) -> Any:
    mapping = getattr(obj, name, None)
    if isinstance(mapping, dict):
        return mapping.get(key)
    return None


def _pop_if_same(obj: Any, name: str, key: str, expected: Any) -> None:
    if expected is None:
        return
    mapping = getattr(obj, name, None)
    if isinstance(mapping, dict) and mapping.get(key) is expected:
        mapping.pop(key, None)


def _release_runner_state(
    runner: Any,
    session_key: str,
    old_running_agent: Any,
    old_runner_pending: Any,
) -> None:
    running_agents = getattr(runner, "_running_agents", None)
    if (
        isinstance(running_agents, dict)
        and running_agents.get(session_key) is old_running_agent
    ):
        try:
            runner._release_running_agent_state(session_key)
        except Exception:
            running_agents.pop(session_key, None)
            running_ts = getattr(runner, "_running_agents_ts", None)
            if isinstance(running_ts, dict):
                running_ts.pop(session_key, None)
            busy_ts = getattr(runner, "_busy_ack_ts", None)
            if isinstance(busy_ts, dict):
                busy_ts.pop(session_key, None)

    _pop_if_same(runner, "_pending_messages", session_key, old_runner_pending)


def _release_adapter_state(
    adapter: Any,
    session_key: str,
    old_adapter_guard: Any,
    old_adapter_task: Any,
    old_adapter_pending: Any,
) -> None:
    if adapter is None:
        return

    _pop_if_same(adapter, "_pending_messages", session_key, old_adapter_pending)
    _pop_if_same(adapter, "_session_tasks", session_key, old_adapter_task)

    if old_adapter_guard is None:
        return
    release_guard = getattr(adapter, "_release_session_guard", None)
    if callable(release_guard):
        release_guard(session_key, guard=old_adapter_guard)
        return
    active_sessions = getattr(adapter, "_active_sessions", None)
    if (
        isinstance(active_sessions, dict)
        and active_sessions.get(session_key) is old_adapter_guard
    ):
        active_sessions.pop(session_key, None)


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
    session_key = get_session_env("HERMES_SESSION_KEY") or ""

    if platform_str != "zello" or not chat_id:
        return tool_error("end_session only valid in a live zello session")

    source = SessionSource(
        platform=Platform("zello"),
        chat_id=chat_id,
        user_id=user_id,
        chat_type="channel",
    )
    if not session_key:
        try:
            session_key = runner._session_key_for_source(source)
        except Exception:
            session_key = ""

    try:
        adapter = runner.adapters.get(source.platform)
    except Exception:
        adapter = None

    old_running_agent = _mapping_get(runner, "_running_agents", session_key)
    old_runner_pending = _mapping_get(runner, "_pending_messages", session_key)

    old_adapter_guard = _mapping_get(adapter, "_active_sessions", session_key)
    old_adapter_task = _mapping_get(adapter, "_session_tasks", session_key)
    old_adapter_pending = _mapping_get(adapter, "_pending_messages", session_key)

    event = MessageEvent(text="/new", source=source)
    await runner._handle_reset_command(event)
    _release_runner_state(
        runner, session_key, old_running_agent, old_runner_pending
    )

    _release_adapter_state(
        adapter,
        session_key,
        old_adapter_guard,
        old_adapter_task,
        old_adapter_pending,
    )

    return tool_result(ok=True, reason=(args or {}).get("reason", ""))
