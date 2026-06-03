"""Smoke test: ``register(ctx)`` invocation + ``plugin.yaml`` parses.

Validates the surface that the hermes plugin loader will exercise at
gateway startup.  Does NOT actually connect to Zello.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest


@dataclass
class _FakeCtx:
    """Records the kwargs ``register_platform`` was called with.

    Mirrors ``hermes_cli.plugins.PluginContext``'s surface: the plugin
    invokes ``register_platform`` (for the zello adapter) and
    ``register_tool`` (for the end_session tool); we record both so a
    failed registration surfaces in the captured-dict shape rather than
    as an AttributeError.
    """

    captured: dict[str, Any] = field(default_factory=dict)
    tools: list[dict[str, Any]] = field(default_factory=list)

    def register_platform(self, **kwargs: Any) -> None:
        self.captured = kwargs

    def register_tool(self, **kwargs: Any) -> None:
        self.tools.append(kwargs)


def test_register_passes_expected_keys_to_ctx():
    from hermes_zello_plugin import register

    ctx = _FakeCtx()
    register(ctx)

    cap = ctx.captured
    assert cap["name"] == "zello"
    assert cap["label"] == "Zello"
    assert callable(cap["adapter_factory"])
    assert callable(cap["check_fn"])
    assert callable(cap["validate_config"])
    assert callable(cap["is_connected"])
    assert "ZELLO_ISSUER" in cap["required_env"]
    assert cap["setup_fn"] is None  # no interactive wizard in v1
    assert callable(cap["env_enablement_fn"])
    assert cap["cron_deliver_env_var"] == "ZELLO_HOME_CHANNEL"
    assert callable(cap["standalone_sender_fn"])
    assert cap["allowed_users_env"] == "ZELLO_ALLOWED_USERS"
    assert cap["allow_all_env"] == "ZELLO_ALLOW_ALL_USERS"
    assert cap["max_message_length"] == 0
    assert cap["emoji"] == "📻"
    assert cap["pii_safe"] is False
    assert cap["allow_update_command"] is True
    assert "Zello" in cap["platform_hint"]


def test_plugin_yaml_minimally_valid():
    """``plugin.yaml`` should parse and contain the kind=platform shape."""
    yaml_text = (Path(__file__).resolve().parent.parent / "plugin.yaml").read_text()
    # Cheap structural checks rather than a full YAML lib dep
    assert "name: hermes-zello-plugin" in yaml_text
    assert "kind: platform" in yaml_text
    for var in (
        "ZELLO_ISSUER",
        "ZELLO_PRIVATE_KEY_PATH",
        "ZELLO_USERNAME",
        "ZELLO_PASSWORD",
        "ZELLO_CHANNEL",
    ):
        assert var in yaml_text, f"{var} missing from plugin.yaml"


def test_env_enablement_returns_seed_when_channel_set(monkeypatch):
    from hermes_zello_plugin.adapter import _env_enablement

    monkeypatch.setenv("ZELLO_CHANNEL", "pichufletos")
    monkeypatch.delenv("ZELLO_HOME_CHANNEL", raising=False)
    seed = _env_enablement()
    assert seed is not None
    assert seed["channel"] == "pichufletos"
    assert seed["home_channel"]["platform"] == "zello"
    assert seed["home_channel"]["chat_id"] == "pichufletos"


def test_env_enablement_returns_none_when_channel_unset(monkeypatch):
    from hermes_zello_plugin.adapter import _env_enablement

    monkeypatch.delenv("ZELLO_CHANNEL", raising=False)
    assert _env_enablement() is None


def test_validate_config_false_when_required_env_blank(monkeypatch):
    from hermes_zello_plugin.adapter import validate_config

    for k in (
        "ZELLO_ISSUER",
        "ZELLO_PRIVATE_KEY_PATH",
        "ZELLO_USERNAME",
        "ZELLO_PASSWORD",
        "ZELLO_CHANNEL",
    ):
        monkeypatch.delenv(k, raising=False)
    assert validate_config(object()) is False


def test_validate_config_true_when_all_required_present(monkeypatch):
    from hermes_zello_plugin.adapter import validate_config

    monkeypatch.setenv("ZELLO_ISSUER", "iss")
    monkeypatch.setenv("ZELLO_PRIVATE_KEY_PATH", "/tmp/k")
    monkeypatch.setenv("ZELLO_USERNAME", "u")
    monkeypatch.setenv("ZELLO_PASSWORD", "p")
    monkeypatch.setenv("ZELLO_CHANNEL", "c")
    assert validate_config(object()) is True


# ── Import-time independence (dashboard plugin discovery) ────────────────


def test_adapter_module_imports_without_aiozello_or_opuslib(monkeypatch):
    """``hermes_zello_plugin.adapter`` must load when aiozello/opuslib
    aren't importable.  The hermes-dashboard service runs with a slimmer
    PYTHONPATH than the gateway — without lazy imports the plugin would
    fail to register there and the dashboard would silently omit
    zello from the platform-status list.
    """
    import importlib
    import sys

    # Drop any cached plugin modules so re-import re-runs top-level code.
    for mod in list(sys.modules):
        if (
            mod == "hermes_zello_plugin"
            or mod.startswith("hermes_zello_plugin.")
            or mod == "aiozello"
            or mod.startswith("aiozello.")
            or mod == "opuslib"
            or mod.startswith("opuslib.")
        ):
            monkeypatch.delitem(sys.modules, mod, raising=False)
    # Block re-import: a None entry in sys.modules makes ``import X`` raise.
    monkeypatch.setitem(sys.modules, "aiozello", None)
    monkeypatch.setitem(sys.modules, "opuslib", None)

    # Top-level package + lazy-attribute ``register`` must both work.
    pkg = importlib.import_module("hermes_zello_plugin")
    register = pkg.register  # triggers __getattr__ → adapter import
    assert callable(register)

    ctx = _FakeCtx()
    register(ctx)
    assert ctx.captured["name"] == "zello"


# ── _env_enablement: config.yaml channel_prompts seed ────────────────────


def test_env_enablement_seeds_channel_prompts_from_config_yaml(
    monkeypatch, tmp_path
):
    """``_env_enablement`` reads ``zello.channel_prompts`` from
    ``$HERMES_HOME/config.yaml`` and includes it in the seed dict.

    Required because production hermes (faa13e49) iterates
    ``list(Platform)`` only in its YAML→extra bridging loop; plugin
    platforms must seed their own channel_prompts to reach the
    adapter's ``self.config.extra``.
    """
    from hermes_zello_plugin.adapter import _env_enablement

    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        "zello:\n"
        "  channel_prompts:\n"
        "    nil-agent-2317d6b: |\n"
        "      Behave like a diario.\n"
    )
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("ZELLO_CHANNEL", "nil-agent-2317d6b")
    monkeypatch.delenv("ZELLO_HOME_CHANNEL", raising=False)

    seed = _env_enablement()
    assert seed is not None
    assert "channel_prompts" in seed
    assert (
        seed["channel_prompts"]["nil-agent-2317d6b"].strip()
        == "Behave like a diario."
    )


def test_env_enablement_omits_channel_prompts_when_absent_in_yaml(
    monkeypatch, tmp_path
):
    """No ``zello`` block in config.yaml → seed has no ``channel_prompts``
    key.  ``resolve_channel_prompt`` will then return None and only the
    static PLATFORM_HINT applies — same shape as before this feature.
    """
    from hermes_zello_plugin.adapter import _env_enablement

    cfg = tmp_path / "config.yaml"
    cfg.write_text("telegram:\n  reactions: true\n")  # no zello block
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("ZELLO_CHANNEL", "ch")

    seed = _env_enablement()
    assert seed is not None
    assert "channel_prompts" not in seed


def test_env_enablement_swallows_yaml_errors(monkeypatch, tmp_path):
    """A malformed config.yaml must not crash plugin enablement —
    channel_prompts is a best-effort seed.
    """
    from hermes_zello_plugin.adapter import _env_enablement

    cfg = tmp_path / "config.yaml"
    cfg.write_text("zello:\n  channel_prompts:\n    foo: [unbalanced")
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("ZELLO_CHANNEL", "ch")

    seed = _env_enablement()
    assert seed is not None  # still produces a seed
    assert "channel_prompts" not in seed  # but no prompts


# ── _apply_yaml_config: zello.* YAML → ZELLO_* env vars ──────────────────


def test_apply_yaml_config_translates_keys_to_env(monkeypatch):
    """The Mattermost-style YAML→env bridge sets ``ZELLO_*`` env vars
    from ``zello:`` config keys when those env vars are not already set.
    """
    from hermes_zello_plugin.adapter import _apply_yaml_config

    for k in (
        "ZELLO_ALLOWED_USERS",
        "ZELLO_ALLOW_ALL_USERS",
        "ZELLO_AGGREGATOR_WINDOW_S",
        "ZELLO_MAX_UTTERANCE_S",
        "ZELLO_HOME_CHANNEL",
    ):
        monkeypatch.delenv(k, raising=False)

    import os
    result = _apply_yaml_config(
        {"zello": {}},  # full yaml_cfg (unused by this hook)
        {
            "allowed_users": ["alice", "bob"],
            "allow_all_users": False,
            "aggregator_window_s": 0.5,
            "max_utterance_s": 120,
            "home_channel": "ch-xyz",
        },
    )
    assert result is None  # everything flows through env
    assert os.environ["ZELLO_ALLOWED_USERS"] == "alice,bob"
    assert os.environ["ZELLO_ALLOW_ALL_USERS"] == "false"
    assert os.environ["ZELLO_AGGREGATOR_WINDOW_S"] == "0.5"
    assert os.environ["ZELLO_MAX_UTTERANCE_S"] == "120"
    assert os.environ["ZELLO_HOME_CHANNEL"] == "ch-xyz"


def test_apply_yaml_config_env_takes_precedence(monkeypatch):
    """Pre-existing env vars must NOT be overwritten by config.yaml
    values.  Mirrors Mattermost's ``not os.getenv(...)`` guard.
    """
    from hermes_zello_plugin.adapter import _apply_yaml_config

    import os
    monkeypatch.setenv("ZELLO_ALLOWED_USERS", "from-env")
    monkeypatch.setenv("ZELLO_AGGREGATOR_WINDOW_S", "9.9")

    _apply_yaml_config(
        {"zello": {}},
        {"allowed_users": ["alice"], "aggregator_window_s": 0.1},
    )
    assert os.environ["ZELLO_ALLOWED_USERS"] == "from-env"
    assert os.environ["ZELLO_AGGREGATOR_WINDOW_S"] == "9.9"


def test_register_passes_apply_yaml_config_fn():
    """Registration kwargs include the new ``apply_yaml_config_fn`` hook.

    The compat filter in :func:`register` drops it transparently on
    older hermes versions whose ``PlatformEntry`` lacks the field, so
    the captured kwargs always contain the function regardless of
    runtime hermes version (the filter operates on the registry side).
    """
    from hermes_zello_plugin import register

    ctx = _FakeCtx()
    register(ctx)
    assert callable(ctx.captured.get("apply_yaml_config_fn"))


def test_check_requirements_does_not_need_aiozello(monkeypatch):
    """``check_fn`` runs in the dashboard process where aiozello may be
    absent.  It must only verify env vars + pyjwt (a stable hermes
    dep), not the heavy runtime libs.
    """
    import sys
    from hermes_zello_plugin.adapter import check_requirements

    # Pretend aiozello / opuslib / ffmpeg are absent.
    monkeypatch.setitem(sys.modules, "aiozello", None)
    monkeypatch.setitem(sys.modules, "opuslib", None)
    monkeypatch.setenv("PATH", "")  # ffmpeg no longer resolvable

    # Required env vars present → True.
    monkeypatch.setenv("ZELLO_ISSUER", "iss")
    monkeypatch.setenv("ZELLO_PRIVATE_KEY_PATH", "/tmp/k")
    monkeypatch.setenv("ZELLO_USERNAME", "u")
    monkeypatch.setenv("ZELLO_PASSWORD", "p")
    monkeypatch.setenv("ZELLO_CHANNEL", "c")
    assert check_requirements() is True
