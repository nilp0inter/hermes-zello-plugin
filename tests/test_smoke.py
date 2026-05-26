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

    Mirrors ``hermes_cli.plugins.PluginContext.register_platform``'s
    signature: ``name``, ``label``, ``adapter_factory``, ``check_fn``
    are positional/early; everything else lands in ``**entry_kwargs``.
    """

    captured: dict[str, Any] = field(default_factory=dict)

    def register_platform(self, **kwargs: Any) -> None:
        self.captured = kwargs


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
