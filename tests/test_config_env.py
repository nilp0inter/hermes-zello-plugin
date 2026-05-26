"""Tests for ``hermes_zello_plugin.config``."""

from __future__ import annotations

import pytest

from hermes_zello_plugin.config import (
    REQUIRED_ENV,
    ZelloConfig,
    load_config,
    missing_required,
)


_BASE_ENV = {
    "ZELLO_ISSUER": "iss-abc",
    "ZELLO_PRIVATE_KEY_PATH": "/tmp/private.key",
    "ZELLO_USERNAME": "bot-acct",
    "ZELLO_PASSWORD": "hunter2",
    "ZELLO_CHANNEL": "pichufletos",
}


def test_load_minimal_required_env_uses_defaults():
    cfg = load_config(_BASE_ENV)
    assert isinstance(cfg, ZelloConfig)
    assert cfg.issuer == "iss-abc"
    assert cfg.private_key_path == "/tmp/private.key"
    assert cfg.username == "bot-acct"
    assert cfg.password == "hunter2"
    assert cfg.channel == "pichufletos"
    # Optional defaults
    assert cfg.allowed_users == frozenset()
    assert cfg.allow_all_users is False
    assert cfg.home_channel is None
    assert cfg.aggregator_window_s == pytest.approx(2.0)
    assert cfg.max_utterance_s == pytest.approx(300.0)


def test_effective_home_channel_falls_back_to_channel():
    cfg = load_config({**_BASE_ENV, "ZELLO_HOME_CHANNEL": ""})
    assert cfg.effective_home_channel == "pichufletos"

    cfg2 = load_config({**_BASE_ENV, "ZELLO_HOME_CHANNEL": "broadcast"})
    assert cfg2.effective_home_channel == "broadcast"


def test_allowed_users_csv_parsing_trims_and_filters_blanks():
    env = {**_BASE_ENV, "ZELLO_ALLOWED_USERS": " alice ,bob,, , carol "}
    cfg = load_config(env)
    assert cfg.allowed_users == frozenset({"alice", "bob", "carol"})


def test_allow_all_truthy_strings():
    for raw in ("true", "True", "TRUE", "1", "yes", "on"):
        cfg = load_config({**_BASE_ENV, "ZELLO_ALLOW_ALL_USERS": raw})
        assert cfg.allow_all_users is True, f"expected truthy for {raw!r}"

    for raw in ("false", "0", "no", "off", "", "garbage"):
        cfg = load_config({**_BASE_ENV, "ZELLO_ALLOW_ALL_USERS": raw})
        assert cfg.allow_all_users is False, f"expected falsy for {raw!r}"


def test_float_overrides_and_invalid_fallback():
    cfg = load_config({**_BASE_ENV, "ZELLO_AGGREGATOR_WINDOW_S": "3.5"})
    assert cfg.aggregator_window_s == pytest.approx(3.5)

    cfg2 = load_config({**_BASE_ENV, "ZELLO_AGGREGATOR_WINDOW_S": "not-a-float"})
    assert cfg2.aggregator_window_s == pytest.approx(2.0)  # default

    cfg3 = load_config({**_BASE_ENV, "ZELLO_MAX_UTTERANCE_S": "60"})
    assert cfg3.max_utterance_s == pytest.approx(60.0)


def test_missing_required_lists_blank_and_absent_keys():
    env = {**_BASE_ENV, "ZELLO_ISSUER": "", "ZELLO_PASSWORD": "   "}
    del env["ZELLO_CHANNEL"]
    miss = missing_required(env)
    assert set(miss) == {"ZELLO_ISSUER", "ZELLO_PASSWORD", "ZELLO_CHANNEL"}


def test_load_config_raises_on_missing_required():
    env = dict(_BASE_ENV)
    del env["ZELLO_USERNAME"]
    with pytest.raises(ValueError) as exc_info:
        load_config(env)
    assert "ZELLO_USERNAME" in str(exc_info.value)


def test_password_not_stripped_to_preserve_edge_whitespace():
    # Passwords with leading/trailing whitespace are unusual but valid.
    env = {**_BASE_ENV, "ZELLO_PASSWORD": "  spaced  "}
    cfg = load_config(env)
    assert cfg.password == "  spaced  "


def test_required_env_tuple_is_the_documented_five():
    assert set(REQUIRED_ENV) == {
        "ZELLO_ISSUER",
        "ZELLO_PRIVATE_KEY_PATH",
        "ZELLO_USERNAME",
        "ZELLO_PASSWORD",
        "ZELLO_CHANNEL",
    }
