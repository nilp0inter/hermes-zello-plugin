"""hermes-zello-plugin — Zello platform adapter for Hermes Agent.

Plugin entry point: ``register(ctx)``.  Loaded lazily via PEP 562
``__getattr__`` so importing peripheral modules (``outbound``,
``aggregator``, ``config``) does NOT trigger the adapter's
``gateway.platforms.base`` import.  This lets test harnesses and standalone
scripts use individual modules without a hermes-agent install.

The hermes plugin loader does ``from hermes_zello_plugin import register``
which routes through ``__getattr__`` → ``adapter.register``.
"""
from typing import Any

__all__ = ["register"]


def __getattr__(name: str) -> Any:  # PEP 562
    if name == "register":
        from .adapter import register

        return register
    raise AttributeError(f"module 'hermes_zello_plugin' has no attribute {name!r}")
