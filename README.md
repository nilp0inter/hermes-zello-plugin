# hermes-zello-plugin

Zello Channel API platform plugin for [Hermes Agent](https://github.com/NousResearch/hermes-agent).

Long-form voice memos in, short clarifying questions / action proposals out.
PTT-driven, half-duplex. Voice-only in v1.

## Architecture

Inbound: Zello channel → aiozello WebSocket → per-user utterance aggregator
(handles the ~60 s phone-app PTT segmentation) → Ogg-Opus packaged via
ffmpeg → Hermes `MessageEvent(message_type=MessageType.VOICE)` → Groq STT
→ agent → text reply.

Outbound: text → Hermes TTS (ElevenLabs) → audio file → ffmpeg PCM decode →
opuslib re-encode → aiozello outbound stream with realtime pacing → Zello
channel.

See `HERMES-ZELLO-PLAN.md` for the full design report and `DELTAS.md` for
deviations discovered during implementation.

## Install

The plugin is consumed as a tree under `~/.hermes/plugins/zello/`. Hermes
discovers `plugin.yaml` and imports `hermes_zello_plugin.register(ctx)`.

For local development:

```
nix develop
uv sync
uv run pytest
```

`flake.nix` ships a devShell with `python3`, `uv`, `ffmpeg-headless`, and
`libopus` (on `LD_LIBRARY_PATH` for `opuslib`'s ctypes loader).

## Configure

See `.env.example` for the full env-var surface. Required:

- `ZELLO_ISSUER`, `ZELLO_PRIVATE_KEY_PATH`, `ZELLO_USERNAME`, `ZELLO_PASSWORD`,
  `ZELLO_CHANNEL`.

Optional:

- `ZELLO_ALLOWED_USERS`, `ZELLO_ALLOW_ALL_USERS`, `ZELLO_HOME_CHANNEL`,
  `ZELLO_AGGREGATOR_WINDOW_S` (default 2), `ZELLO_MAX_UTTERANCE_S` (default
  300).

Out-of-band setup checklist: `SETUP.md`.

## Test

```
uv run pytest -q
```

Tests stub `gateway.*` modules so they run without a `hermes-agent`
checkout.

## License

Apache-2.0.
