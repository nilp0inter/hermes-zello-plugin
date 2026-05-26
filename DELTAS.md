# Deltas vs. `HERMES-ZELLO-PLAN.md`

Captures every place the implementation deviates from the plan, with the
reason.  The plan is canonical for design intent; this file is canonical
for what the code actually does.  When in doubt, read the code — both
documents drift over time.

---

## §4.5 — `ZelloAdapter` overrides

| Plan said | Code does | Why |
|---|---|---|
| Plugin spins its own JWT refresh task (re-issue + reconnect every ~50 min) | aiozello owns the refresh: `Application(token_loader=ltm.issue, token_refresh_interval_s=3000.0)`.  Adapter passes the callable + interval; no plugin-side timer. | aiozello shipped `LocalTokenManager.issue` as a zero-arg callable plus a `_refresh_timer_loop` that triggers a soft reconnect (closes WS, reloads token via `token_loader`).  Outbound-stream-active backoff (≤30 s) is in upstream too.  Plugin re-implementing this would duplicate state. |
| Plugin reconnects with exponential backoff on WS close | aiozello owns the reconnect loop: `Application.run()` retries with 0.5 s → 60 s cap, ±25 % jitter, resets to 0.5 s on logon success. | Same — upstream shipped it.  Adapter just spawns `app.run()` as a long-running task and lets it heal. |
| `Platform.ZELLO` enum value, or dynamic platform identity | `Platform("zello")` — works via `gateway.config.Platform._missing_()` once `platform_registry.is_registered("zello")` returns True. | Hermes `Platform` enum has `_missing_()` that fabricates pseudo-members for plugin-registered platforms (see `gateway/config.py:130-173`).  Same idiom IRC uses (`plugins/platforms/irc/adapter.py:104`). |

## §4.6 — Inbound utterance aggregator

| Plan said | Code does | Why |
|---|---|---|
| Subscribe to `on_stream_start`, `on_stream`, `on_stream_stop` raw packet callbacks | Subscribe to `on_stream_start` and `on_stream_stop` only.  Inbound audio is drained from `app.streams[stream_id]` — an `IncomingAudioStream` with `decode()` async generator yielding PCM. | aiozello populates `app.streams` on `on_stream_start` and routes binary opus packets into the per-stream `asyncio.Queue` (`Application._handle_message` in `aiozello/__main__.py`).  No `on_stream` packet callback exists in `KNOWN_CALLBACKS`. |
| Concatenate raw opus packets into Ogg-Opus container via `pyogg` or hand-rolled framing (§9 Q1) | Drain `stream.decode()` to PCM, concatenate per-user PCM, then `ffmpeg -f s16le ... -c:a libopus -f ogg pipe:1` on flush. | `IncomingAudioStream.decode()` already runs `opuslib.Decoder` internally, so PCM is what's available.  Re-using ffmpeg (already needed for outbound) drops the `pyogg` C-lib dependency.  Slight lossy double-encode is irrelevant for Whisper STT.  Resolves §9 Q1. |
| `MessageEvent(message_type=MessageType.VOICE, attachments=[<path>], source=...)` | `MessageEvent(text="", message_type=MessageType.VOICE, media_urls=[<path>], media_types=["audio/ogg"], source=...)` | `MessageEvent` field is `media_urls: List[str]` + `media_types: List[str]` (`gateway/platforms/base.py:1175-1176`).  `attachments` does not exist on the dataclass. |
| Aggregator window starts on `on_stream_stop` | Aggregator window starts when the per-stream consumer task's `decode()` generator exhausts. | `Application._handle_message` puts `None` into the stream queue on `on_stream_stop`, which terminates `decode()` naturally — see `aiozello/__main__.py:386` and `aiozello/stream.py:79-82`.  Same effect, simpler control flow; resolves §9 Q5 (flush-on-disconnect is in `flush_all()`). |

## §4.7 — Outbound opus pipeline

| Plan said | Code does | Why |
|---|---|---|
| Procedural API: `self._open_outbound_stream(...)`, `self._ws.send_bytes(encode_audio_packet(...))`, `self._close_outbound_stream(...)` | Context-manager API: `async with app.outbound_stream(codec_header, packet_duration_ms) as stream: await stream.send(opus_bytes)` | aiozello shipped this shape — `OutboundAudioStream` with auto-incremented seq.  Cleaner.  Underlying `_send_start`/`_send_packet`/`_send_stop` are private to the stream object. |
| `frames_per_packet=2, frame_size_ms=60` (120 ms compound packets) | `frames_per_packet=1, frame_size_ms=60` (60 ms single-frame packets) | libopus' high-level encoder produces single-frame Opus packets.  Building TOC-byte code-1 compound packets by hand for marginal bandwidth savings is not worth the complexity at WAN bit-rates.  Both shapes are spec-compliant. |
| Drift-forward pacing inline in `send_voice` | Same logic, extracted into `outbound.DriftForwardPacer` with injected `time_fn` / `sleep_fn` | Testable in isolation against a deterministic fake clock. |

## §4.9 — Dependencies

| Plan said | Code does | Why |
|---|---|---|
| `pyogg` for inbound concatenation | No `pyogg`.  ffmpeg subprocess handles PCM → Ogg-Opus. | See §4.6 above. |

## §5 — aiozello upstream fixes — STATUS

Pinned at `c79fca8adc1dc4b3426e62898e83d2c71bd38d61` (HEAD of `main` at
plugin work start).  pyproject still says `version = "0.1.0"`; no PyPI
publish; no git tags.  Pin by SHA in `pyproject.toml`'s
`[tool.uv.sources]`.

| §5 fix | Status |
|---|---|
| Module side-effects gated under `if __name__ == "__main__":` | ✅ `aiozello/__main__.py:412` |
| Root-logger config removed from import path | ✅ `logging.basicConfig` only inside `__main__` guard |
| Outbound audio API on `Application` | ✅ (different shape — see §4.7 above) |
| `on_text_message` in `KNOWN_CALLBACKS` + dispatched | ✅ `aiozello/__main__.py:101` |
| Typed-dataclass callback dispatch | ✅ `convert_to_dataclass()`; `from` field renamed to `sender` |
| JWT refresh hook | ✅ via `token_loader=` + `token_refresh_interval_s=` |
| Reconnect + exponential backoff + jitter | ✅ in `Application.run()` |
| `Application` import-without-env-vars test | ❌ no `tests/test_application.py` — plugin compensates with a smoke test |
| Bump to 0.2.0 + PyPI publish | ❌ — pin by SHA |

## §6 — NixOS integration (this repo)

Not touched by the plugin repo — lives in `nilp0inter/nixos-config` per
the plan.  This repo ships:

- `flake.nix` with a `devShells.default` (uv, python3, ffmpeg-headless,
  libopus on `LD_LIBRARY_PATH`) and a `packages.default` that stages the
  plugin tree (`plugin.yaml` + `hermes_zello_plugin/`) into a single
  Nix-store path the host aspect can bind-mount.
- The plan §6.1 mentions `buildPythonApplication` with
  `propagatedBuildInputs`; the implementation uses a bare
  `stdenv.mkDerivation` that copies the source tree, leaving Python
  dependency resolution to the consumer (hermes-agent's plugin loader
  picks them up from its own venv).  Resolves §9 Q3.

## §7 — Out of scope

All v1 negative constraints honored:

- Smart-chunking of outbound TTS — not implemented.
- Barge-in — not implemented.
- Streaming-during-PTT STT — not implemented.
- Inbound text — logged + discarded by `_on_text_message`.
- Inbound images / locations — no callbacks wired; aiozello's default
  `print_callback` debug-logs them and drops them on the floor.
- Outbound text fallback — `send()` returns `SendResult(success=False,
  error="Zello v1 is voice-only; ...")`.
- Multiple channels — `ZELLO_CHANNEL` is a single string.
- Per-platform LLM override — not exposed.
- Local STT / TTS — not packaged.
- Interactive setup wizard — `setup_fn=None`.

---

## Tests covered

`tests/`:

- `test_config_env.py` — env-var parsing, allow-list CSV, bool parsing,
  float parsing with fallback, missing-required listing.  9 tests.
- `test_outbound_pacing.py` — `chunk_pcm` padding + boundary cases,
  `DriftForwardPacer` steady-state, slow-iteration catch-up, long-term
  drift convergence.  6 tests.
- `test_aggregator.py` — single stream flush, multi-stream concat,
  per-user isolation, allow-list deny, allow-all bypass, max-utterance
  ceiling, `flush_all()`, unknown stream_id, `on_stream_stop` no-op.  9
  tests.
- `test_smoke.py` — `register(ctx)` kwargs, plugin.yaml structure,
  `_env_enablement`, `validate_config` true/false paths.  6 tests.

Plus 30 passing total (`uv run pytest -q`).

## Tests NOT covered (manual / end-to-end only)

- Live connect to Zello WS.
- Real ffmpeg PCM↔Ogg-Opus round-trip (the subprocess is invoked at
  runtime but unit tests don't drive it).
- libopus dlopen / opuslib encode in CI (we import opuslib at module
  load via `outbound.py`; runs OK because the devshell exposes
  `libopus` on `LD_LIBRARY_PATH`).
- 90 s PTT split into 2 streams from a real phone app, end-to-end
  through hermes STT.  Plan §8.2 acceptance criterion.
