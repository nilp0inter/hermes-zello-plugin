# Hermes Zello Plugin — Technical Report

Status: spec, not implemented.
Audience: coding agent (and human reviewer) implementing the integration.
Scope: v1 only. v2 items are explicitly listed in [§ Out of scope](#out-of-scope-negative-constraints).

---

## 1. Context

`nil-agent` is a NousResearch [hermes-agent](https://github.com/NousResearch/hermes-agent) instance running in a systemd-nspawn container on the `tachyon` host of this fleet. It is defined by:

- `modules/aspects/agents/nil-agent.nix` (container + agent service)
- `modules/aspects/nil-agent.nix` (identity / age key)
- Imports upstream module via `inputs.hermes-agent.nixosModules.default`
- API at `192.168.100.11:8642` (bearer token), dashboard at `:9119`
- Caddy vhosts: `api.nil-agent.ruso{negro,blanco}.com` (plain), `dashboard.nil-agent.ruso{negro,blanco}.com` (mTLS)
- Existing platforms: Telegram (`TELEGRAM_ALLOWED_USERS=13622077`, group `-395539431`)

The goal is to add **Zello** as an additional first-class hermes messaging platform so the user can dictate long voice memos (typical 30 s – 1.5 min, max 3–4 min) from the Zello consumer app on a phone, and receive short voice replies (clarifying questions or proposed actions) in the same dedicated Zello channel.

The integration ships as a standalone hermes platform plugin (`hermes-zello-plugin`), uses the upstream platform-plugin SPI documented at `gateway/platforms/ADDING_A_PLATFORM.md` of the hermes-agent repo, and requires upstream fixes to [`nilp0inter/aiozello`](https://github.com/nilp0inter/aiozello) before it can be consumed cleanly. No core hermes-agent fork is required.

---

## 2. Locked design decisions

| # | Decision | Notes |
|---|---|---|
| 1 | **One-shot PTT semantics**, no streaming-during-PTT | Per-PTT → one `MessageType.VOICE` event → one agent turn → one outbound PTT |
| 2 | **Dedicated private Zello channel** for nil-agent (name TBD by user in Zello developer console) | One channel; no shared/human channel addressing |
| 3 | **STT = `groq`** (built-in hermes provider, `whisper-large-v3-turbo` or current Groq Whisper default) | Cost ≈ $0.04 / hour audio; ~500 ms for short, scales linearly |
| 4 | **TTS = `elevenlabs`** (built-in hermes provider, Flash v2.5 model) | TTFB ~300–800 ms |
| 5 | **aiozello reuse: fix upstream first, then depend** | Plugin pins a post-fix aiozello commit |
| 6 | **v1 scope: voice only** | Inbound text/image/location ignored or no-op |
| 7 | **No smart-chunking** of outbound TTS in v1 | Replies are short by design (asymmetric model, see §4.2) — single outbound PTT |
| 8 | **Plugin name: `hermes-zello-plugin`** | Standalone repo, recommend `nilp0inter/hermes-zello-plugin` |
| A | **Barge-in deferred to v2** | Replies are short; user can wait through TTS |
| B | (n/a — smart-chunk deferred) | |
| C | **Per-channel LLM override: none** | Keep agent-wide `deepseek/deepseek-v4-flash` |
| D | **Utterance aggregator: mandatory, 2 s default window** | Phone-app caps PTT at 60 s, so typical (30 s – 1.5 min) inputs will arrive as 1–3 back-to-back streams. Aggregate same-user streams within 2 s of previous stop into one logical `MessageType.VOICE` event. Tunable via `ZELLO_AGGREGATOR_WINDOW_S` env var. |

Asymmetric interaction model (load-bearing for §4.2 PLATFORM_HINTS): the user dictates at length; the agent responds with a short clarifying question or "shall I do X?" proposal. Never long expositions.

---

## 3. Architecture overview

Three deliverables:

| # | Component | Repo / location | Owner |
|---|---|---|---|
| A | aiozello upstream fixes | `nilp0inter/aiozello` (open PR) | nilp0inter |
| B | `hermes-zello-plugin` itself | `nilp0inter/hermes-zello-plugin` (new) | nilp0inter |
| C | NixOS integration (package, sub-aspect, secrets) | this repo (`nilp0inter/nixos-config`) | nilp0inter |

### Data flow (inbound)

```
Zello channel  ─── wss://zello.io/ws ───►  hermes-zello-plugin (in nil-agent nspawn)
                                                │
                                                │ aiozello: opus packets in
                                                ▼
                                          UtteranceAggregator
                                          (groups same-user streams
                                           within ZELLO_AGGREGATOR_WINDOW_S)
                                                │
                                                ▼
                                          Allow-list check (ZELLO_ALLOWED_USERS)
                                                │
                                                ▼
                                          Write Ogg-Opus container to tmp
                                          via cache_audio_from_bytes(data, ext=".ogg")
                                                │
                                                ▼
                                          self.handle_message(MessageEvent(
                                              message_type=MessageType.VOICE,
                                              attachments=[<path>],
                                              source=SessionSource(...),
                                          ))
                                                │
                                                ▼
                                          Hermes gateway → groq STT → agent loop → reply text
                                                │
                                                ▼ (text reply)
                                          Hermes TTS (elevenlabs) → audio file
                                                │
                                                ▼
                                          adapter.send_voice(chat_id, audio_path)
```

### Data flow (outbound `send_voice`)

```
audio_path (mp3/wav/ogg from elevenlabs)
        │
        ▼
ffmpeg/pydub decode → PCM int16 mono @ 16 kHz
        │
        ▼
opuslib.Encoder(sample_rate=16000, channels=1, application=VOIP)
encode at frame_size_ms=60, frames_per_packet=2
        │
        ▼
ws.send_str(start_stream JSON: type=audio, codec=opus,
            codec_header=encode_codec_header(16000, 2, 60),
            packet_duration=120)
        │
        ▼
for each packet: ws.send_bytes(encode_audio_packet(stream_id, seq, opus_bytes))
                 await asyncio.sleep(packet_duration_s)   # realtime pacing
        │
        ▼
ws.send_str(stop_stream JSON: stream_id=...)
```

---

## 4. Component B: `hermes-zello-plugin`

### 4.1 Repository layout

```
hermes-zello-plugin/
├── plugin.yaml
├── pyproject.toml          # uv-managed (deviates from aiozello's poetry)
├── README.md
├── flake.nix               # vix-style, perSystem, exposes `packages.default`
├── flake.lock
├── hermes_zello_plugin/
│   ├── __init__.py         # exposes `register(ctx)`
│   ├── adapter.py          # ZelloAdapter(BasePlatformAdapter)
│   ├── aggregator.py       # UtteranceAggregator
│   ├── outbound.py         # send_voice opus encode + ws pacing
│   ├── config.py           # env var parsing
│   ├── allowlist.py        # _is_user_authorized helper
│   └── platform_hint.py    # the PLATFORM_HINTS string (constant)
└── tests/
    ├── test_aggregator.py
    ├── test_outbound_pacing.py
    └── test_config_env.py
```

### 4.2 `plugin.yaml`

```yaml
name: hermes-zello-plugin
label: Zello
kind: platform
version: 0.1.0
description: >
  Zello Channel API platform adapter for Hermes Agent.
  Long-form voice memos in, short clarifying questions / action
  proposals out. PTT-driven, half-duplex.
author: nilp0inter
requires_env:
  - name: ZELLO_ISSUER
    description: "Zello developer-console issuer ID (JWT iss claim)"
    prompt: "Zello issuer"
    password: false
  - name: ZELLO_PRIVATE_KEY_PATH
    description: "Filesystem path to the Zello-issued RSA private key (PEM)"
    prompt: "Private key path"
    password: false
  - name: ZELLO_USERNAME
    description: "Zello account username this bot connects as"
    prompt: "Zello username"
    password: false
  - name: ZELLO_PASSWORD
    description: "Zello account password"
    prompt: "Zello password"
    password: true
  - name: ZELLO_CHANNEL
    description: "Zello channel to join (must be created in the developer console)"
    prompt: "Channel name"
    password: false
optional_env:
  - name: ZELLO_ALLOWED_USERS
    description: "Comma-separated Zello usernames allowed to talk to the bot"
    prompt: "Allowed users (comma-separated)"
    password: false
  - name: ZELLO_ALLOW_ALL_USERS
    description: "Allow any channel member to talk to the bot (dev only)"
    prompt: "Allow all? (true/false)"
    password: false
  - name: ZELLO_HOME_CHANNEL
    description: "Channel for cron / notification delivery (defaults to ZELLO_CHANNEL)"
    prompt: "Home channel (or empty)"
    password: false
  - name: ZELLO_AGGREGATOR_WINDOW_S
    description: "Seconds of idle after a stream_stop before flushing aggregated utterance (default 2)"
    prompt: "Aggregator window (s)"
    password: false
  - name: ZELLO_MAX_UTTERANCE_S
    description: "Max wall-clock seconds an aggregator will hold before forced flush (default 300)"
    prompt: "Max utterance (s)"
    password: false
```

### 4.3 `__init__.py` — plugin entry point

```python
from .adapter import register
__all__ = ["register"]
```

### 4.4 `register(ctx)` (in `adapter.py`)

Mirrors `plugins/platforms/irc/adapter.py:927-969` of hermes-agent:

```python
def register(ctx):
    from .platform_hint import PLATFORM_HINT
    ctx.register_platform(
        name="zello",
        label="Zello",
        adapter_factory=lambda cfg: ZelloAdapter(cfg),
        check_fn=check_requirements,           # checks aiohttp, pyjwt, opuslib import
        validate_config=validate_config,       # checks required env vars
        is_connected=is_connected,
        required_env=[
            "ZELLO_ISSUER", "ZELLO_PRIVATE_KEY_PATH",
            "ZELLO_USERNAME", "ZELLO_PASSWORD", "ZELLO_CHANNEL",
        ],
        install_hint="pip install aiozello opuslib",
        setup_fn=None,                         # no interactive wizard in v1
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="ZELLO_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="ZELLO_ALLOWED_USERS",
        allow_all_env="ZELLO_ALLOW_ALL_USERS",
        max_message_length=0,                  # voice-only, no text size limit relevant
        emoji="📻",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=PLATFORM_HINT,
    )
```

### 4.5 `ZelloAdapter` required overrides

| Method | Behavior |
|---|---|
| `__init__(self, config)` | Call `super().__init__(config, Platform.ZELLO)` — but `Platform.ZELLO` is not a built-in enum value; the plugin SPI accepts a string `name="zello"` and creates a dynamic platform identity. Follow the IRC plugin's pattern (it does not extend `Platform` enum). |
| `connect() -> bool` | Issue JWT, open WS via aiozello's fixed `Application`, register inbound callbacks, return True on logon success. Start the `UtteranceAggregator` task. Start a JWT-refresh task (re-issue + reconnect every ~50 min to stay inside the 60-min TTL). |
| `disconnect()` | Cancel aggregator + refresh tasks, close WS gracefully, log out. |
| `send(chat_id, text, ...) -> SendResult` | **v1 NO-OP** with `SendResult(success=False, error="Zello v1 is voice-only; use send_voice")`. Caller is hermes core, which should never invoke `send` for a voice-only platform; the no-op + logged warning catches misroutes. |
| `send_typing(chat_id)` | No-op (Zello has no typing indicator). Return immediately. |
| `send_image(chat_id, url, caption)` | **v1 NO-OP**, return failure. |
| `get_chat_info(chat_id) -> dict` | Return `{"name": ZELLO_CHANNEL, "type": "channel", "chat_id": ZELLO_CHANNEL}`. There is exactly one chat — the dedicated channel. |
| `send_voice(chat_id, audio_path, ...) -> SendResult` | Drive the outbound opus pipeline (see §4.7). |
| `play_tts(chat_id, audio_path, ...)` | Delegate to `send_voice`. |
| `prepare_tts_text(text)` | Default scrubber (strip markdown) is fine; can override later if needed. |

### 4.6 Inbound: `UtteranceAggregator`

Responsibilities:
- Subscribe to aiozello `on_stream_start`, `on_stream`, `on_stream_stop`.
- Group consecutive streams from the same `from` user where each next `on_stream_start` arrives within `ZELLO_AGGREGATOR_WINDOW_S` (default 2.0) of the previous `on_stream_stop`. Use an `asyncio.TimerHandle` per active aggregation; reset on new stream start.
- On flush (window expires or `ZELLO_MAX_UTTERANCE_S` exceeded):
  - Concatenate received opus packets into a single Ogg-Opus container (use `pyogg` or write the Ogg framing manually — small).
  - Drop the file via hermes's `cache_audio_from_bytes(data, ext=".ogg")` (imported from `gateway.platforms.base`).
  - Check allow-list (`ZELLO_ALLOWED_USERS` / `ZELLO_ALLOW_ALL_USERS`). Drop with logged warning if denied.
  - Build `MessageEvent(message_type=MessageType.VOICE, attachments=[<path>], source=SessionSource(platform="zello", chat_id=ZELLO_CHANNEL, user_id=<zello-username>, ...))` via `self.build_source(...)` per the base contract.
  - Dispatch via `await self.handle_message(event)`.

Edge cases:
- New aggregation starts for user A while user B's aggregation is open → maintain per-user state. Each user has at most one open aggregation.
- WS reconnect mid-aggregation → flush current buffer (best-effort) before reconnect, then resume fresh.

### 4.7 Outbound: `send_voice` opus pipeline

```python
async def send_voice(self, chat_id, audio_path, ...):
    pcm16 = await self._decode_to_pcm16_16k_mono(audio_path)   # ffmpeg or pydub
    encoder = opuslib.Encoder(16000, 1, opuslib.APPLICATION_VOIP)
    frame_size_ms = 60
    frames_per_packet = 2
    samples_per_frame = 16000 // 1000 * frame_size_ms          # 960
    samples_per_packet = samples_per_frame * frames_per_packet # 1920
    packet_duration_s = (frame_size_ms * frames_per_packet) / 1000.0  # 0.12

    stream_id = await self._open_outbound_stream(
        codec_header=encode_codec_header(16000, frames_per_packet, frame_size_ms),
        packet_duration_ms=int(frame_size_ms * frames_per_packet),
    )
    try:
        seq = 0
        next_send_at = asyncio.get_event_loop().time()
        for packet_pcm in self._chunk_pcm(pcm16, samples_per_packet):
            opus_bytes = encoder.encode(packet_pcm, samples_per_frame)
            await self._ws.send_bytes(encode_audio_packet(stream_id, seq, opus_bytes))
            seq += 1
            next_send_at += packet_duration_s
            sleep_for = next_send_at - asyncio.get_event_loop().time()
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
    finally:
        await self._close_outbound_stream(stream_id)
    return SendResult(success=True, message_id=str(stream_id))
```

Pacing matters: Zello servers expect realtime delivery (~one `packet_duration` of audio per `packet_duration` of wall-clock). Bursts will be dropped or cause the listener side to fast-forward / glitch. The above scheme drifts forward (`next_send_at`-based), not back, so a slow loop iteration auto-catches up by reducing the next sleep.

### 4.8 PLATFORM_HINTS (load-bearing)

`platform_hint.py` exports `PLATFORM_HINT` — injected into the system prompt whenever the agent's current turn was triggered from the Zello platform. Recommended starting text:

```
You are speaking over Zello, a half-duplex push-to-talk voice channel.
The user dictates voice memos — typically 30 seconds to a few minutes —
and the message you receive is the transcription of what they said.
Your reply will be spoken back via TTS over the same channel.

Behavior rules:
- Reply with a short clarifying question OR a "shall I do X?" action
  proposal. Never long explanations, never recaps of what the user said.
- Target ~15 seconds of spoken audio. Hard cap ~30 seconds.
- No markdown, no code blocks, no lists — plain prose only.
- No emoji.
- If you need to perform an action that requires approval, propose it
  explicitly ("Want me to ...?") and wait for the next user PTT before
  executing.
- Transcriptions may include filler ("uh", "um") and ASR errors. Be
  charitable; ask one clarifying question rather than guessing on
  ambiguous wording.
```

### 4.9 Dependencies

- `aiozello >= <post-fix-version>` (see §5 for required fixes)
- `aiohttp` (transitive via aiozello)
- `pyjwt[crypto]` (transitive)
- `opuslib` (transitive; also needed directly for outbound encoder)
- `pyogg` (or hand-rolled Ogg framing) — for inbound concatenation
- ffmpeg binary at runtime (for outbound audio decode of arbitrary TTS output formats) OR `pydub` + `audioop` if ffmpeg is undesirable

System library: `libopus.so.0` must be loadable. nix-ld is on inside the nil-agent container; the integration aspect (§6) must put libopus in `LD_LIBRARY_PATH`.

### 4.10 Logging

- Module logger only; do not call `logging.basicConfig()` or set root-logger handlers (aiozello v0.1.0 does this — one of the upstream fixes).
- Use `self.name` (set by `BasePlatformAdapter`) as the log prefix following IRC adapter convention: `logger.info("[%s] connected to channel %s", self.name, channel)`.
- Redact `ZELLO_PASSWORD` and the JWT in all log output.

---

## 5. Component A: aiozello upstream fixes

PR target: `nilp0inter/aiozello`. Minimum scope needed for the plugin to import and use it cleanly:

| Fix | Current state (file:line) | Required state |
|---|---|---|
| Module import has side-effects | `aiozello/__main__.py:186-193` reads env vars + runs `asyncio.run(app.run())` at import time | Move all of that into an `if __name__ == "__main__":` guard. The `Application` class must be importable without env vars set. |
| Hardcoded root-logger configuration | `aiozello/__main__.py:19-21` sets `DEBUG` level + adds a `StreamHandler` to the module logger | Drop the `setLevel` and `addHandler` calls. Library code never configures handlers. |
| No outbound audio API on `Application` | `aiozello/protocol.py:23-28` defines `encode_audio_packet`, but `Application` (in `__main__.py`) exposes no `start_stream` / `send_audio` / `stop_stream` methods | Add three async methods on `Application`: `start_outbound_stream(codec_header, packet_duration_ms) -> int` (returns server-assigned `stream_id`), `send_audio_packet(stream_id, seq, opus_bytes)`, `stop_outbound_stream(stream_id)`. These wrap `ws.send_str(...)` for the JSON commands and `ws.send_bytes(...)` for the audio packets per the Zello Channel API spec. |
| No `on_text_message` callback dispatch | `aiozello/__main__.py:77` `KNOWN_CALLBACKS` lacks it; `protocol.py:62-66` `TextMessage` dataclass is dead | Add `on_text_message` to `KNOWN_CALLBACKS`. Dispatch the `"on_text_message"` server command to it. (Even though v1 of the hermes plugin ignores text, the dispatch should be correct.) Same shape for `on_channel_status` → use `ChannelStatus`, `on_stream_start` → `StreamStart`, `on_stream_stop` → `StreamStop`. |
| Typed-dataclass dispatch (optional but recommended) | Callbacks receive raw `**data` dicts | Construct the appropriate dataclass from the dispatch loop and pass it as the single positional arg. Plugin code is much easier to write against typed objects. |
| No JWT refresh | `LocalTokenManager.issue()` exists but `Application` never re-issues | Expose a hook so the consumer can replace the token mid-session (e.g. `Application.refresh_token(new_jwt)` that re-sends a `logon` command, or simpler: factor `Application.run()` so it can be re-entered with a fresh token after WS reconnect). Plugin will own the refresh-schedule task. |
| No reconnect with backoff | `Application.run()` exits on WS close | Wrap the inner loop in a reconnect helper with exponential backoff + jitter, capped at e.g. 60 s. Reset backoff on successful logon. |
| Tests for `Application` | Only auth/codec/stream unit tests exist | Add at minimum: import-without-env-vars test, callback-registration test, outbound start/data/stop happy path against a mock WS. |
| Bump to `0.2.0`, publish to PyPI (or pin via VCS in consumer) | n/a | Plugin pins a commit or a `^0.2.0` semver. |

These fixes are mechanical and small (estimated <400 LOC delta total). User owns the repo so review is internal.

---

## 6. Component C: NixOS integration in this repo

### 6.1 New package: `modules/packages/hermes-zello-plugin.nix`

Vix-style `perSystem` package. Fetches the plugin repo via `pkgs.fetchFromGitHub` pinned to a specific rev, builds with `python3.pkgs.buildPythonApplication` (or just stages the source tree — the plugin is loaded by hermes at runtime, not run as a standalone binary). The package output is a directory containing the plugin tree as hermes expects:

```
${package}/
├── plugin.yaml
└── hermes_zello_plugin/
    ├── __init__.py
    └── ...
```

Runtime Python deps (`aiozello`, `aiohttp`, `pyjwt[crypto]`, `opuslib`, `pyogg`) declared in `propagatedBuildInputs`. ffmpeg in `runtimeInputs` if used. The hermes-agent runtime inside the container picks up the plugin from `$HERMES_HOME/plugins/zello/`, so the package's job is to make the tree available — actual placement is by tmpfiles in the aspect.

### 6.2 New sub-aspect: extend `modules/aspects/agents/nil-agent.nix`

Either extend the existing `provides.nil-agent` block, or add a sibling `provides.nil-agent-zello` sub-aspect that nil-agent's host includes. Adding a sibling keeps the base nil-agent provider free of Zello-specific concerns; recommended.

The sub-aspect's `nixos` module must, inside the container `config`:

1. Bind-mount the package's plugin tree into the container at `${HERMES_HOME}/plugins/zello/`:

   ```nix
   bindMounts."/var/lib/hermes/.hermes/plugins/zello" = {
     hostPath = "${pkgs.hermes-zello-plugin}";
     isReadOnly = true;
   };
   ```

   (Or use a `systemd.tmpfiles` `L+` rule to symlink — bind-mount is more robust to Nix store GC churn.)

2. Add `pkgs.libopus`, `pkgs.ffmpeg-headless` (or `pkgs.ffmpeg`) to the container's `environment.systemPackages` so the runtime can resolve them.

3. Extend `services.hermes-agent.environment` with the non-secret Zello config:

   ```nix
   ZELLO_USERNAME = "<bot-username>";
   ZELLO_CHANNEL = "<channel-name>";
   ZELLO_ALLOWED_USERS = "<your-zello-handle>";
   ZELLO_AGGREGATOR_WINDOW_S = "2";
   ZELLO_MAX_UTTERANCE_S = "300";
   ```

4. Secret keys land via the existing `hermes-env` env file path (already mounted into the container — see `agents/nil-agent.nix:171-177`). Add to the agent's sops file (host-side action, done by the user with sops, not the coding agent): `ZELLO_ISSUER`, `ZELLO_PASSWORD`, and the PEM private key as a separate sops secret with `path = "${HERMES_HOME}/secrets/zello-private-key.pem"` (mounted via bindMount or sops-nix's in-container deployment, owner = `nil-agent`, mode `0400`). Set `ZELLO_PRIVATE_KEY_PATH` env var to that path.

5. STT/TTS provider config in `services.hermes-agent.settings`:

   ```nix
   stt.provider = "groq";
   tts.provider = "elevenlabs";
   tts.providers.elevenlabs.model = "eleven_flash_v2_5";
   ```

   Plus the corresponding API keys via the sops `hermes-env` file: `GROQ_API_KEY`, `ELEVENLABS_API_KEY`.

### 6.3 Sops mutations (user-performed)

Coding agent should NOT decrypt or write to sops files. Produce a checklist for the user instead:

```
# add to resources/secrets/runtime/_agents/nil-agent/secrets.sops.yaml's
# hermes-env multiline value:
ZELLO_ISSUER=<from Zello developer console>
ZELLO_PASSWORD=<bot account password>
GROQ_API_KEY=<from console.groq.com>
ELEVENLABS_API_KEY=<from elevenlabs.io>

# add new sops secret (separate key for binary PEM):
nil-agent-zello-private-key: |
  -----BEGIN RSA PRIVATE KEY-----
  ...
  -----END RSA PRIVATE KEY-----
```

### 6.4 Flake input (if plugin is fetched as a flake input)

Append in `modules/aspects/agents/default.nix` or wherever upstream-input pins live:

```nix
flake-file.inputs.hermes-zello-plugin.url =
  "github:nilp0inter/hermes-zello-plugin/<commit-sha>";
```

Then run `just write-flake` (NOT `nix flake update` directly — see CLAUDE.md house rules).

If the plugin is instead packaged purely under `modules/packages/`, fetch via `fetchFromGitHub` in the .nix file and skip the flake input.

---

## 7. Out of scope (negative constraints)

Explicitly NOT in v1. Do not implement, do not stub, do not add config knobs for these:

- **Smart-chunking of outbound TTS over multiple PTTs.** Replies are short by prompt design (§4.8).
- **Barge-in** (interrupting outbound TTS with a new inbound PTT). Replies are short enough to wait through.
- **Streaming-during-PTT STT.** Hermes's central STT dispatch handles the whole utterance at flush time.
- **Inbound Zello text messages.** Plugin acknowledges and discards.
- **Inbound Zello images / locations.** Same — acknowledge and discard.
- **Outbound text fallback.** `send()` returns failure.
- **Multiple Zello channels.** One dedicated channel per `nil-agent` instance.
- **Per-platform LLM override.** Agent-wide `deepseek/deepseek-v4-flash` is fine.
- **Local STT (whisper.cpp).** Tachyon has no GPU; 0.5× realtime on CPU is unusable for typical 30 s – 1.5 min inputs.
- **Local TTS (piper).** Acceptable quality-wise but elevenlabs is more natural for voice replies and latency is comparable for short outputs.
- **Interactive setup wizard.** `setup_fn=None`. All config via env vars only.
- **`hermes gateway` UI integration beyond the auto-wired bits.** No custom status display, no custom `/zello` slash command.
- **PyPI publish for the plugin in v1.** Consume from git directly via Nix.
- **Zello consumer-app PTT 60 s segmentation as a hard problem.** The `UtteranceAggregator` makes it invisible to hermes. No further handling needed.

---

## 8. Acceptance criteria

### 8.1 Component A (aiozello fixes)

- [ ] `from aiozello import Application` succeeds with NO env vars set (regression test).
- [ ] Calling `logging.getLogger("aiozello")` returns a logger with no extra handlers attached.
- [ ] `Application` exposes async `start_outbound_stream`, `send_audio_packet`, `stop_outbound_stream`.
- [ ] `KNOWN_CALLBACKS` contains `on_text_message`; corresponding server command is dispatched.
- [ ] Reconnect-with-backoff verified in a test against a mock WS that closes after N seconds.
- [ ] Unit tests pass: `pytest -q` green.

### 8.2 Component B (`hermes-zello-plugin`)

- [ ] `plugin.yaml` validates against hermes's plugin schema (run with `hermes plugins validate <path>` if such a CLI exists; otherwise eyeball against `plugins/platforms/irc/plugin.yaml`).
- [ ] `register(ctx)` invoked at plugin-load time registers `name="zello"` in `platform_registry`.
- [ ] Inbound: keying a 90 s PTT from a Zello phone app (which generates 2 sequential ≤60 s streams) produces ONE `MessageType.VOICE` event in hermes containing the full 90 s of audio. Verified by transcription completeness in the agent's log.
- [ ] Inbound: a non-allow-listed Zello user's PTT is dropped with a warning log and produces NO `MessageEvent`.
- [ ] Outbound: agent text reply of <=30 s spoken duration is delivered as one outbound PTT, audible on the channel, free of glitches / fast-forwarding (verified by listening).
- [ ] JWT refresh: a 2-hour live session does not disconnect (token TTL is 1 hour, refresh happens silently around the 50-minute mark).
- [ ] WS reconnect: killing tachyon's network for 30 s and restoring it results in reconnect within 60 s, no plugin process crash.
- [ ] Unit tests pass for aggregator, outbound pacing, config env parsing.

### 8.3 Component C (NixOS integration)

- [ ] `just check tachyon` succeeds.
- [ ] `just switch tachyon` (or `just fw` per user's preferred deploy path) deploys cleanly. (Coding agent should NOT run this — propose the command and let the user run it.)
- [ ] After deploy, `journalctl -M nil-agent -u hermes-agent` shows the plugin registered (`Registered platform adapter: zello (plugin)`) and connected to the Zello channel.
- [ ] `hermes gateway status` (run inside the container) lists Zello as connected.
- [ ] All sops secrets resolve at boot; no plaintext key on disk outside `/run/secrets/`.

---

## 9. Open questions for the implementer

These were not resolved in the design discussion and require either a code-time decision with rationale, or a brief check-back with the user:

1. **Ogg-Opus framing library.** `pyogg` adds a C dep; rolling Ogg page framing by hand is ~150 LOC. Pick one and note the rationale in the PR.
2. **ffmpeg vs pydub for outbound TTS decode.** ffmpeg is heavier but universal. pydub depends on ffmpeg under the hood anyway. Recommend ffmpeg-headless invoked via `asyncio.create_subprocess_exec`.
3. **Plugin packaging shape in Nix.** `buildPythonApplication` produces a wrapped entry point we don't need; `mkDerivation` that stages the tree may be cleaner. Pick based on what reuses existing `modules/packages/` conventions.
4. **Zello channel access.** The user must create the Zello channel and a developer-console application (issuer + RSA keypair) before any of this runs. Coding agent should produce a one-page setup checklist as a `SETUP.md` in the plugin repo. Channel name will be plugged into `ZELLO_CHANNEL` env var.
5. **Aggregator flush on disconnect.** Mid-aggregation WS close: flush buffered audio to hermes anyway (probably yes — partial transcription is better than silent loss) or drop? Recommend flush-on-close.
6. **`SessionSource` schema for Zello.** Plugin must populate `platform="zello"`, `chat_id=<channel-name>`, `user_id=<zello-username>`. Confirm no additional fields are required by inspecting `gateway/session.py` `SessionSource` dataclass at implementation time — schema may have evolved since this report.

---

## 10. Reference: source pins

For the coding agent's reproducibility — exact commits researched during design:

- `NousResearch/hermes-agent` — main branch at the time of this report; key files:
  - `gateway/platforms/ADDING_A_PLATFORM.md` — plugin SPI contract
  - `gateway/platforms/base.py:709-767` — `cache_audio_from_bytes`, `cache_audio_from_url`
  - `gateway/platforms/base.py:2243-2283` — `send_voice`, `play_tts`, `prepare_tts_text` defaults
  - `gateway/platforms/base.py:3560-3650` — Telegram `send_voice` reference
  - `gateway/platforms/telegram.py:5165-5183` — Telegram inbound voice memo handling
  - `gateway/platform_registry.py` — `PlatformEntry` dataclass
  - `agent/transcription_registry.py` — STT provider registry; built-ins listed line 40-47
  - `agent/tts_registry.py` — TTS provider registry; built-ins listed line 48-59
  - `plugins/platforms/irc/{plugin.yaml,__init__.py,adapter.py}` — closest-shape working example (long-lived socket adapter, stdlib only)

- `nilp0inter/aiozello` — main branch at the time of this report:
  - `aiozello/__main__.py:19-21` — root-logger setup to remove
  - `aiozello/__main__.py:77` — `KNOWN_CALLBACKS` list to extend
  - `aiozello/__main__.py:119-184` — `Application` class to extend with outbound methods + reconnect
  - `aiozello/__main__.py:186-193` — module-level side-effects to gate
  - `aiozello/protocol.py:23-28` — `encode_audio_packet` (already usable)
  - `aiozello/codec.py:5-21` — `encode_codec_header` (already usable)
  - `aiozello/auth.py:50-81` — `LocalTokenManager` (already usable)

- `zelloptt/zello-channel-api` — `API.md` for the Channel API spec (logon, start_stream, on_stream_start, audio packet framing, error codes).

---

End of report.
