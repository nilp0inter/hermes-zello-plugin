# SETUP — Zello developer console + plugin deploy

Out-of-band steps the plugin can't do for you.  Resolves
`HERMES-ZELLO-PLAN.md` §9 Q4.

## 1. Zello developer console (https://developers.zello.com)

1. Create an account if you don't have one.  Two-tier model: a
   *developer* account owns the Channel API "Application"; a separate
   *consumer Zello* account is what the bot logs in as on the channel.
2. Create an Application in the developer console.  Note the **issuer
   ID** (used as the JWT `iss` claim) — looks like a long opaque token.
3. Generate an RSA keypair in the console.  Download the **private key
   (PEM)** — keep this offline; it cannot be re-downloaded.  Treat it
   like a database password.
4. Create or assign a **Zello account** for the bot.  Anything goes for
   the username, but it must be unique on Zello and you must know the
   password.  (This project uses `nilp0inter` as the bot account.)
5. Create a **dedicated channel** in the developer console.  Anything
   for the name; this project uses `pichufletos`.  Add the bot account
   as a member.  Add your own consumer Zello account as a member too —
   if you skip this, you cannot key the mic into the channel from your
   phone.
6. Install the Zello consumer app on your phone, log in with your
   personal Zello account (NOT the bot account), join the channel.
   PTT into the channel to confirm it shows up in the developer
   console's live log.

## 2. Local dev environment

```bash
cd /path/to/hermes-zello-plugin
cp .env.example .env
$EDITOR .env       # fill in ZELLO_ISSUER, USERNAME, PASSWORD, CHANNEL
mv ~/Downloads/zello-private.key ./private.key
chmod 600 private.key

nix develop                # ffmpeg + libopus + python + uv
uv sync                    # installs aiozello (pinned SHA), opuslib, pyjwt, ...
uv run pytest -q           # 30 tests; smoke + unit
```

## 3. Production deploy (NixOS / sops-nix)

This plugin repo does NOT contain the deploy aspect — that lives in
`nilp0inter/nixos-config` per the plan §6.  Once the aspect is wired,
add to the agent's sops file (host-side, by you with `sops`):

```yaml
# resources/secrets/runtime/_agents/nil-agent/secrets.sops.yaml
# extend the existing hermes-env multi-line value with:
ZELLO_ISSUER=<from Zello developer console>
ZELLO_PASSWORD=<bot account password>
GROQ_API_KEY=<from console.groq.com>
ELEVENLABS_API_KEY=<from elevenlabs.io>

# Add a new sops secret for the binary PEM key, with
#   path = "${HERMES_HOME}/secrets/zello-private-key.pem"
#   owner = "nil-agent", mode = 0400
nil-agent-zello-private-key: |
  -----BEGIN RSA PRIVATE KEY-----
  ...
  -----END RSA PRIVATE KEY-----
```

Non-secret env (in `services.hermes-agent.environment`):

```nix
ZELLO_USERNAME = "nilp0inter";
ZELLO_CHANNEL  = "pichufletos";
ZELLO_ALLOWED_USERS = "nilp0inter_dev";   # YOUR phone's Zello account
ZELLO_AGGREGATOR_WINDOW_S = "2";
ZELLO_MAX_UTTERANCE_S = "300";
ZELLO_PRIVATE_KEY_PATH = "/var/lib/hermes/secrets/zello-private-key.pem";

# STT/TTS for voice
# (in services.hermes-agent.settings)
stt.provider = "groq";
tts.provider = "elevenlabs";
tts.providers.elevenlabs.model = "eleven_flash_v2_5";
```

Bind-mount the plugin tree into the nspawn container:

```nix
bindMounts."/var/lib/hermes/.hermes/plugins/zello" = {
  hostPath = "${pkgs.hermes-zello-plugin}";   # = packages.default from this flake
  isReadOnly = true;
};
```

Add `pkgs.libopus` + `pkgs.ffmpeg-headless` to the container's
`environment.systemPackages` so opuslib's ctypes loader and the
plugin's ffmpeg subprocess both resolve.

Deploy (YOU run this — coding agent does not):

```bash
just check tachyon
just switch tachyon
```

## 4. Smoke test the live integration

```bash
ssh tachyon -- journalctl -M nil-agent -u hermes-agent -f
```

Expected lines on first connect:

```
[Zello] connected — channel=pichufletos username=nilp0inter allowed=['nilp0inter_dev']
[Zello] channel_status channel=pichufletos status=online users_online=...
```

From the phone, key the mic into `pichufletos`, say a short sentence,
release.  Expected within 5-10 seconds:

```
zello: flushing utterance from sender='nilp0inter_dev' (X.Xs, ... bytes, reason=window)
[Zello] dispatching VOICE MessageEvent sender=nilp0inter_dev bytes=... path=...
... (Hermes STT / agent / TTS lines)
[Zello] outbound: streamed N opus packets (X.XXs of audio)
```

And you should hear the agent's reply over the channel.

## 5. Troubleshooting

- **`cannot import name 'Application' from 'aiozello'`** — pinned commit
  predates a re-export PR.  The plugin imports via
  `aiozello.__main__.Application`; if you bump to a newer aiozello that
  re-exports, you can revert the import path.
- **No PCM after `decode()`** — Zello is sending a codec other than
  Opus (rare).  Plugin logs "stream from sender=... produced no PCM"
  and drops the utterance.  Check `journalctl` for any prior
  `on_unknown_command` logs.
- **`ffmpeg binary not found on PATH`** — container is missing
  `pkgs.ffmpeg-headless` in `environment.systemPackages`.  Re-deploy.
- **`OSError: ... libopus.so.0`** — `LD_LIBRARY_PATH` doesn't include
  the libopus store path.  In the container, either set
  `LD_LIBRARY_PATH` explicitly in the systemd unit or rely on
  `programs.nix-ld.enable = true` + library hints.  The plan §6.2
  spells out the env setup.
- **Allow-list denies everyone silently** — empty `ZELLO_ALLOWED_USERS`
  with `ZELLO_ALLOW_ALL_USERS=false` is the safe default but appears
  broken.  Check journal for `dropping stream from unauthorized
  sender=`.  Add your Zello handle to the comma-separated list.
- **Bot replies to itself in a loop** — the bot's outbound PTT echoes
  back as an `on_stream_start` from `username=<bot>`.  The allow-list
  drops it because the bot account is NOT in `ZELLO_ALLOWED_USERS`.
  Keep it that way.
