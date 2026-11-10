# Chromecast Voice Remote → Linux dictation mic

Hold the **voice button** on a Chromecast Voice Remote, speak, release, and the words are typed
into whatever window has focus. No laptop keyboard, no global shortcut.

This is the setup guide for the Chromecast-specific additions in this fork. For the daemon
itself — options, D-Bus control, other remotes — see the [root README](../README.md).

```
Chromecast Remote --BLE/ATVV--> atvvoice --PCM--> null sink
                                                     |
                                        module-remap-source (atvvoice-chromecast-remote)
                                                     |
                              Handy (system default input) --Whisper--> paste into focus
```

Verified on Pop!_OS 22.04 (GNOME, X11) with the G9N9N remote, PipeWire 1.0.x, Handy 0.8.x.

## What the remote negotiates

Read from `CAPS_RESP` with `atvvoice -d <ADDR> -vv`:

| Field | Value |
|---|---|
| Protocol | ATVV **v1.0** |
| Codec | **ADPCM 16 kHz**, mono, 160-byte frames |
| Interaction | **HoldToTalk** — audio only while the button is held |
| Stop reason | `AUDIO_STOP` with `HttButtonRelease` |

Three quirks this fork handles, all of which make the remote look dead on stock atvvoice:

1. The TX characteristic (`AB5E0002`) is **write-without-response only**; an ATT Write Request
   fails with `NotPermitted`, so the write op must be chosen from the characteristic flags.
2. A HoldToTalk remote **re-sends `START_SEARCH`** right after `AUDIO_START` on a single press.
   Treating that as a toggle-off closes the mic ~30 ms in.
3. A bare `pw_stream` `Audio/Source` node is **never scheduled** on PipeWire 1.0.x (`pw-top`
   shows `QUANT 0`, the process callback never runs) even though the node reports running. The
   mic therefore goes through a null sink that always has a driver, republished as a source.

## Requirements

- Linux with **PipeWire** and **pipewire-pulse** (`pactl`, `pw-cat`)
- **BlueZ** (`bluetoothctl`)
- **Rust** (`cargo`) — <https://rustup.rs>
- **Handy** for speech-to-text — <https://handy.computer> (not bundled)
- `xdotool` on X11, or `ydotool`/`wtype` on Wayland (see [Wayland](#wayland))
- A paired and trusted remote

Debian/Ubuntu build deps: `libpipewire-0.3-dev libdbus-1-dev libclang-dev pipewire-bin
pulseaudio-utils bluez pkg-config build-essential`. `./install.sh --install-deps` installs them.

## 1. Pair the remote

```bash
bluetoothctl
> scan on
# hold Back + Home until the remote appears
> devices                       # note the remote's address
> pair AA:BB:CC:DD:EE:FF
> trust AA:BB:CC:DD:EE:FF
> connect AA:BB:CC:DD:EE:FF
> quit
```

The installer auto-detects a device named `Chromecast Remote`, or takes `--mac`.

## 2. Install

```bash
git clone https://github.com/hs-ye/ATVVoice ~/GitRepos/ATVVoice
cd ~/GitRepos/ATVVoice
./install.sh --dry-run                  # prints every command, changes nothing
./install.sh --install-deps --set-default
```

| Flag | Effect |
|---|---|
| `--mac AA:BB:...` | Remote address (default: auto-detect) |
| `--prefix DIR` | Install prefix (default `~/.local`) |
| `--no-hwdb` | Skip the button remap |
| `--no-handy` | Do not touch Handy's settings |
| `--set-default` | Make the virtual mic the system default input |
| `--install-deps` | `sudo apt-get install` the missing packages |
| `--dry-run` | Print commands without running them |
| `--force` | Proceed despite a missing remote or running Handy |

`install.sh` builds the checkout it sits in — there is no patch series and no pinned commit.
Copy `install.env.example` to `install.env` for defaults.

## 3. Install Handy

Install from <https://handy.computer>, open it once to download a model (**whisper-small.en**
runs ~2.3× real time on the Radeon 680M), then quit it and re-run `./install.sh --no-hwdb`.
Handy rewrites its settings on exit, so the merge must happen while it is stopped.

Merged settings (`contrib/handy-settings.json`):

| Setting | Value | Why |
|---|---|---|
| `always_on_microphone` | `true` | keeps the stream open, so the toggle starts instantly and the first syllable is not clipped |
| `vad_enabled` | `true` | strips silence; a press with no speech produces no text |
| `vad_backend` | `silero` | |
| `selected_microphone` | `pipewire` | Handy only ever records the system default |
| `paste_method` | `direct` | types the transcript; see Wayland for the alternative |

## 4. Route the virtual mic

```bash
pactl set-default-source atvvoice-chromecast-remote
```

Handy records only the **system default input**, so this is required. The trade-off: every app
that does not pick its own input hears silence unless the voice button is held. `--set-default`
does it for you.

## 5. Test

Hold the voice button, say something, release. The transcript is pasted into the focused window.

```bash
systemctl --user status atvvoice
journalctl --user -u atvvoice -f              # AUDIO_START / trigger(start) / trigger(stop)
pactl list short sources | grep atvvoice
tail ~/.local/state/handy-remote/trigger.log  # what the wrapper decided
```

## What the installer changes

| Path | Purpose |
|---|---|
| `~/.local/bin/atvvoice` | the daemon |
| `~/.local/bin/handy-remote-trigger` | idempotent Handy start/stop wrapper |
| `~/.config/systemd/user/atvvoice.service` | user service, enabled at boot |
| `/etc/udev/hwdb.d/90-chromecast-remote.hwdb` | button remap (`--no-hwdb` skips) |
| `~/.local/share/com.pais.handy/settings_store.json` | Handy settings (backed up first) |
| `~/.local/state/handy-remote/` | wrapper state, lock and `trigger.log` |

## Button map

The extra buttons emit evdev codes above X11's 255-keycode ceiling (KEY_SELECT 353, KEY_TV 377,
588/589), so X cannot address them and they look dead. `hwdb` remaps them by **scancode**,
below X, which also means it works on Wayland:

| Button | Becomes |
|---|---|
| OK | `Enter` |
| Back | `Esc` |
| Home | `Super` (workspace overview) |
| Volume + / − | `PageUp` / `PageDown` |
| YouTube | `/` (slash commands) |
| Netflix | `Backspace` |
| Switch input | `Tab` |
| Mute | `Home` (start of line) |
| D-pad / Power | unchanged (Power suspends) |

Edit `contrib/90-chromecast-remote.hwdb` and re-run `./install.sh --force`, or apply live:

```bash
sudo systemd-hwdb update
sudo udevadm trigger --action=change /sys/class/input/eventNN
```

`hwdb` maps one keycode to one keycode — no combos. For `Ctrl+C`-style actions you need keyd or
a desktop shortcut (and keyd cannot address codes above 255 either).

## How the trigger works

atvvoice fires `--trigger-cmd` on every `Streaming` transition with `ATVVOICE_TRIGGER=start|stop`.
`contrib/handy-remote-trigger` then:

- reads Handy's real state from the last `TranscribeAction::start|stop called` line in
  `~/.local/share/com.pais.handy/logs/handy.log`, because `handy --toggle-transcription` is a
  bare toggle that drifts;
- no-ops when already in the requested state, **except** that a `start` restarts a recording it
  did not begin, so a press always means listen;
- serialises with `flock`, ignores a `start` older than five minutes, and confirms the toggle
  landed before returning;
- **recovers `DISPLAY`** when it is missing. This matters: the user service starts at boot before
  GNOME exports `DISPLAY`, and `handy --toggle-transcription` is a GUI binary that exits 1
  without an X display — the failure that silently breaks every press after a reboot;
- logs every decision to `~/.local/state/handy-remote/trigger.log`.

## Boot behaviour

The service starts at boot and logs `Waiting for device to connect…` until the remote wakes; a
BLE HID remote does not reconnect while asleep. The sink and remap are created only after
negotiation, so **the virtual mic does not exist until the remote has connected once**. Wake it
with any button, give it a second, and the first voice press may be consumed by connection setup.

## Troubleshooting

| Symptom | Cause | Check |
|---|---|---|
| No source, or silence | daemon not running / remote not connected | `systemctl --user status atvvoice`; `journalctl --user -u atvvoice -n 50` |
| Audio decodes but capture is silent | `Audio/Source` stream not scheduled | `pw-top` shows `QUANT 0`; the sink backend handles this |
| Handy does not see the mic | Handy only records the system default | `pactl get-default-source` |
| Press does nothing after a reboot | no `DISPLAY` when the service started | `tail ~/.local/state/handy-remote/trigger.log` for `display=...` |
| Press logged but no recording | wrapper could not reach Handy | same log: `toggle(start) failed` |
| Text not pasted | no typing tool, or a Wayland session | `command -v xdotool ydotool wtype`; `echo $XDG_SESSION_TYPE` |
| Device name truncated to one word | pipewire-pulse splits arguments on whitespace | use a hyphenated description |
| Audio clips | gain too high | compare peak vs 32768; lower `--gain` (default 14 dB) |
| Buttons unchanged | remote was not connected when hwdb was applied | reconnect, then `./install.sh --force` |
| Press/release inverted | stale Handy state | the wrapper self-corrects on the next press; see `trigger.log` |

## Wayland

**Half of this works unchanged, and the half that types does not.**

| Layer | Wayland status |
|---|---|
| atvvoice (BLE → PipeWire) | works — libpipewire + BlueZ, no X calls |
| udev hwdb remap | works — evdev level, below the compositor |
| Remote's HID keys (Enter, Esc, Super…) | works — real input events |
| `pactl set-default-source` | works |
| Handy recording | works |
| Handy **typing the transcript** | **needs a Wayland injector** |
| `handy-remote-trigger` | needs `WAYLAND_DISPLAY` support |

Wayland forbids one client from typing into another, so `xdotool` does nothing. Handy ships
`ydotool`, `wtype` and `wl-copy` backends; pick by compositor:

- **`wtype`** — needs `wlr-virtual-keyboard`; **wlroots only** (Sway, Hyprland). Not GNOME/Mutter.
- **`ydotool`** — injects through `uinput`, so it works everywhere, but needs `/dev/uinput`
  access, a running `ydotoold`, and a udev rule.
- **`wl-copy`** — only fills the clipboard; something still has to press Ctrl+V.

GNOME Wayland therefore means `ydotool` (or a libei / RemoteDesktop-portal implementation, which
Handy does not currently carry).

### Bringing up a Wayland machine

Inventory first:

```bash
echo "$XDG_SESSION_TYPE $XDG_CURRENT_DESKTOP $WAYLAND_DISPLAY"
loginctl show-session "$XDG_SESSION_ID" -p Type -p Desktop
command -v wtype ydotool ydotoold wl-copy
ls -l /dev/uinput
tail -20 ~/.local/share/com.pais.handy/logs/handy.log | grep -i "device resolve\|Using device"
```

Then:

1. Install the injector. On GNOME: `sudo apt-get install ydotool` (if unavailable, build from
   <https://github.com/ReimuNotMoe/ydotool>), enable `ydotoold`, and give the user access:
   ```bash
   echo 'KERNEL=="uinput", GROUP="input", MODE="0660", OPTIONS+="static_node=uinput"' |
     sudo tee /etc/udev/rules.d/60-uinput.rules
   sudo udevadm control --reload-rules && sudo udevadm trigger
   sudo usermod -aG input "$USER"      # re-login
   systemctl --user enable --now ydotoold 2>/dev/null || ydotoold &
   ```
2. Prove injection independently of Handy: open a text field and run
   `ydotool type "hello from ydotool"` (or `wtype "hello"` on wlroots).
3. Point Handy at that backend. Quit Handy, set `paste_method` in
   `~/.local/share/com.pais.handy/settings_store.json`, and restart it. `direct` uses the native
   tool chain; if the transcript lands but is not typed, try the clipboard method plus a
   compositor-side paste binding.
4. Install as usual: `./install.sh --dry-run`, then `./install.sh --set-default`.

### What still needs code

1. `contrib/handy-remote-trigger` recovers only `DISPLAY`; add `WAYLAND_DISPLAY` (and fall back
   to `XDG_SESSION_TYPE=wayland`) so the CLI can reach the running app on a pure-Wayland session.
2. `install.sh` installs `xdotool` unconditionally; branch on `XDG_SESSION_TYPE` and install
   `ydotool` (or `wtype`) instead, plus the `uinput` rule.
3. This document's dependency table should stop saying "X11 only" once verified.

### What to bring back

From the Wayland machine, so we can finish the support properly:

```bash
echo "$XDG_SESSION_TYPE $XDG_CURRENT_DESKTOP"
loginctl show-session "$XDG_SESSION_ID" -p Type -p Desktop
command -v ydotool wtype
ydotool type "test" && echo "ydotool ok"
tail -20 ~/.local/state/handy-remote/trigger.log
grep -iE "paste|wtype|ydotool|xdotool" ~/.local/share/com.pais.handy/logs/handy.log | tail
```

## Uninstall

```bash
./uninstall.sh
```

Stops and removes the service and binaries, removes the hwdb (unless `DO_HWDB=0`), removes the
wrapper state, and restores Handy's settings backup. The default input is left as-is.

## Known limitations

- The hwdb remap is specific to the Chromecast Voice Remote's scancodes.
- Handy has no per-device input picker; the system default is the only lever.
- `hwdb` cannot emit key combinations.
- The virtual mic exists only after the remote has connected once per boot.
- Wayland text injection is compositor-specific and unverified by us so far.

## Credits

Derived from [`b0o/atvvoice`](https://github.com/b0o/atvvoice) (MIT). Handy is
[`cjpais/Handy`](https://github.com/cjpais/Handy). This fork is MIT — see `LICENSE`.
