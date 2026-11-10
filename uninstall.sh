#!/usr/bin/env bash
# Remove the atvvoice service, binaries and (optionally) the button remap.
set -euo pipefail

PREFIX="${PREFIX:-$HOME/.local}"
DO_HWDB="${DO_HWDB:-1}"

say() { printf '\n==> %s\n' "$*"; }

say "stopping and disabling atvvoice"
systemctl --user disable --now atvvoice 2>/dev/null || true
rm -f "${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/atvvoice.service"
systemctl --user daemon-reload 2>/dev/null || true

say "removing binaries and state"
rm -f "$PREFIX/bin/atvvoice" "$PREFIX/bin/handy-remote-trigger"
rm -rf "${XDG_STATE_HOME:-$HOME/.local/state}/handy-remote"

if ((DO_HWDB)); then
    say "removing the button remap"
    sudo rm -f /etc/udev/hwdb.d/90-chromecast-remote.hwdb
    sudo systemd-hwdb update
    sudo udevadm trigger --action=change --subsystem-match=input || true
fi

backup="$HOME/.local/share/com.pais.handy/settings_store.json.bak-atvvoice"
if [[ -f "$backup" ]]; then
    say "restoring Handy settings from $backup"
    cp "$backup" "${backup%.bak-atvvoice}"
fi

cat <<'EOF'

Removed. The default input was left as-is; point it back at your laptop mic with:
  pactl set-default-source <your-laptop-mic>
EOF
