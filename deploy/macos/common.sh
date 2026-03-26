#!/usr/bin/env bash
set -euo pipefail

MACOS_DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VIDEO_PROCESS_ROOT="$(cd "$MACOS_DEPLOY_DIR/../.." && pwd)"
CONSTRUCTURE_ROOT="$(cd "$VIDEO_PROCESS_ROOT/.." && pwd)"

SSH_KEY="${SSH_KEY:-/home/taiwei/.ssh/id_mini_wenjie}"
KNOWN_HOSTS="${KNOWN_HOSTS:-/tmp/vp_mac_known_hosts}"
MAIN_HOST="${MAIN_HOST:-192.168.20.4}"

MAC1_TARGET="${MAC1_TARGET:-wenjieliu@10.0.0.127}"
MAC2_TARGET="${MAC2_TARGET:-magi2@10.0.0.128}"
MAC3_TARGET="${MAC3_TARGET:-magi1@10.0.0.126}"

log_section() {
  echo "== $* =="
}

ssh_run() {
  local target="$1"
  shift
  ssh -i "$SSH_KEY" \
    -o BatchMode=yes \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile="$KNOWN_HOSTS" \
    "$target" "$@"
}

rsync_push() {
  local source="$1"
  local target="$2"
  local dest="$3"
  rsync -az --delete \
    --exclude '.venv/' \
    --exclude '__pycache__/' \
    -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=$KNOWN_HOSTS" \
    "$source" "$target:$dest"
}

install_user_runtime() {
  local target="$1"
  local need_ffmpeg="${2:-false}"
  log_section "install_user_runtime $target (ffmpeg=$need_ffmpeg)"
  ssh_run "$target" "bash -lc '
    set -euo pipefail
    mkdir -p \$HOME/.local/bin \$HOME/Library/LaunchAgents \$HOME/Library/Logs/constructure \$HOME/Constructure \$HOME/ConstructureData
    export PATH=\$HOME/.local/bin:\$PATH
    if ! command -v uv >/dev/null 2>&1; then
      curl -LsSf https://astral.sh/uv/install.sh | sh
      export PATH=\$HOME/.local/bin:\$PATH
    fi
    uv python install 3.12
    ffmpeg_ok=false
    if [ \"$need_ffmpeg\" = true ] && command -v ffmpeg >/dev/null 2>&1; then
      if ffmpeg -hide_banner -encoders 2>/dev/null | grep -q videotoolbox; then
        ffmpeg_ok=true
      fi
    fi
    if [ \"$need_ffmpeg\" = true ] && [ \"\$ffmpeg_ok\" != true ]; then
      tmpdir=\$(mktemp -d)
      curl -L --fail --silent --show-error https://www.osxexperts.net/ffmpeg80arm.zip -o \$tmpdir/ffmpeg.zip
      curl -L --fail --silent --show-error https://www.osxexperts.net/ffprobe80arm.zip -o \$tmpdir/ffprobe.zip
      unzip -oq \$tmpdir/ffmpeg.zip -d \$HOME/.local/bin
      unzip -oq \$tmpdir/ffprobe.zip -d \$HOME/.local/bin
      chmod +x \$HOME/.local/bin/ffmpeg \$HOME/.local/bin/ffprobe
      xattr -dr com.apple.quarantine \$HOME/.local/bin/ffmpeg \$HOME/.local/bin/ffprobe >/dev/null 2>&1 || true
      rm -rf \$tmpdir
    fi
    if [ \"$need_ffmpeg\" = true ]; then
      ffmpeg -hide_banner -encoders 2>/dev/null | grep -q videotoolbox
    fi
  '"
}
