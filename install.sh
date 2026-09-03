#!/bin/sh
set -eu

RELEASE_BASE="${FRICAM_EDGE_RELEASE_BASE:-https://github.com/fricam-labs/edge/releases/latest/download}"
INSTALL_DIR="${FRICAM_EDGE_INSTALL_DIR:-/opt/fricam-edge}"
UPDATE_BIN="${FRICAM_EDGE_UPDATE_BIN:-/usr/local/libexec/fricam-edge-update}"
SYSTEMD_DIR="${FRICAM_EDGE_SYSTEMD_DIR:-/etc/systemd/system}"

if [ "$(id -u)" -ne 0 ]; then
    echo "fricam-edge: run the installer as root (for example: curl ... | sudo sh)" >&2
    exit 1
fi
for command in curl docker sha256sum install mktemp systemctl; do
    command -v "$command" >/dev/null 2>&1 || {
        echo "fricam-edge: required command not found: $command" >&2
        exit 1
    }
done
docker compose version >/dev/null 2>&1 || {
    echo "fricam-edge: Docker Compose v2 is required" >&2
    exit 1
}

temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT HUP INT TERM

assets="compose.yml update.sh fricam-edge-update.service fricam-edge-update.timer SHA256SUMS"
for asset in $assets; do
    curl --fail --location --silent --show-error \
        "$RELEASE_BASE/$asset" --output "$temporary_dir/$asset"
done

(
    cd "$temporary_dir"
    sha256sum --check SHA256SUMS
)

install -d -m 0755 "$INSTALL_DIR" "$(dirname "$UPDATE_BIN")" "$SYSTEMD_DIR"
install -m 0644 "$temporary_dir/compose.yml" "$INSTALL_DIR/compose.yml"
install -m 0755 "$temporary_dir/update.sh" "$UPDATE_BIN"
install -m 0644 "$temporary_dir/fricam-edge-update.service" \
    "$SYSTEMD_DIR/fricam-edge-update.service"
install -m 0644 "$temporary_dir/fricam-edge-update.timer" \
    "$SYSTEMD_DIR/fricam-edge-update.timer"

systemctl daemon-reload
systemctl enable --now fricam-edge-update.timer
systemctl start fricam-edge-update.service

echo "fricam-edge: installed; automatic stable-channel updates are enabled"
