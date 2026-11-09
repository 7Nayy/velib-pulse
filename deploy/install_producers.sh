#!/usr/bin/env bash
# Install the producers as systemd services on the VM that hosts them (VM2).
# Usage (from the repo root, as a sudoer):
#   sudo ./deploy/install_producers.sh            # Kafka mode
#   sudo ARCHIVE_ONLY=1 ./deploy/install_producers.sh   # before Kafka exists
set -euo pipefail

APP=/opt/velib-pulse
ENV_DIR=/etc/velib-pulse
DATA=/var/lib/velib-pulse/archive

id velib &>/dev/null || useradd --system --home "$APP" --shell /usr/sbin/nologin velib
mkdir -p "$APP" "$ENV_DIR" "$DATA"
cp -r producers "$APP/"
python3 -m venv "$APP/.venv"
"$APP/.venv/bin/pip" install -q --upgrade pip
"$APP/.venv/bin/pip" install -q -r producers/requirements.txt

if [[ ! -f "$ENV_DIR/producers.env" ]]; then
  cp .env.example "$ENV_DIR/producers.env"
  sed -i "s|^ARCHIVE_DIR=.*|ARCHIVE_DIR=$DATA|" "$ENV_DIR/producers.env"
fi
chown -R velib:velib "$APP" "$DATA"
chmod 640 "$ENV_DIR/producers.env"; chgrp velib "$ENV_DIR/producers.env"

for svc in velib-producer weather-producer; do
  cp "deploy/systemd/$svc.service" /etc/systemd/system/
  if [[ "${ARCHIVE_ONLY:-0}" == "1" ]]; then
    sed -i 's|\(ExecStart=.*\.py\)$|\1 --archive-only|' "/etc/systemd/system/$svc.service"
  fi
done
systemctl daemon-reload
systemctl enable --now velib-producer weather-producer
systemctl --no-pager status velib-producer weather-producer | head -20
echo "Logs: journalctl -u velib-producer -f"
