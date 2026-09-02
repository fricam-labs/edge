#!/bin/sh
set -eu

FFMPEG=/usr/lib/ffmpeg/7.0/bin/ffmpeg
METRICS_URL=http://127.0.0.1:8099/metrics
CAMERAS=$(python3 -c "import json,urllib.request; print(' '.join(json.load(urllib.request.urlopen('$METRICS_URL')).keys()))")

for camera in $CAMERAS; do
  start=$(date +%s%3N)
  rc=0
  timeout 10 "$FFMPEG" -hide_banner -loglevel error \
    -probesize 32768 -analyzeduration 100000 -flags low_delay \
    -i "http://127.0.0.1:8099/stream/$camera.ts" \
    -frames:v 1 -f null - >/dev/null 2>&1 || rc=$?
  end=$(date +%s%3N)
  echo "$camera first-frame=$((end-start))ms rc=$rc"
  if [ "$rc" -ne 0 ]; then exit "$rc"; fi
done
