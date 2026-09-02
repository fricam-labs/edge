#!/bin/sh
set -eu

FFMPEG=/usr/lib/ffmpeg/7.0/bin/ffmpeg
DIRECT_URL='http://127.0.0.1:1984/api/stream.ts?src=giris'
CACHED_URL='http://127.0.0.1:8099/stream/giris.ts'

for mode in direct cached; do
  if [ "$mode" = direct ]; then url=$DIRECT_URL; else url=$CACHED_URL; fi
  echo "$mode"
  iteration=1
  while [ "$iteration" -le 5 ]; do
    start=$(date +%s%3N)
    timeout 8 "$FFMPEG" -hide_banner -loglevel error \
      -probesize 32768 -analyzeduration 100000 -flags low_delay \
      -i "$url" -frames:v 1 -f null - >/dev/null 2>&1
    end=$(date +%s%3N)
    echo "$((end-start))"
    iteration=$((iteration+1))
  done
done
