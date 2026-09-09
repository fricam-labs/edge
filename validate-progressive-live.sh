#!/bin/sh
# Run on the Frigate Docker host after loading fricam-edge:progressive-dev.
# This bounded validation always restores the original Edge container, including
# failures, SSH disconnects, or an explicit stop of the validation container.
set -eu
candidate=fricam-edge-progressive-validation
original=fricam-edge
monitor_image=$(docker inspect frigate -f '{{.Config.Image}}')
docker inspect "$original" >/dev/null
if docker inspect "$candidate" >/dev/null 2>&1; then
  echo 'Validation container already exists; refusing to replace it.' >&2
  exit 1
fi
restore() {
  docker rm -f "$candidate" >/dev/null 2>&1 || true
  docker start "$original" >/dev/null
  echo 'Original Edge restored.'
}
trap restore EXIT HUP INT TERM
docker stop "$original" >/dev/null
docker run -d --name "$candidate" --network host --read-only --cap-drop ALL \
  --security-opt no-new-privileges -v fricam_edge_identity:/data:ro \
  -e LISTEN_ADDR=0.0.0.0:8099 -e EDGE_RELAY_URL=wss://relay.fricam.app \
  -e FRIGATE_URL=http://127.0.0.1:5000 -e GO2RTC_URL=http://127.0.0.1:1984 \
  -e WARM_SOURCE_OVERRIDES="${VALIDATION_WARM_SOURCE_OVERRIDES:-}" \
  -e WARM_POLICY=safe -e MAX_HD_STREAMS=1 fricam-edge:progressive-dev >/dev/null
attempt=0
while [ "$attempt" -lt 60 ]; do
  sleep 10
  [ "$(docker inspect -f '{{.State.Running}}' "$candidate" 2>/dev/null)" = true ] || break
  docker run --rm --network host --entrypoint python3 "$monitor_image" -c '
import json,urllib.request
s=json.load(urllib.request.urlopen("http://127.0.0.1:5000/api/stats",timeout=5))
h=json.load(urllib.request.urlopen("http://127.0.0.1:8099/health",timeout=5))
gpu=s.get("gpu_usages",{})
print(json.dumps({"edge":h,"gpu":gpu,"detectors":{k:{"inference_speed":v.get("inference_speed")} for k,v in s.get("detectors",{}).items()},"cameras":{k:{"camera_fps":v.get("camera_fps"),"process_fps":v.get("process_fps"),"skipped_fps":v.get("skipped_fps")} for k,v in s.get("cameras",{}).items()}}))
for value in gpu.values():
 usage=float(str(value.get("gpu","0")).rstrip("%"))
 if usage>=85: raise SystemExit("GPU threshold reached; restoring baseline")
'
  attempt=$((attempt+1))
done
