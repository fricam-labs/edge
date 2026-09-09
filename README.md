# Fricam Edge

[![Tests](https://github.com/fricam-labs/edge/actions/workflows/container.yml/badge.svg?branch=main&event=push)](https://github.com/fricam-labs/edge/actions/workflows/container.yml?query=branch%3Amain)

Open-source acceleration service for the Fricam Android client and self-hosted
Frigate servers.

## Install

On a Linux host with Docker Engine, Docker Compose v2, curl, and systemd:

```sh
curl -fsSL https://github.com/fricam-labs/edge/releases/latest/download/install.sh | sudo sh
```

The idempotent installer stores the Compose project in `/opt/fricam-edge`,
deploys the current `stable` image, and enables a daily update timer with a
randomized maintenance window. An update is accepted only after the container
passes its health check; otherwise the previous local image is restored.

Inspect or trigger updates with:

```sh
systemctl status fricam-edge-update.timer
sudo systemctl start fricam-edge-update.service
journalctl -u fricam-edge-update.service
```

For a NAS or another host without systemd, download `compose.yml` and
`update.sh` from the latest GitHub release, store them together, and run
`update.sh` from that platform's scheduler. The standard manual Compose update
remains `docker compose pull fricam-edge && docker compose up -d --no-build`.
Watchtower is intentionally not bundled: it is unmaintained and would give a
general-purpose container access to the Docker daemon.

Then select that server profile in Fricam and open **Settings → Fricam Edge**.
Run one Edge instance beside each Frigate server. Fricam stores pairing material
per server profile, so multi-server setups remain isolated.

The sidecar discovers every enabled Frigate camera and keeps an economical
go2rtc stream warm, storing its latest decodable GOP in RAM. A new client
receives the cached startup frame and continues watching live video while HD
is prepared. It writes no camera media to disk.

`WARM_POLICY=safe` is the default. Only configured RTSP passthrough and
FFmpeg copy-only sources are automatically warmed. A native grid/substream is
preferred when available; labels rank sources but do not establish their cost.
Encoder sources, custom FFmpeg templates, ambiguous sources, and alias cycles
are excluded, including from automatic fallback. This prevents camera discovery
from starting simultaneous full-resolution H.265-to-H.264 encoders.

Current Android and iOS clients negotiate `progressive_video` with two video
tracks in one WebRTC connection. HD starts after the first startup frame is
displayed and an `edge/upgrade` request arrives. The startup decoder remains
visible until the client acknowledges a decoded HD frame with
`edge/quality-ready`. HD failure retains/restores the startup stream; pause and
disconnect release the HD consumer. Idle prewarmed connections do not keep an
expensive HD producer running. `MAX_HD_STREAMS=1` limits distinct demand encoder
sources; viewers of the same source share a lease. Native sources already
classified as safe do not consume this encoder budget. This is a source-count
limit, not a claim that one arbitrary 4K encoder fits every GPU.

`GET /capabilities` reports camera plans, rejected warm sources, and HD leases.
`GET /health` advertises `progressive_video`; older clients retain their existing
protocol. A configured camera without an eligible warm source remains in
discovery/metrics with `warm_status=no_copy_only_warm_source`, but does not
start an encoder in the background. Active WebRTC viewing then requires a
demand source and cannot offer the same cold-start latency. For fast startup on
every camera, configure a native H.264 substream. H.265 passthrough can serve
legacy TS, but cannot bootstrap the H.264-only WebRTC bridge.

For hardware that has been measured to support a particular economical
transcoded substream, explicitly opt it in with a camera-to-configured-stream
JSON map, for example `WARM_SOURCE_OVERRIDES={"front":"front_sub"}`. The
override never falls back to another source and its cost must be budgeted by
the operator. `WARM_POLICY=all` restores legacy eager warming and can saturate
the GPU. Neither option is enabled automatically.

Discovery runs every 30 seconds by default. Newly enabled cameras are added and
removed or disabled cameras are stopped without restarting the sidecar. `HD` is
the preferred Frigate live-stream label. If it stalls, the sidecar automatically
rotates through that camera's eligible warm sources. While on a fallback stream it retries the preferred
stream every `PREFERRED_RETRY_SEC` (default 120, backing off up to 8x when the
preferred stream keeps failing), so a go2rtc restart does not leave cameras pinned
to a transcoded fallback. The last valid in-memory GOP remains available while
the source reconnects, and is replaced as soon as the fallback produces a new
keyframe.

Endpoints bind to port 8099. The Compose file exposes this on the private
LAN/Tailscale interfaces so the Android app can auto-discover it:

- `GET /health`
- `GET /metrics`
- `GET /stream/<camera-name>.ts`
- `GET /webrtc?src=<stream-name>` (paired WebSocket signaling)
- `GET /webrtc/progressive?src=<stream-name>` (paired progressive video)
- `GET /capabilities` (warm policy and HD lease diagnostics)

The LAN MPEG-TS endpoint supports H.264 and H.265 streams advertised in the PMT.
Remote WebRTC is intentionally H.264-only so an incompatible camera fails during
negotiation instead of producing a long black-screen timeout. For an H.265 camera,
prefer a native H.264 substream for warm startup, with a go2rtc H.264 restream
(for example `ffmpeg:<source>#video=h264`) as the HD viewing source if needed.
Do not eagerly warm a set of full-resolution encoders. The sidecar uses Docker
host networking to reach the loopback-only go2rtc API in the existing Frigate
container. Do not port-forward 8099 to the public internet.

For remote access it opens one outbound `wss://relay.fricam.app` control
connection. The Worker forwards only bounded SDP/ICE JSON. Camera media travels
over DTLS-SRTP through a direct WebRTC path or Cloudflare TURN fallback; the
Worker and TURN service cannot decrypt it. No Cloudflare Tunnel, inbound public
port, or cloud media storage is used. Android pairs while on the LAN through
`POST /pair`, whose Frigate bearer token is validated only against the private
loopback Frigate HTTPS endpoint.

The same authenticated outbound connection carries Fricam Remote Access when
the configured Frigate address is unreachable. Only bounded `/api/*` requests
from an entitled, paired client are accepted; login, configuration writes,
path traversal, and request bodies above 256 KiB are rejected. Request metadata
and streamed response chunks use AES-256-GCM between the Android app and the
user's Edge container, so Cloudflare routes ciphertext without reading API
responses, snapshots, or recordings. A working LAN, VPN, or external address
remains preferred and does not consume the separate Remote Access allowance.

Two-way audio uses the same Edge contract on every network. On the LAN the app
opens the authenticated `/webrtc` WebSocket directly; away from home it opens
the same logical session through the managed relay and uses P2P first with TURN
fallback. For remote talk, the self-hosted Edge process terminates DTLS-SRTP and
bridges PCMA RTP over loopback into the camera's go2rtc backchannel. This is
required for TURN compatibility with go2rtc's fixed ICE listener. Cloudflare
still sees only encrypted packets; plaintext audio exists only in the user's
self-hosted Edge process and camera path. It is never cached, recorded, or sent
to the Fricam Worker.

`e2e/` contains the relay-only regression client used to verify the managed
entitlement, Cloudflare TURN route, and a real `_talk` backchannel. It accepts
the Personal Pro test entitlement only over stdin and never stores it.

The runtime is a statically linked Go binary in a non-root distroless container.
The image has no shell, package manager, Python runtime, or writable root filesystem.

`benchmark.sh` compares direct and cached startup latency. `validate-all.sh`
discovers every active cache endpoint and decodes one frame from each.
Use `GET /capabilities` to verify the progressive policy and camera source plan
before enabling remote access for users.

## License

MIT
