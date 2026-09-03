# Fricam Edge

Open-source acceleration service for the Fricam Android client and self-hosted
Frigate servers.

## Install

On the host running Frigate:

```sh
curl -fsSL https://fricam.app/edge/compose.yml | docker compose -f - up -d
```

Then select that server profile in Fricam and open **Settings → Fricam Edge**.
Run one Edge instance beside each Frigate server. Fricam stores pairing material
per server profile, so multi-server setups remain isolated.

The sidecar discovers every enabled Frigate camera, resolves its
preferred live stream, keeps that go2rtc MPEG-TS stream warm, and stores only the
latest decodable GOP in RAM. A new client receives that GOP immediately and then
continues on the live stream. It writes no camera media to disk.

Discovery runs every 30 seconds by default. Newly enabled cameras are added and
removed or disabled cameras are stopped without restarting the sidecar. `HD` is
the preferred Frigate live-stream label. If it stalls, the sidecar automatically
rotates through that camera's other configured live streams and finally the
camera-named go2rtc stream. The last valid in-memory GOP remains available while
the source reconnects, and is replaced as soon as the fallback produces a new
keyframe.

Endpoints bind to port 8099. The Compose file exposes this on the private
LAN/Tailscale interfaces so the Android app can auto-discover it:

- `GET /health`
- `GET /metrics`
- `GET /stream/<camera-name>.ts`
- `GET /webrtc?src=<stream-name>` (paired WebSocket signaling)

The sidecar supports H.264 and H.265 streams advertised in the MPEG-TS PMT. It uses
Docker host networking to reach the loopback-only go2rtc API in the existing
Frigate container. Do not port-forward 8099 to the public internet.

For remote access it opens one outbound `wss://relay.fricam.app` control
connection. The Worker forwards only bounded SDP/ICE JSON. Camera media travels
over DTLS-SRTP through a direct WebRTC path or Cloudflare TURN fallback; the
Worker and TURN service cannot decrypt it. No Cloudflare Tunnel, inbound public
port, or cloud media storage is used. Android pairs while on the LAN through
`POST /pair`, whose Frigate bearer token is validated only against the private
loopback Frigate HTTPS endpoint.

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

## License

MIT
