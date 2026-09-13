# Installing beside a Frigate Home Assistant add-on

The [main install](../README.md#install) assumes a Linux Docker host where
you already run Frigate yourself, and Edge joins it with `network_mode:
host` to reach the loopback-only Frigate API (`127.0.0.1:5000`) and go2rtc
(`127.0.0.1:1984`). If Frigate instead runs as a Home Assistant **add-on**,
there's no such host to `docker compose` into by default, since Supervisor
owns the add-on container lifecycle. This page covers getting Edge running
next to that add-on anyway.

## The constraint that decides everything

Edge only talks to Frigate and go2rtc over loopback, in the same network
namespace as those services. That means:

- Edge **must run on the same machine** that hosts the Frigate add-on, not
  on a different machine running your primary Home Assistant instance.
- Your Frigate add-on needs host networking enabled for Edge to reach it
  over `127.0.0.1` at all. Check **Settings → Add-ons → Frigate →
  Configuration** in Home Assistant; Frigate generally wants this anyway for
  camera/go2rtc discovery, but confirm it before proceeding.

## Two install paths

### Home Assistant Supervised (a Debian host you manage)

You already have normal SSH/Docker access to the host underneath Supervisor.
Follow the [main install steps](../README.md#install) as-is. Adjust
`FRIGATE_URL` / `GO2RTC_URL` in `compose.yml` if your Frigate add-on
publishes different loopback ports than the defaults
(`127.0.0.1:5000` and `127.0.0.1:1984`).

### Home Assistant OS (HAOS appliance)

HAOS doesn't expose a shell or `docker` by default; Supervisor mediates
everything through add-ons. Getting Edge running here means getting direct
access to the underlying host's Docker daemon, which is an
**advanced/unsupported** but well-known path in the Home Assistant
community:

1. Install the **Advanced SSH & Web Terminal** add-on (or **Terminal & SSH**
   with Protection mode turned off). Protection mode off is required - it's
   what exposes the real host shell and `docker` CLI instead of the
   sandboxed Supervisor CLI.
2. SSH into the host (or use the web terminal) and confirm Docker access:
   ```sh
   docker ps
   ```
   You should see the Frigate add-on's container listed.
3. Run the installer, same as any other Docker host:
   ```sh
   curl -fsSL https://github.com/fricam-labs/edge/releases/latest/download/install.sh | sudo sh
   ```
4. Verify Frigate/go2rtc are reachable before trusting the container to come
   up healthy:
   ```sh
   curl -s http://127.0.0.1:5000/api/version
   curl -s http://127.0.0.1:1984/api/config
   ```
   If either fails, the Frigate add-on isn't on host networking - fix that
   first, in the Frigate add-on config, not Edge.

Keep in mind: the Edge container runs outside Supervisor's management. It
won't show up in the Add-ons UI or in Supervisor's automatic backups, and
Supervisor won't restart it - the `fricam-edge-update.timer` systemd unit
from the installer handles that instead, same as any other install. A HAOS
system upgrade shouldn't touch containers Supervisor doesn't own, but this
isn't a configuration HAOS testing specifically covers, so it's worth a
quick recheck after major HAOS upgrades.

If you'd rather not run an unmanaged container on a HAOS appliance, run
Frigate as a standalone Docker Compose service on separate hardware (a NAS
or mini PC) instead of as an HA add-on, and connect it to Home Assistant
through the Frigate integration instead of the add-on. That's the install
target the main README already covers.

## After install

Pairing and remote access work exactly like the standard install:

- Keep your phone on the same Wi-Fi as the Frigate + Edge machine for the
  one-time pairing (**Settings → Fricam Edge → Pair**).
- After pairing, remote access goes through Edge's outbound relay
  connection - no inbound port, no tunnel, and no need to proxy Frigate
  through another Home Assistant instance's external domain.
- Don't port-forward 8099, or any Frigate/go2rtc port, to the public
  internet. That's what Edge's relay path replaces.
