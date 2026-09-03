#!/bin/sh
set -eu

INSTALL_DIR="${FRICAM_EDGE_INSTALL_DIR:-/opt/fricam-edge}"
COMPOSE_FILE="${FRICAM_EDGE_COMPOSE_FILE:-$INSTALL_DIR/compose.yml}"
IMAGE="${FRICAM_EDGE_IMAGE:-ghcr.io/fricam-labs/edge:stable}"
CONTAINER="${FRICAM_EDGE_CONTAINER:-fricam-edge}"
LOCK_FILE="${FRICAM_EDGE_LOCK_FILE:-/run/lock/fricam-edge-update.lock}"
HEALTH_TIMEOUT="${FRICAM_EDGE_HEALTH_TIMEOUT:-90}"

log() {
    printf '%s fricam-edge-update: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

if [ "$(id -u)" -ne 0 ]; then
    log "must run as root"
    exit 1
fi
for command in docker flock date install sleep; do
    command -v "$command" >/dev/null 2>&1 || {
        log "required command not found: $command"
        exit 1
    }
done
test -r "$COMPOSE_FILE" || {
    log "compose file not found: $COMPOSE_FILE"
    exit 1
}
docker compose version >/dev/null 2>&1 || {
    log "Docker Compose v2 is required"
    exit 1
}

install -d -m 0755 "$(dirname "$LOCK_FILE")"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    log "another update is already running"
    exit 0
fi

compose() {
    docker compose --project-directory "$INSTALL_DIR" -f "$COMPOSE_FILE" "$@"
}

container_image_id() {
    docker inspect --format '{{.Image}}' "$CONTAINER" 2>/dev/null || true
}

image_id() {
    docker image inspect --format '{{.Id}}' "$IMAGE" 2>/dev/null || true
}

wait_healthy() {
    elapsed=0
    while [ "$elapsed" -lt "$HEALTH_TIMEOUT" ]; do
        state="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$CONTAINER" 2>/dev/null || true)"
        case "$state" in
            healthy|running) return 0 ;;
            unhealthy|exited|dead) return 1 ;;
        esac
        sleep 2
        elapsed=$((elapsed + 2))
    done
    return 1
}

old_image="$(container_image_id)"
log "checking $IMAGE"
compose pull fricam-edge
new_image="$(image_id)"
test -n "$new_image" || {
    log "pulled image cannot be inspected"
    exit 1
}

if [ -n "$old_image" ] && [ "$old_image" = "$new_image" ]; then
    if wait_healthy; then
        log "already current and healthy ($new_image)"
        exit 0
    fi
    log "current container is not healthy; recreating it"
    compose up --detach --no-build --remove-orphans
    if wait_healthy; then
        log "current image recovered after recreation"
        exit 0
    fi
    log "current image remains unhealthy; manual intervention required"
    exit 1
fi

log "deploying $new_image"
if compose up --detach --no-build --remove-orphans && wait_healthy; then
    version="$(docker inspect --format '{{index .Config.Labels "org.opencontainers.image.version"}}' "$CONTAINER" 2>/dev/null || true)"
    log "update healthy${version:+ (version $version)}"
    exit 0
fi

log "new container failed its health check"
if [ -z "$old_image" ]; then
    log "no previous image is available for rollback"
    exit 1
fi

log "rolling back to $old_image"
docker image tag "$old_image" "$IMAGE"
compose up --detach --no-build --remove-orphans
if wait_healthy; then
    log "rollback healthy; retaining failed image for diagnostics"
else
    log "rollback also failed; manual intervention required"
fi
exit 1
