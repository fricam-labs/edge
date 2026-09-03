#!/bin/sh
set -eu

repository_dir="$(cd -- "$(dirname "$0")" && pwd)"
test_dir="$(mktemp -d)"
trap 'rm -rf "$test_dir"' EXIT HUP INT TERM
mkdir -p "$test_dir/bin" "$test_dir/install"
: >"$test_dir/install/compose.yml"

cat >"$test_dir/bin/id" <<'EOF'
#!/bin/sh
echo 0
EOF
cat >"$test_dir/bin/date" <<'EOF'
#!/bin/sh
echo 2026-01-01T00:00:00Z
EOF
cat >"$test_dir/bin/sleep" <<'EOF'
#!/bin/sh
exit 0
EOF
cat >"$test_dir/bin/docker" <<'EOF'
#!/bin/sh
set -eu
case "$1 $2" in
    "compose version") exit 0 ;;
    "inspect --format")
        case "$3" in
            *'.Image'*) cat "$TEST_STATE/current" ;;
            *'.State.Health'*) cat "$TEST_STATE/health" ;;
            *) echo v-test ;;
        esac
        ;;
    "image inspect") cat "$TEST_STATE/tag" ;;
    "image tag") printf '%s\n' "$3" >"$TEST_STATE/tag" ;;
    *)
        case "$*" in
            *" pull fricam-edge") exit 0 ;;
            *" up --detach --no-build --remove-orphans")
                tag="$(cat "$TEST_STATE/tag")"
                printf '%s\n' "$tag" >"$TEST_STATE/current"
                if [ -f "$TEST_STATE/fail-new" ] && [ "$tag" = sha256:new ]; then
                    echo unhealthy >"$TEST_STATE/health"
                else
                    echo healthy >"$TEST_STATE/health"
                fi
                ;;
            *) echo "unexpected docker command: $*" >&2; exit 1 ;;
        esac
        ;;
esac
EOF
chmod 0755 "$test_dir/bin/id" "$test_dir/bin/date" "$test_dir/bin/sleep" "$test_dir/bin/docker"

run_update() {
    PATH="$test_dir/bin:$PATH" \
    TEST_STATE="$test_dir" \
    FRICAM_EDGE_INSTALL_DIR="$test_dir/install" \
    FRICAM_EDGE_LOCK_FILE="$test_dir/update.lock" \
    FRICAM_EDGE_HEALTH_TIMEOUT=4 \
        "$repository_dir/update.sh"
}

echo sha256:old >"$test_dir/current"
echo sha256:new >"$test_dir/tag"
echo healthy >"$test_dir/health"
run_update
test "$(cat "$test_dir/current")" = sha256:new

echo sha256:old >"$test_dir/current"
echo sha256:new >"$test_dir/tag"
echo healthy >"$test_dir/health"
: >"$test_dir/fail-new"
if run_update; then
    echo "expected unhealthy update to fail" >&2
    exit 1
fi
test "$(cat "$test_dir/current")" = sha256:old
test "$(cat "$test_dir/health")" = healthy

echo "update tests passed"
