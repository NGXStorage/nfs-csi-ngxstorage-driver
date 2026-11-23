#!/usr/bin/env bash
# Driver-owned live scenario entry point.
# Usage: BENCH=ocp2|k8s bash tests/scenario/run.sh <test|all> [--keep] [--include-disruptive] [--include-stress]
set -uo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
DRIVER_TESTS=$(cd "$ROOT/.." && pwd)
DRIVER_ROOT=$(cd "$DRIVER_TESTS/.." && pwd)
PROTOCOL="$(basename "$DRIVER_ROOT" | tr '[:upper:]' '[:lower:]')"
source "$ROOT/lib.sh"
[[ "${ALLOW_LIVE:-0}" == "1" ]] || { echo "run live scenarios through tests/run-all-tests.sh --allow-live" >&2; exit 2; }

BENCH=${BENCH:-ocp2}
TEST=${1:?usage: tests/scenario/run.sh <test|all> [--keep] [--include-disruptive] [--include-stress]}
KEEP=0
INCLUDE_DISRUPTIVE=${INCLUDE_DISRUPTIVE:-0}
INCLUDE_STRESS=${INCLUDE_STRESS:-0}
for arg in "$@"; do
    case "$arg" in
        --keep) KEEP=1 ;;
        --include-disruptive) INCLUDE_DISRUPTIVE=1 ;;
        --include-stress) INCLUDE_STRESS=1 ;;
    esac
done
export INCLUDE_DISRUPTIVE INCLUDE_STRESS

# Disruptive/stress gate enforced by this real executor, so direct invocation
# (named reboot/stress scenario or `all`) cannot bypass the campaign policy of
# tests/run-all-tests.sh. Named invocations are refused; campaign runs skip.
gate_scenario() { # $1 = scenario name
    case "$1" in
        volume-recovery-host-reboot)
            [ "$INCLUDE_DISRUPTIVE" = 1 ] || {
                echo "refusing $1: it reboots a worker node; pass --include-disruptive (or INCLUDE_DISRUPTIVE=1) only after an explicit operator decision" >&2
                return 2; } ;;
        benchmark-stress-*)
            [ "$INCLUDE_STRESS" = 1 ] || {
                echo "refusing $1: long-running stress parameter sweep; pass --include-stress (or INCLUDE_STRESS=1)" >&2
                return 2; } ;;
    esac
    return 0
}
gate_scenario "$TEST" || exit $?

case "$BENCH" in ocp2|k8s) ;; *) echo "unsupported bench: $BENCH (ocp2|k8s)" >&2; exit 2 ;; esac

KUBE_BIN="${KUBE_BIN:-$( [[ "$BENCH" == ocp2 ]] && echo oc || echo kubectl )}"
NGX_API_KEY="${STORAGE_API_KEY:-${NGX_API_KEY:-}}"
: "${NGX_CONTROLLER:?set NGX_CONTROLLER in the runtime environment}"
: "${NGX_API_KEY:?set STORAGE_API_KEY in the runtime environment}"
: "${NGX_POOL_NAME:?set NGX_POOL_NAME in the runtime environment}"
NGX_SDK_CLI="${NGX_SDK_CLI:-$DRIVER_TESTS/tools/bin/ngxstorage-cli}"
command -v "$KUBE_BIN" >/dev/null || { echo "$KUBE_BIN is required" >&2; exit 127; }
[ -x "$NGX_SDK_CLI" ] || { echo "NGX_SDK_CLI is not executable" >&2; exit 2; }

export CSI_TEST_ROOT="$ROOT"
export CSI_DRIVER_TESTS="$DRIVER_TESTS"
cache_root="$HOME/.cache/ngxstorage"
case "$cache_root" in /tmp|/tmp/*|/var/tmp|/var/tmp/*) echo "refusing temporary cache under /tmp or /var/tmp" >&2; exit 2 ;; esac
export TMPDIR="$cache_root"
export CSI_TEST_CACHE="$cache_root/ngxstorage-csi-${PROTOCOL}"
mkdir -p "$CSI_TEST_CACHE"
export KUBE_BIN PROTOCOL BENCH KEEP TEST_NS=${NAMESPACE:-default}
export NGX_CONTROLLER NGX_API_KEY NGX_POOL_NAME NGX_DATA_IP NGX_SDK_CLI CSI_TEST_ROOT CSI_DRIVER_TESTS CSI_TEST_CACHE TMPDIR

run_one() {
    TEST_DIR="$ROOT/$1"
    TEST_NAME=$1
    [ -f "$TEST_DIR/run-test.sh" ] || { echo "test not found: $TEST_DIR" >&2; return 2; }

    NS=$TEST_NS
    STAMP=$(date +%F-%H%M%S)
    # $STAMP alone has second resolution — two runs of the same test in
    # the same second would share OUT sentinels (.ns-owned, .render-collected,
    # .cleanup-failed). The runner pid makes OUT run-unique.
    OUT="$TEST_DIR/logs/$BENCH-$STAMP-$$"
    mkdir -p "$OUT"
    START=$(date -u +%FT%TZ)
    MARKER="SCEN-$TEST_NAME-$$-$(date +%s)"
    # Run identity: one fresh render dir per run, exported so every pre/run/
    # post/cleanup process renders into the SAME directory. Repeated runs of the
    # same TEST_NAME get distinct identities and never share stale manifests;
    # parallel runs never collide. The scratch lives under $TMPDIR (not a RAM
    # tmpfs) and is collected at the end of the run unless --keep retains it.
    RENDER_DIR="$(mktemp -d "$TMPDIR/csi-scenario-render.XXXXXX")"
    RENDER_ID="$(basename "$RENDER_DIR")"
    export TEST_NAME TEST_DIR NS OUT START STAMP MARKER RENDER_DIR RENDER_ID

    # Each test logs into its own file through a subshell redirect so
    # campaign runs never bleed output between tests.
    (
        echo "[runner] protocol=$PROTOCOL test=$TEST_NAME bench=$BENCH stamp=$STAMP"
        local test_rc=0
        backend_snapshots start
        if [ -f "$TEST_DIR/pre-test.sh" ]; then
            bash "$TEST_DIR/pre-test.sh" || test_rc=$?
        fi
        if [ "$test_rc" -eq 0 ]; then
            bash "$TEST_DIR/run-test.sh" || test_rc=$?
        fi
        # post-test.sh is teardown: it must run after a failure too, or
        # cluster state (taints, RO shares, backend volumes) leaks into the
        # next run.
        # NFS runner: TEST_DIR IS the source dir (RENDER_DIR is a separate
        # scratch copy), so post-test.sh survives run_test_cleanup's render
        # collection. Run it from TEST_DIR — never from an inherited SRC_DIR
        # (undefined here; the snapshot-runner SRC_DIR contract is FC/iSCSI).
        if [ -f "$TEST_DIR/post-test.sh" ]; then
            local cleanup_rc=0
            bash "$TEST_DIR/post-test.sh" || cleanup_rc=$?
            if [ "$cleanup_rc" -ne 0 ]; then
                echo "[ERROR] teardown başarısız (rc=$cleanup_rc)"
                [ "$test_rc" -eq 0 ] && test_rc=$cleanup_rc
            fi
        fi
        exit "$test_rc"
    ) >> "$OUT/run.log" 2>&1
    rc=$?
    if [ "$rc" -eq 0 ] && [ -f "$OUT/.cleanup-failed" ]; then
        echo "[runner] cleanup başarısız; senaryo PASS olarak kabul edilmiyor" | tee -a "$OUT/run.log" >&2
        rc=1
    fi

    collect_logs
    local status=FAIL
    [ $rc -eq 0 ] && status=PASS
    if [ "$rc" -eq 0 ] && grep -q 'SKIP:' "$OUT/run.log" 2>/dev/null; then
        status=SKIPPED
    fi

    {
        echo
        echo "## $BENCH / $STAMP"
        echo
        echo "- Result: **$status** (rc=$rc)"
        echo "- Start: $START"
        echo "- Log: logs/$BENCH-$STAMP/"
    } >> "$TEST_DIR/result.md"

    # Collect this run's render scratch here (run.sh is the run owner) so it is
    # reclaimed even when run-test.sh never ran (e.g. pre-test failed). Cleanup
    # does the same via collect_render_scratch when it runs; the $OUT sentinel
    # makes the collection at-most-once. --keep retains the rendered evidence.
    collect_render_scratch
    echo "[runner] $TEST_NAME result=$status ($BENCH/$STAMP)"
    return $rc
}

if [ "$TEST" = all ]; then
    overall=0
    for dir in "$ROOT"/*/; do
        [ -f "$dir/run-test.sh" ] || continue
        # benchmark-stress is a long-running parameter sweep and host-reboot
        # reboots a node; both stay out of the campaign unless explicitly
        # included, mirroring tests/run-all-tests.sh.
        case "$(basename "$dir")" in
            benchmark-stress-*)
                [ "$INCLUDE_STRESS" = 1 ] || { echo "[skip] $(basename "$dir") (use --include-stress)"; continue; } ;;
            volume-recovery-host-reboot)
                [ "$INCLUDE_DISRUPTIVE" = 1 ] || { echo "[skip] $(basename "$dir") (use --include-disruptive)"; continue; } ;;
        esac
        run_one "$(basename "$dir")" || overall=$?
    done
    exit $overall
fi

run_one "$TEST"
exit $?
