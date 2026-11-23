#!/bin/bash
# NGX NFS CSI — sustained data-path IO test (40G worker NIC focused).
#
# Runs concurrent dd streams against every PVC exposed by stress pods. Two
# modes:
#
#   low-io   — 4K blocks, continuous overwrite of the SAME file per mount
#              (io.bin) so PVCs never fill; sustained small-block IO.
#   high-io  — large sequential blocks (default 1M) writing sized files
#              (io_<size>.bin) per mount; measures bulk throughput.
#   stop     — kills all local running IO streams (leaves written files).
#   clear-files — removes IO test files (io.bin, dd_*.bin, etc.) from every
#                 /dataN mount to reset the matrix PVCs for a fresh run.
#   cleanup  — deletes stress pods, PVCs, and StorageClasses.
#
# The test discovers stress pods and their /dataN mounts dynamically and runs
# one stream per mount, so 16 pods x 10 mounts = 160 concurrent streams.
#
# Usage:
# ./io-stress.bash low-io   [--duration N] [--cleanup]
# ./io-stress.bash high-io  [--size 15G] [--cleanup]
# ./io-stress.bash stop
# ./io-stress.bash clear-files
# ./io-stress.bash cleanup
#
# Env: KUBECONFIG must point at the cluster. Defaults assume the stress
# parameter matrix pods (stress-pN) are running in the default namespace.

set -u

MODE="${1:-low-io}"
DURATION="${DURATION:-60}"      # low-io: seconds to run (default 1 min)
SIZE="${SIZE:-1G}"             # high-io: per-mount file size
BLOCK_SIZE_LOW="${BLOCK_SIZE_LOW:-4k}"
BLOCK_SIZE_HIGH="${BLOCK_SIZE_HIGH:-1M}"
KEEP_PODS="${KEEP_PODS:-0}"    # 1 = don't delete stress pods/pvcs/scs

: "${KUBECONFIG:?set KUBECONFIG from the runtime cluster profile}"
export KUBECONFIG
WORKDIR="$HOME/.cache/opencode/tmp/ngxstorage-nfs-stress"
case "$WORKDIR" in /tmp|/tmp/*|/var/tmp|/var/tmp/*) echo "refusing scratch under /tmp or /var/tmp" >&2; exit 2 ;; esac
LOG_DIR="$WORKDIR/logs"

die() { echo "ERROR: $*" >&2; exit 1; }

# stress_pods prints the running stress pod names.
stress_pods() {
    oc get pods --no-headers 2>/dev/null \
        | awk '$1 ~ /^stress-p[0-9]+$/ && $3 ~ /Running/ {print $1}' \
        | sort -u
}

# pod_mounts POD prints "<container> <mountName> <mountPath>" for /dataN mounts.
pod_mounts() {
    local pod="$1"
    oc get pod "$pod" -o jsonpath='{range .spec.containers[*]}{.name}{"\n"}{range .volumeMounts[*]}{.name}|{.mountPath}{"\n"}{end}{end}' 2>/dev/null \
        | awk -F'|' '
            /^[^|]+$/ { c=$1; next }
            NF==2 && $2 ~ /^\/data[0-9]+$/ { print c, $1, $2 }
        '
}

ensure_pods() {
    local pods
    pods=$(stress_pods)
    if [ -z "$pods" ]; then
        die "no running stress-p* pods found in default namespace; run examples/benchmark-stress-parameters/generate-benchmark-stress-parameters.bash first"
    fi
}

run_low_io() {
    ensure_pods
    mkdir -p "$LOG_DIR"
    echo ">>> LOW-IO: 4K continuous same-file overwrite on all PVCs ($DURATION s)"
    local count=0
    local pods
    pods=$(stress_pods)
    for pod in $pods; do
        while read -r container vol mount; do
            [ -z "$mount" ] && continue
            (
                end=$(( $(date +%s) + DURATION ))
                while [ "$(date +%s)" -lt "$end" ]; do
                    oc exec "$pod" -c "$container" -- \
                        sh -c "dd if=/dev/zero of=$mount/io.bin bs=$BLOCK_SIZE_LOW count=256 conv=fdatasync,notrunc 2>/dev/null"
                done
 ) > "$LOG_DIR/${pod}_${container}_$(echo "$vol"|tr -d .).log" 2>&1 &
            count=$((count+1))
        done < <(pod_mounts "$pod")
    done
    echo ">>> launched $count concurrent 4K streams (Ctrl-C / wait $DURATION s)"
    wait
    echo ">>> low-io test complete: $count streams, $DURATION s"
}

run_high_io() {
    ensure_pods
    mkdir -p "$LOG_DIR"
    # Convert human size to bytes for dd count
    local count_bytes
    count_bytes=$(numfmt --from=iec "$SIZE" 2>/dev/null || echo 0)
    [ "$count_bytes" -eq 0 ] && die "invalid SIZE $SIZE"
    echo ">>> HIGH-IO: $SIZE sequential writes ($BLOCK_SIZE_HIGH blocks) on all PVCs"
    local count=0
    local pods
    pods=$(stress_pods)
    for pod in $pods; do
        while read -r container vol mount; do
            [ -z "$mount" ] && continue
            (
                oc exec "$pod" -c "$container" -- \
                    sh -c "dd if=/dev/zero of=$mount/io_$SIZE.bin bs=$BLOCK_SIZE_HIGH count=$((count_bytes/$(numfmt --from=iec "$BLOCK_SIZE_HIGH"))) conv=fdatasync 2>&1"
 ) > "$LOG_DIR/${pod}_${container}_$(echo "$vol"|tr -d .).log" 2>&1 &
            count=$((count+1))
        done < <(pod_mounts "$pod")
    done
    echo ">>> launched $count concurrent $SIZE writes; waiting..."
    wait
    echo ">>> high-io test complete: $count streams of $SIZE"
    # Summarize throughput
    grep -h "bytes.*copied" "$LOG_DIR"/*.log 2>/dev/null | sed 's/^/  /' | head -20
    echo ">>> full logs in $LOG_DIR"
}

cleanup() {
    if [ "$KEEP_PODS" = "1" ]; then
        echo ">>> keeping stress resources (KEEP_PODS=1)"
        return
    fi
    echo ">>> deleting stress pods, pvcs, storageclasses"
    oc delete pod --all --wait=false --ignore-not-found >/dev/null 2>&1
    oc delete pvc --all --wait=false --ignore-not-found >/dev/null 2>&1
    oc delete sc --all --ignore-not-found >/dev/null 2>&1
}

# Stop all running IO streams (oc exec dd processes on this host). Does not
# delete stress pods/PVCs — only kills the local driver streams. Files already
# written (io.bin / io_<size>.bin) remain on the volumes.
stop_all() {
    echo ">>> stopping all running IO streams (oc exec dd)..."
    pkill -f "oc exec" 2>/dev/null
    pkill -f "while true; do dd" 2>/dev/null
    sleep 2
    local left
    left=$(ps aux 2>/dev/null | grep "oc exec" | grep -v grep | wc -l)
    echo ">>> remaining oc exec streams: $left"
    if [ "$left" -gt 0 ]; then
        pkill -9 -f "oc exec" 2>/dev/null
        sleep 1
        left=$(ps aux 2>/dev/null | grep "oc exec" | grep -v grep | wc -l)
        echo ">>> after force kill: $left"
    fi
}

# clear_files deletes the IO test files (io.bin, io_*.bin, ddtest.bin,
# dd_stress-*.bin, dd_15g_*.bin, rand.bin, f15g.bin, marker.txt) from every
# /dataN mount so the matrix PVCs are clean for a fresh run. Runs one rm per
# mount via oc exec. Use after `stop` to reset the volumes.
clear_files() {
    ensure_pods
    echo ">>> clearing IO test files from all /dataN mounts..."
    local count=0
    local pods
    pods=$(stress_pods)
    for pod in $pods; do
        while read -r container vol mount; do
            [ -z "$mount" ] && continue
            oc exec "$pod" -c "$container" -- \
                sh -c "rm -f $mount/io.bin $mount/io_*.bin $mount/ddtest.bin $mount/dd_stress-*.bin $mount/dd_15g_*.bin $mount/rand.bin $mount/f15g.bin $mount/marker.txt 2>/dev/null" \
                >/dev/null 2>&1
            count=$((count+1))
        done < <(pod_mounts "$pod")
    done
    echo ">>> cleared files from $count mounts"
}

case "$MODE" in
    low-io)     run_low_io ;;
    high-io)    run_high_io ;;
    stop)       stop_all ;;
    clear-files) clear_files ;;
    cleanup)    cleanup ;;
    *) die "usage: $0 {low-io|high-io|stop|clear-files|cleanup} [--duration N] [--size N] [--cleanup]" ;;
esac
