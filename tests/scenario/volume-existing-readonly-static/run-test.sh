#!/usr/bin/env bash
source "$CSI_TEST_ROOT/lib.sh"
set -euo pipefail
trap 'rc=$?; run_test_cleanup || true; exit $rc' EXIT
EX_ID=$(cat "$OUT/existing-volume.id")
sdk() { "$NGX_SDK_CLI" --controllers "$NGX_CONTROLLER" --api-key "$NGX_API_KEY" --pool-name "$NGX_POOL_NAME" "$@"; }
# Phase A: publish once while the share is still writable so the driver
# creates the PV child directory.
apply_wait_pod
POD_RO=$(pod_from_manifest "$(ls "$RENDER_DIR"/*-pod*.yaml | head -1)")
wait_pod_ready "$POD_RO" 600
log "FAZ A: writable share üzerinde PV child dizini oluşturuldu"
sdk set-share-readonly "$EX_ID" true
oc delete pod "$POD_RO" -n "$NS" --wait=true >/dev/null
oc apply -f "$RENDER_DIR"/*pod*.yaml -n "$NS" >/dev/null
wait_pod_ready "$POD_RO" 600
log "FAZ B: read-only export üzerine recursive mount başarılı"
wout=""
for attempt in $(seq 1 12); do
    wout=$(toc exec "$POD_RO" -n "$NS" -- sh -c 'echo rox-test > /data/rox-write-test' 2>&1 || true)
    printf '%s' "$wout" | grep -qi "read-only" && break
    sleep 5
done
printf '%s' "$wout" | grep -qi "read-only" || fail "read-only static mount yazmaya izin verdi: $wout"
log "VOLUME-EXISTING-READONLY-STATIC OK: yazma reddedildi"
