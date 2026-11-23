#!/usr/bin/env bash
source "$CSI_TEST_ROOT/lib.sh"
set -euo pipefail
trap 'rc=$?; run_test_cleanup || true; exit $rc' EXIT
# RWX share with three pods: pod1 and pod3 mount writable, pod2 read-only.
# The engine verifies the matrix itself instead of trusting the fixtures.
apply_wait_pod
P1=volume-access-readonly-volume-access-readwrite-multipod-pod1
P2=volume-access-readonly-volume-access-readwrite-multipod-pod2
P3=volume-access-readonly-volume-access-readwrite-multipod-pod3
wait_pod_ready "$P1"
wait_pod_ready "$P2"
wait_pod_ready "$P3"
oc exec "pod/$P1" -n "$NS" -- sh -c "echo $MARKER >/data/from-pod1.txt && sync"
oc exec "pod/$P2" -n "$NS" -- cat /data/from-pod1.txt | grep -q "$MARKER" \
    || fail "pod2 (ro) pod1'in yazdığını okuyamadı"
# An allowed write must FAIL here (readOnly mount): any success is a real
# violation. The failure text varies by kernel/busybox, so the exit status
# is the assertion, and a successful write attempt is proven by absence of
# the file afterwards.
write_rc=0
oc exec "pod/$P2" -n "$NS" -- sh -c "echo x >/data/ro-write" 2>/dev/null || write_rc=$?
[ "$write_rc" -ne 0 ] || fail "pod2 write kabul edildi (ro mount ihlali)"
file_rc=0
oc exec "pod/$P2" -n "$NS" -- sh -c "test -e /data/ro-write" 2>/dev/null && file_rc=0 || file_rc=1
[ "$file_rc" -ne 0 ] || fail "pod2 ro mount'ta dosya oluştu"
log "pod2 ro mount doğrulandı (write reddedildi, dosya oluşmadı)"
log "pod2 ro mount doğrulandı (read ok, write reject)"
oc exec "pod/$P3" -n "$NS" -- sh -c "echo $MARKER-p3 >/data/from-pod3.txt && sync"
oc exec "pod/$P1" -n "$NS" -- cat /data/from-pod3.txt | grep -q "$MARKER-p3" \
    || fail "pod1 (rw) pod3'ün yazdığını okuyamadı (RWX cross-write)"
log "RWX MULTI-POD OK: iki yazar + bir okur, cross-write ve ro-reject doğrulandı"
