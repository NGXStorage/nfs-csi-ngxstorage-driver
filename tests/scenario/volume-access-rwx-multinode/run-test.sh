#!/usr/bin/env bash
source "$CSI_TEST_ROOT/lib.sh"
set -euo pipefail
trap 'rc=$?; run_test_cleanup || true; exit $rc' EXIT
# Render the manifests to the temporary copy first: the node pins below are
# substituted on the copy, never in place, so the source fixtures keep their
# __NODE_A__/__NODE_B__ placeholders for the next run.
render_test_dir
# Ensure the authorized namespace exists before the first namespaced apply
# (this scenario applies SC/PVC directly, not via apply_wait_pod).
ensure_namespace "" || fail "namespace hazırlanamadı (fail-closed): $NS"
POD_A_FILE=$(ls "$RENDER_DIR"/*pod-a.yaml | head -1)
POD_B_FILE=$(ls "$RENDER_DIR"/*pod-b.yaml | head -1)
POD_A=$(pod_from_manifest "$POD_A_FILE")
POD_B=$(pod_from_manifest "$POD_B_FILE")
[ -n "$POD_A" ] && [ -n "$POD_B" ] || fail "rwx-multinode pod manifestleri eksik"
# Pin the two pods to two distinct Ready workers.
mapfile -t READY_NODES < <(oc get nodes --no-headers | awk '$2=="Ready"{print $1}' | awk '/worker/{print}' | head -2)
[ "${#READY_NODES[@]}" -ge 2 ] || mapfile -t READY_NODES < <(oc get nodes --no-headers | awk '$2=="Ready"{print $1}' | head -2)
[ "${#READY_NODES[@]}" -ge 2 ] || fail "iki Ready worker bulunamadı"
# Evidence: record whether the rendered copies still carry the substitution
# placeholders BEFORE substituting — a literal/stale worker pin makes the
# substitution a silent no-op, which is exactly the mismatch we must not pass.
pin_a_src=$(grep -c '__NODE_A__' "$POD_A_FILE" 2>/dev/null || true)
pin_b_src=$(grep -c '__NODE_B__' "$POD_B_FILE" 2>/dev/null || true)
log "RWX substitution ön-kontrol: pod-a __NODE_A__=$pin_a_src pod-b __NODE_B__=$pin_b_src (0 = literal/farklı node, no-op riski)"
render_substitute "__NODE_A__" "${READY_NODES[0]}"
render_substitute "__NODE_B__" "${READY_NODES[1]}"
# The rendered nodeSelector must equal the two selected (distinct) Ready
# workers; read back from the render output so a stale/literal fixture can
# never yield a silent RWX pass.
verify_two_node_placement "$POD_A_FILE" "$POD_B_FILE" "${READY_NODES[0]}" "${READY_NODES[1]}"
note_node "${READY_NODES[0]}"; note_node "${READY_NODES[1]}"
log "RWX node pin: $POD_A -> ${READY_NODES[0]}, $POD_B -> ${READY_NODES[1]}"
oc apply -f "$RENDER_DIR"/*-sc.yaml -f "$RENDER_DIR"/*-pvc.yaml -n "$NS" >/dev/null
wait_test_pvcs_bound
oc apply -f "$RENDER_DIR"/*pod-a.yaml -f "$RENDER_DIR"/*pod-b.yaml -n "$NS" >/dev/null
wait_pod_ready "$POD_A" 600
wait_pod_ready "$POD_B" 600
# Cross-node RWX: a writes, b reads+writes, a reads back.
toc exec "$POD_A" -n "$NS" -- sh -c "echo node-a > $MOUNT_PATH/cross.txt && sync"
got=$(toc exec "$POD_B" -n "$NS" -- sh -c "cat $MOUNT_PATH/cross.txt" 2>/dev/null || true)
[ "$got" = "node-a" ] || fail "b, a'nın yazdığını okuyamadı: $got"
toc exec "$POD_B" -n "$NS" -- sh -c "echo node-b > $MOUNT_PATH/cross2.txt && sync"
got2=$(toc exec "$POD_A" -n "$NS" -- sh -c "cat $MOUNT_PATH/cross2.txt" 2>/dev/null || true)
[ "$got2" = "node-b" ] || fail "a, b'nin yazdığını okuyamadı: $got2"
log "RWX MULTI-NODE DOĞRULANDI: iki worker arasında çift yönlü yaz/oku"
