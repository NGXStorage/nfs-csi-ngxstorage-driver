#!/usr/bin/env bash
source "$CSI_TEST_ROOT/lib.sh"
set -euo pipefail
HOST_TEARDOWN_TIMEOUT=900

PODS=16
CONTAINERS=2
MOUNTS=5
TOTAL=$((PODS * CONTAINERS * MOUNTS))
SUFFIX="hc$(date +%m%d%H%M%S)"
NS="nfs-stress-$SUFFIX"
SC_FILE="$OUT/01-storageclasses.yaml"
PVC_FILE="$OUT/02-pvcs.yaml"
POD_FILE="$OUT/03-pods.yaml"
NS_FILE="$OUT/00-namespace.yaml"
IDENTITY_FILE="$OUT/host/nfs-identities.tsv"
PRE_FILE="$OUT/host/pre-delete-mounts.tsv"
mkdir -p "$OUT/host"

cleanup() {
    local rc=$? failed=0
    trap - EXIT
    [ ! -f "$POD_FILE" ] || oc delete -f "$POD_FILE" -n "$NS" --ignore-not-found=true --wait=true --timeout=300s >/dev/null 2>&1 || failed=1
    if [ "$HOST_TEARDOWN_PROVEN" = 1 ]; then
        oc delete -f "$PVC_FILE" -n "$NS" --ignore-not-found=true --wait=true --timeout=1800s >/dev/null 2>&1 || failed=1
        oc delete -f "$SC_FILE" --ignore-not-found=true --wait=true --timeout=300s >/dev/null 2>&1 || failed=1
        if [ "$failed" -eq 0 ]; then
            local deadline remaining
            deadline=$((SECONDS + 1800))
            while [ "$SECONDS" -lt "$deadline" ]; do
                remaining=$(oc get pv -o jsonpath='{range .items[*]}{.spec.claimRef.namespace}{"\n"}{end}' 2>/dev/null) || { failed=1; break; }
                if ! printf '%s\n' "$remaining" | grep -Fxq "$NS"; then break; fi
                sleep 5
            done
            printf '%s\n' "$remaining" | grep -Fxq "$NS" && failed=1
        fi
        if [ "$failed" -eq 0 ]; then
            oc delete "namespace/$NS" --ignore-not-found=true --wait=true --timeout=180s >/dev/null 2>&1 || failed=1
        fi
    elif [ "$PVC_APPLIED" = 1 ]; then
        # A backend share must not be deleted while host mount teardown is
        # unproven. Leave the exact namespace/resources for investigation.
        failed=1
        echo "[ERROR] host teardown not proven; preserving PVCs/StorageClasses in $NS" >&2
    fi
    if [ "$failed" -ne 0 ]; then
        touch "$OUT/.cleanup-failed"
        [ "$rc" -ne 0 ] || rc=1
    fi
    exit "$rc"
}
HOST_TEARDOWN_PROVEN=0
PVC_APPLIED=0
trap cleanup EXIT

# Re-name every generated object so this 160-volume run cannot reuse or
# delete another test's default-namespace resources.
sed -E "s/(nfs-csi-ngxstorage-[A-Za-z0-9-]+)/\\1-$SUFFIX/g" "$TEST_DIR/01-storageclasses.yaml" > "$SC_FILE"
sed -E "s/(nfs-csi-ngxstorage-[A-Za-z0-9-]+)/\\1-$SUFFIX/g; s/stress-p([0-9]+)/$SUFFIX-p\\1/g; s/namespace: default/namespace: $NS/g" \
    "$TEST_DIR/02-pvcs.yaml" > "$PVC_FILE"
sed -E "s/stress-p([0-9]+)/$SUFFIX-p\\1/g; s/namespace: default/namespace: $NS/g" \
    "$TEST_DIR/03-pods.yaml" > "$POD_FILE"
cat > "$NS_FILE" <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: $NS
EOF

oc apply -f "$NS_FILE" >/dev/null
oc apply -f "$SC_FILE" >/dev/null
oc apply -f "$PVC_FILE" -n "$NS" >/dev/null
PVC_APPLIED=1
log "created 16 StorageClasses and $TOTAL unique PVCs in isolated namespace=$NS"

deadline=$((SECONDS + 1800))
while [ "$SECONDS" -lt "$deadline" ]; do
    phases=$(oc get pvc -n "$NS" -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}') \
        || fail "PVC status API read failed"
    bound=$(printf '%s\n' "$phases" | awk '$0=="Bound"{n++} END{print n+0}')
    [ "$bound" -eq "$TOTAL" ] && break
    log "PVC provisioning: bound=$bound/$TOTAL"
    sleep 10
done
[ "$bound" -eq "$TOTAL" ] || fail "only $bound/$TOTAL PVCs reached Bound"

oc apply -f "$POD_FILE" -n "$NS" >/dev/null
deadline=$((SECONDS + 1200))
for i in $(seq 0 $((PODS - 1))); do
    pod="$SUFFIX-p$i"
    while [ "$SECONDS" -lt "$deadline" ]; do
        ready=$(oc get "pod/$pod" -n "$NS" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
        [ "$ready" = True ] && break
        sleep 3
    done
    [ "$ready" = True ] || fail "stress pod not Ready: $pod"
    log "pod Ready $((i + 1))/$PODS: $pod"
done

# Each container must read back the marker its generated command wrote on
# all five actual NFS mounts; this replaces the old /data false-positive.
for i in $(seq 0 $((PODS - 1))); do
    pod="$SUFFIX-p$i"
    for j in $(seq 0 $((CONTAINERS - 1))); do
        toc exec "$pod" -n "$NS" -c "app-$j" -- sh -c \
            "expected='ok-ngx-$i-$j'; for d in /data0 /data1 /data2 /data3 /data4; do test \"\$(cat \"\$d/write-test.txt\")\" = \"\$expected\" || exit 11; done" \
            || fail "NFS marker readback failed: pod=$pod container=app-$j"
    done
done
log "DATA INTEGRITY OK: all $TOTAL actual PVC mounts read back their markers"

# Resolve all 160 pod-volume mount targets before deleting any workload.
: > "$IDENTITY_FILE"
for i in $(seq 0 $((PODS - 1))); do
    pod="$SUFFIX-p$i"
    node=$(oc get "pod/$pod" -n "$NS" -o jsonpath='{.spec.nodeName}') || fail "pod node lookup failed: $pod"
    uid=$(oc get "pod/$pod" -n "$NS" -o jsonpath='{.metadata.uid}') || fail "pod UID lookup failed: $pod"
    [ -n "$node" ] && [ -n "$uid" ] || fail "pod identity empty: $pod"
    note_node "$node"
    for j in $(seq 0 $((CONTAINERS - 1))); do
        for k in $(seq 0 $((MOUNTS - 1))); do
            claim="$SUFFIX-p$i-c$j-m$k"
            pv=$(pv_for_pvc "$claim") || fail "PVC-to-PV lookup failed: $claim"
            [ -n "$pv" ] || fail "PVC not bound to PV: $claim"
            mount_path="/var/lib/kubelet/pods/$uid/volumes/kubernetes.io~csi/$pv/mount"
            printf '%s\t%s\t%s\t%s\t%s\n' "$pod" "$node" "$claim" "$pv" "$mount_path" >> "$IDENTITY_FILE"
        done
    done
done
[ "$(wc -l < "$IDENTITY_FILE")" -eq "$TOTAL" ] || fail "expected $TOTAL host mount identities"

# Capture NFS source and filesystem type while mounted, one read-only debug
# session per node. The post-delete check compares these exact mount targets.
: > "$PRE_FILE"
nodes=$(cut -f2 "$IDENTITY_FILE" | sort -u)
for node in $nodes; do
    host_cmd=""
    expected=0
    while IFS=$'\t' read -r pod host pvc pv mount_path; do
        [ "$host" = "$node" ] || continue
        printf -v step 'source=$(findmnt -rn -o SOURCE -M %s) || exit 71\nfstype=$(findmnt -rn -o FSTYPE -M %s) || exit 72\ncase "$fstype" in nfs|nfs4) ;; *) exit 73 ;; esac\nprintf "MOUNT_PRESENT|%s|%s|%%s|%%s\\n" "$source" "$fstype"\n' \
            "$mount_path" "$mount_path" "$pv" "$mount_path"
        host_cmd+="$step"
        expected=$((expected + 1))
    done < "$IDENTITY_FILE"
    pre=$(node_exec "$node" "set -eu
$host_cmd") || fail "pre-delete NFS mount inventory failed: node=$node"
    captured=$(printf '%s\n' "$pre" | grep -c '^MOUNT_PRESENT|' || true)
    [ "$captured" -eq "$expected" ] || fail "pre-delete mount evidence count mismatch: node=$node expected=$expected got=$captured"
    printf '%s\n' "$pre" >> "$PRE_FILE"
    log "captured $captured active NFS mount identities on $node"
done

# NFS does not create VolumeAttachments; prove none of this workload's PVs is
# still attached, then check host mountinfo directly after normal Pod deletion.
oc delete -f "$POD_FILE" -n "$NS" --wait=true --timeout=600s >/dev/null
deadline=$((SECONDS + 300))
while [ "$SECONDS" -lt "$deadline" ]; do
    attachments=$(oc get volumeattachments -o jsonpath='{range .items[*]}{.spec.source.persistentVolumeName}{"\n"}{end}') \
        || fail "VolumeAttachment API read failed"
    attached=0
    while IFS=$'\t' read -r _ _ _ pv _; do
        [ -n "$pv" ] || continue
        printf '%s\n' "$attachments" | grep -Fxq "$pv" && attached=$((attached + 1))
    done < "$IDENTITY_FILE"
    [ "$attached" -eq 0 ] && break
    sleep 3
done
[ "$attached" -eq 0 ] || fail "$attached workload PVs still have VolumeAttachments"

for node in $nodes; do
    host_cmd=""
    expected=0
    while IFS=$'\t' read -r pod host pvc pv mount_path; do
        [ "$host" = "$node" ] || continue
        printf -v step 'if [ ! -r /proc/self/mountinfo ]; then exit 81; fi\nif awk -v target=%s "\$5==target {found=1} END {exit(found?0:1)}" /proc/self/mountinfo; then exit 82; else rc=$?; [ "$rc" -eq 1 ] || exit 83; fi\necho "MOUNT_ABSENT|%s|%s"\n' \
            "$mount_path" "$pv" "$mount_path"
        host_cmd+="$step"
        expected=$((expected + 1))
    done < "$IDENTITY_FILE"
    host_cmd+="echo MOUNTS_ABSENT=$expected"
    result=$(node_exec "$node" "set -eu
$host_cmd") || fail "host NFS teardown check failed: node=$node"
    printf '%s\n' "$result" > "$OUT/host/post-delete-$node.txt"
    grep -Fxq "MOUNTS_ABSENT=$expected" "$OUT/host/post-delete-$node.txt" || fail "host teardown count missing: node=$node"
    absent=$(grep -c '^MOUNT_ABSENT|' "$OUT/host/post-delete-$node.txt" || true)
    [ "$absent" -eq "$expected" ] || fail "host teardown mount count mismatch: node=$node expected=$expected got=$absent"
    log "HOST NFS TEARDOWN OK: node=$node mounts_absent=$absent"
done
HOST_TEARDOWN_PROVEN=1
log "HOST NFS TEARDOWN PASS: all $TOTAL mounts absent after deleting $PODS pods"
log "stress PVC and NFS mount evidence retained under $OUT/host"
