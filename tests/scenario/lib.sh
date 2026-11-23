# shellcheck shell=bash
# Scenario helpers owned by this CSI driver. Sourced by scenario/run.sh; test scripts
# inherit PROTOCOL, TEST_NAME, TEST_DIR, NS, OUT, START, KEEP, KUBE_BIN and
# the NGX_* backend variables. Scripts must NOT be sourced standalone.

KUBE_BIN="${KUBE_BIN:-oc}"
CSI_TEST_ROOT="${CSI_TEST_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
MOUNT_PATH="${MOUNT_PATH:-/data}"
OC_EXEC_TIMEOUT="${OC_EXEC_TIMEOUT:-30}"
DETACH_TIMEOUT="${DETACH_TIMEOUT:-300}"
SNAPSHOT_TIMEOUT="${SNAPSHOT_TIMEOUT:-900}"
CLEANUP_TIMEOUT="${CLEANUP_TIMEOUT:-600}"
EXPECTED_TRAN="${EXPECTED_TRAN:-${PROTOCOL:-}}"
KEEP="${KEEP:-0}"

oc() { command "$KUBE_BIN" "$@"; }
# timeout cannot exec shell functions; call the underlying kube binary.
toc() { timeout "$OC_EXEC_TIMEOUT" "$KUBE_BIN" "$@"; }

log()  { echo "[$(date +%T)] $*"; }
fail() { echo "[ERROR] $*" >&2; exit 1; }

# Locate the live controller pod and temporarily place the test-only csi-race
# binary in the shared /csi emptyDir. The helper is deliberately never baked
# into a production driver image. Copying through stdin avoids relying on tar
# being present in the minimal runtime image (`oc cp` would require it).
csi_race_prepare() {
    local binary="${CSI_TEST_CACHE:?}/csi-race" controller_ns pod container
    if [ -n "${CSI_PREBUILT_DIR:-}" ]; then
        # Explicit verified-prebuilt entry (fail-closed, no silent fallback);
        # env unset keeps the original go-build path below unchanged.
        local prebuilt_lib
        prebuilt_lib="$(cd "$(dirname "${BASH_SOURCE[0]}")/../sanity/cloud" && pwd)/prebuilt.sh"
        [ -r "$prebuilt_lib" ] || fail "verified-prebuilt helper missing: $prebuilt_lib"
        # shellcheck disable=SC1090
        source "$prebuilt_lib"
        csi_prebuilt_stage csi-race "$binary" \
            || fail "csi-race verified-prebuilt rejected or staging failed"
    elif [ ! -x "$binary" ]; then
        mkdir -p "$CSI_TEST_CACHE"
        (cd "$CSI_DRIVER_TESTS/tools/csi-race" && go build -o "$binary" .) \
            || fail "driver-local csi-race build failed"
    fi
    controller_ns="${CSI_DRIVER_NAMESPACE:-ngxstorage-${PROTOCOL}}"
    container="${PROTOCOL}-csi-ngxstorage"
    pod=$(oc get pods -n "$controller_ns" \
        -l "app.kubernetes.io/name=${PROTOCOL}-csi-ngxstorage,app.kubernetes.io/component=controller" \
        -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}' 2>/dev/null | head -1)
    [ -n "$pod" ] || fail "${PROTOCOL} controller pod bulunamadı: namespace=$controller_ns"
    if ! "$KUBE_BIN" exec -i -n "$controller_ns" "$pod" -c "$container" -- \
        sh -c 'umask 077; cat > /csi/csi-race && chmod 0700 /csi/csi-race' < "$binary"; then
        fail "csi-race test binary controller /csi volume'una aktarılamadı"
    fi
    CSI_RACE_CONTROLLER_NS="$controller_ns"
    CSI_RACE_CONTROLLER_POD="$pod"
    CSI_RACE_CONTROLLER_CONTAINER="$container"
    export CSI_RACE_CONTROLLER_NS CSI_RACE_CONTROLLER_POD CSI_RACE_CONTROLLER_CONTAINER
    log "csi-race hazır: pod=$pod container=$container (test-only /csi emptyDir)"
}

csi_race_exec() {
    [ -n "${CSI_RACE_CONTROLLER_POD:-}" ] || csi_race_prepare
    "$KUBE_BIN" exec -n "$CSI_RACE_CONTROLLER_NS" "$CSI_RACE_CONTROLLER_POD" \
        -c "$CSI_RACE_CONTROLLER_CONTAINER" -- /csi/csi-race "$@"
}

csi_race_cleanup() {
    [ -n "${CSI_RACE_CONTROLLER_POD:-}" ] || return 0
    "$KUBE_BIN" exec -n "$CSI_RACE_CONTROLLER_NS" "$CSI_RACE_CONTROLLER_POD" \
        -c "$CSI_RACE_CONTROLLER_CONTAINER" -- rm -f /csi/csi-race >/dev/null 2>&1 || true
    unset CSI_RACE_CONTROLLER_NS CSI_RACE_CONTROLLER_POD CSI_RACE_CONTROLLER_CONTAINER
}

note_node() {
    [ -n "${1:-}" ] || return 0
    touch "$OUT/.nodes"
    grep -Fxq "$1" "$OUT/.nodes" 2>/dev/null || echo "$1" >> "$OUT/.nodes"
}

wait_pod_ready() { # $1 = pod adı, $2 = süre (s), $3 = transient FailedAttach grace (s)
    log "pod bekleniyor: $1 (≤${2:-600}s)"
    local started=$SECONDS deadline=$((SECONDS + ${2:-600})) state events attach_grace=${3:-0}
    while [ "$SECONDS" -lt "$deadline" ]; do
        state=$(oc get "pod/$1" -n "$NS" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
        [ "$state" = True ] && return 0
        # A pod applied before its PVC binds emits a transient "PVC is not
        # bound" FailedMount; apply_wait_pod already gates on binding, so drop
        # those lines before treating the rest as fatal.
        events=$(oc describe "pod/$1" -n "$NS" 2>/dev/null | grep -viE "not bound|unbound immediate" | tail -40 || true)
        # During a deliberate RWO cross-node handoff, attach/detach-controller
        # can briefly emit Multi-Attach from its informer cache even after the
        # old VolumeAttachment object is gone and node unstage completed. Keep
        # every other fatal event fail-fast, but allow callers a bounded grace
        # for FailedAttachVolume only.
        local fatal_events="$events"
        if [ "$attach_grace" -gt 0 ] && [ $((SECONDS - started)) -lt "$attach_grace" ]; then
            fatal_events=$(printf '%s\n' "$fatal_events" | grep -v 'FailedAttachVolume' || true)
        fi
        if printf '%s\n' "$fatal_events" | grep -qE 'FailedAttachVolume|FailedMount|FailedMapVolume|FailedUnmapVolume|ProvisioningFailed|BackOff|CrashLoopBackOff|ErrImagePull|ImagePullBackOff'; then
            printf '%s\n' "$events"
            fail "pod volume hazırlığı başarısız: $1"
        fi
        sleep 2
    done
    oc describe "pod/$1" -n "$NS" 2>/dev/null | tail -40
    fail "pod Ready olmadı: $1"
}

write_marker() { # $1 = pod adı
    # shellcheck disable=SC2153
    log "veri yaz: marker=$MARKER -> $MOUNT_PATH (pod=$1)"
    # NFS mount'larda `sync` takılabilir; timeout ile sınırla, fail etmesin.
    if toc exec "$1" -n "$NS" -- sh -c \
        "mkdir -p $MOUNT_PATH && echo '$MARKER' > $MOUNT_PATH/verify.txt && (timeout 15 sync || true)" 2>/dev/null; then
        return 0
    fi
    case "$TEST_NAME" in
        *readonly*) log "READ-ONLY DOĞRULANDI: $MOUNT_PATH yazma reddedildi (beklenen)"; return 1 ;;
        *) return 2 ;;
    esac
}

read_marker() { # $1 = pod adı, $2 = beklenen marker
    log "veri oku + doğrula (pod=$1)"
    local got
    got=$(toc exec "$1" -n "$NS" -- sh -c "cat $MOUNT_PATH/verify.txt" 2>/dev/null || true)
    [ "$got" = "$2" ] || fail "veri bütünlüğü BOZUK: okunan=$got beklenen=$2"
    log "VERİ BÜTÜNLÜĞÜ OK: $2 (yazıldı ve okundu)"
}

pod_from_manifest() { # $1 = pod yaml dosyası
    [ -f "$1" ] && grep -m1 '  name:' "$1" | awk '{print $2}'
}

pv_for_pvc() { # $1 = PVC adı
    oc get "pvc/$1" -n "$NS" -o jsonpath='{.spec.volumeName}' 2>/dev/null
}

wait_volume_detached() { # $1 = PV adı, $2 = süre (s)
    local pv="$1" deadline=$((SECONDS + ${2:-$DETACH_TIMEOUT})) attachments
    log "VolumeAttachment detach bekleniyor: pv=$pv (≤${2:-$DETACH_TIMEOUT}s)"
    while [ "$SECONDS" -lt "$deadline" ]; do
        # An API failure is not evidence that the attachment disappeared.
        attachments=$(oc get volumeattachments -o jsonpath='{range .items[*]}{.spec.source.persistentVolumeName}{"\n"}{end}') \
            || return 1
        if ! printf '%s\n' "$attachments" | grep -Fxq "$pv"; then
            log "VolumeAttachment detached: pv=$pv"
            return 0
        fi
        sleep 2
    done
    oc get volumeattachments -o wide 2>/dev/null | grep -F "$pv" || true
    return 1
}

# Create a host-visible debug pod on a node. oc debug node/<n> and the
# sysadmin profile behave differently across oc/kubectl versions (and oc
# 4.20 fails to create the pod entirely), so build the pod explicitly:
# privileged-equivalent host mounts give chroot /host access.
# Prints the pod name; removes nothing.
node_debug_pod() { # $1 = node name
    local node="$1" name="node-dbg-${RANDOM}${RANDOM}"
    if [ "${KUBE_BIN##*/}" = "oc" ]; then
        # oc debug node/<n> runs server-side and grants the privileged SCC to
        # its pod (full capabilities, chroot works) even though the pod spec
        # stays clean; user-created pods with the same annotations get their
        # privileged context stripped by SCC admission. oc debug attaches, so
        # detach it and address the pod by its <sanitized-node>-debug-<x> name.
        local sanitized
        sanitized=${node//./}
        # Reap stale debug pods for this node so long poll loops (boot_id)
        # do not pile them up on the host.
        oc get pod -n "$NS" --no-headers 2>/dev/null | awk -v p="$sanitized-debug-" '$1 ~ "^"p {print $1}' \
            | while read -r stale; do oc delete -n "$NS" "pod/$stale" --wait=false >/dev/null 2>&1 || true; done
        setsid oc debug "node/$node" --image="${NODE_DEBUG_IMAGE:-debian:bookworm-slim}" \
            -- sh -c "sleep 600" >/dev/null 2>&1 </dev/null &
        for _ in $(seq 1 10); do
            sleep 2
            name=$(oc get pod -n "$NS" --no-headers 2>/dev/null | awk -v p="$sanitized-debug-" '$1 ~ "^"p {print $1; exit}')
            [ -n "$name" ] && break
        done
        [ -n "$name" ] || return 1
        printf '%s' "$name"
        return 0
    fi
    oc apply -f - >/dev/null <<EOF || return 1
apiVersion: v1
kind: Pod
metadata:
  name: $name
  annotations:
    # The namespace default SCC strips privileged; the privileged SCC is
    # usable by system:masters (the runner identity) and restores
    # CAP_SYS_CHROOT. On vanilla k8s the annotation is inert.
    openshift.io/sc.securityContextConstraint: privileged
spec:
  nodeSelector:
    kubernetes.io/hostname: $node
  restartPolicy: Never
  hostPID: true
  hostNetwork: true
  tolerations:
  - operator: Exists
  containers:
  - name: dbg
    image: ${NODE_DEBUG_IMAGE:-debian:bookworm-slim}
    securityContext:
      privileged: true
    command: ["sleep", "600"]
    volumeMounts:
    - name: host
      mountPath: /host
  volumes:
  - name: host
    hostPath:
      path: /
EOF
    printf '%s' "$name"
}

node_debug_exec() { # $1 = node, $2 = host command; runs in chroot /host
    local node="$1" command="$2" pod
    pod=$(node_debug_pod "$node") || fail "node debug pod oluşturulamadı: $node"
    oc wait --for=condition=Ready "pod/$pod" -n "$NS" --timeout=120s >/dev/null 2>&1 \
        || { oc delete -n "$NS" "pod/$pod" --wait=false >/dev/null 2>&1 || true; fail "node debug pod Ready olmadı: $pod"; }
    local rc=0
    oc exec -n "$NS" "$pod" -- chroot /host sh -c "$command" || rc=$?
    oc delete -n "$NS" "$pod" --wait=false >/dev/null 2>&1 || true
    return "$rc"
}

# Reboot a node without any SSH credentials.
reboot_node() { # $1 = node name
    local node="$1" reboot_out reboot_rc=0
    # --no-block makes systemctl return as soon as the job is queued, before
    # the exec session can be killed by the shutdown itself.
    reboot_out=$(node_debug_exec "$node" "systemctl reboot --no-block") || reboot_rc=$?
    # systemctl reboot returns 0 immediately once accepted; anything else
    # means the node will keep running, so surface the error instead of
    # polling a Ready node for five minutes.
    [ "$reboot_rc" -eq 0 ] || fail "reboot komutu reddedildi (rc=$reboot_rc): $reboot_out"
    log "reboot sinyali gönderildi: $node"
}

# Out-of-service taint on both platforms: oc uses adm taint, kubectl taint.
taint_out_of_service() { # $1 = node, $2 = apply(true) | remove(false)
    local node="$1" mode="$2"
    if [ "${KUBE_BIN##*/}" = "oc" ]; then
        if [ "$mode" = true ]; then
            oc adm taint node "$node" node.kubernetes.io/out-of-service=nodeshutdown:NoExecute --overwrite >/dev/null
        else
            oc adm taint node "$node" node.kubernetes.io/out-of-service- >/dev/null 2>&1 || true
        fi
    else
        if [ "$mode" = true ]; then
            oc taint node "$node" node.kubernetes.io/out-of-service=nodeshutdown:NoExecute --overwrite >/dev/null
        else
            oc taint node "$node" node.kubernetes.io/out-of-service- >/dev/null 2>&1 || true
        fi
    fi
}

# Read the node's kernel boot id; a changed value proves a real reboot.
boot_id() { # $1 = node
    local node="$1" id
    id=$(node_debug_exec "$node" "cat /proc/sys/kernel/random/boot_id" 2>/dev/null | tr -d '[:space:]' | tail -1)
    [ -n "$id" ] || return 1
    printf '%s' "$id"
}

node_exec() { # $1 = node, $2 = host shell command
    local node="$1" command="$2"
    if [ -n "${CSI_NODE_EXEC:-}" ]; then
        "$CSI_NODE_EXEC" "$node" "$command"
    elif [ "${KUBE_BIN##*/}" = "oc" ]; then
        oc debug "node/$node" --quiet -- chroot /host sh -c "$command"
    else
        # Older kubectl versions return immediately after creating the node
        # debug pod without streaming the command output (rc=0, empty stdout),
        # so run the command via kubectl exec against a short-lived sleep pod.
        local dbg_out dbg_pod rc
        dbg_out=$(oc debug "node/$node" --profile=sysadmin \
            --image="${NODE_DEBUG_IMAGE:-debian:bookworm-slim}" -- sh -c "sleep 600" 2>&1) \
            || fail "node debug pod oluşturulamadı: $dbg_out"
        dbg_pod=$(printf '%s\n' "$dbg_out" | sed -n 's/.*debugging pod \([^ ]*\) with.*/\1/p')
        [ -n "$dbg_pod" ] || fail "node debug pod adı çözülemedi: $dbg_out"
        oc wait --for=condition=Ready "pod/$dbg_pod" -n "$NS" --timeout=120s >/dev/null 2>&1 \
            || oc wait --for=condition=ContainerStatusesExist "pod/$dbg_pod" -n "$NS" --timeout=120s >/dev/null \
            || true
        rc=0
        oc exec -n "$NS" "$dbg_pod" -- chroot /host sh -c "$command" || rc=$?
        oc delete -n "$NS" "pod/$dbg_pod" --wait=false >/dev/null 2>&1 || true
        return "$rc"
    fi
}

snapshot_scenario_flow() {
    render_test_dir
    local VS_MANIFEST SRC_POD_YAML RESTORE_PVC_YAML RESTORE_POD_YAML
    VS_MANIFEST=$(ls "$RENDER_DIR"/*-vs.yaml 2>/dev/null | head -1 || true)
    SRC_POD_YAML=$(ls "$RENDER_DIR"/*-source-pod*.yaml 2>/dev/null | head -1 || true)
    RESTORE_PVC_YAML=$(ls "$RENDER_DIR"/*restore*pvc*.yaml 2>/dev/null | head -1 || true)
    RESTORE_POD_YAML=$(ls "$RENDER_DIR"/*restore*pod*.yaml 2>/dev/null | head -1 || true)
    [ -n "$VS_MANIFEST" ] && [ -n "$RESTORE_PVC_YAML" ] && [ -n "$RESTORE_POD_YAML" ] \
        || fail "snapshot manifestleri eksik"
    local VS_NAME SRC_POD RESTORE_POD
    VS_NAME=$(grep -m1 '  name:' "$VS_MANIFEST" | awk '{print $2}')
    SRC_POD=$(pod_from_manifest "$SRC_POD_YAML")
    RESTORE_POD=$(pod_from_manifest "$RESTORE_POD_YAML")
    log "snapshot senaryosu: $TEST_NAME (vs=$VS_NAME src_pod=$SRC_POD restore_pod=$RESTORE_POD)"

    # Ensure the authorized namespace exists before ANY namespaced apply,
    # mirroring apply_wait_pod. Snapshot fixtures ship no Namespace manifest,
    # so ns_manifest stays empty and ensure_namespace creates $NS bare. Without
    # this the first namespaced object (source PVC) fails with "namespace not
    # found" and cleanup then treats the never-created ns as foreign.
    local ns_manifest=""
    for f in "$RENDER_DIR"/*namespace*.yaml; do
        [ -f "$f" ] || continue
        ns_manifest="$f"
        break
    done
    ensure_namespace "$ns_manifest"

    log "(1) SC + VSC uygulanıyor"
    local f VSC_NAME
    for f in "$RENDER_DIR"/*-sc.yaml "$RENDER_DIR"/*-vsc.yaml; do
        [ -f "$f" ] && oc apply -f "$f" -n "$NS" >/dev/null
    done
    VSC_NAME=$(grep -m1 '  name:' "$RENDER_DIR"/*-vsc.yaml | awk '{print $2}')
    [ -n "$VSC_NAME" ] || fail "VolumeSnapshotClass adı çözülemedi"
    local _
    for _ in $(seq 1 30); do
        oc get "volumesnapshotclass/$VSC_NAME" >/dev/null 2>&1 && break
        sleep 1
    done
    oc get "volumesnapshotclass/$VSC_NAME" >/dev/null 2>&1 || fail "VolumeSnapshotClass hazır değil: $VSC_NAME"
    log "(2) source PVC + source pod uygulanıyor"
    for f in "$RENDER_DIR"/*-source-pvc.yaml "$RENDER_DIR"/*-source-pod.yaml; do
        [ -f "$f" ] && oc apply -f "$f" -n "$NS" >/dev/null
    done
    [ -n "$SRC_POD" ] || fail "source pod bulunamadı"
    wait_pod_ready "$SRC_POD" 300

    log "(3) snapshot öncesi source'a marker yazılıyor"
    write_marker "$SRC_POD" || fail "source marker yazılamadı"

    local ready snapshot_deadline
    log "(4) VolumeSnapshot '$VS_NAME' uygulanıyor + readyToUse (≤${SNAPSHOT_TIMEOUT}s)"
    # Wait for normal snapshot cleanup before reusing its name.
    local vs_ts
    vs_ts=$(oc get volumesnapshot "$VS_NAME" -n "$NS" -o jsonpath='{.metadata.deletionTimestamp}' 2>/dev/null || true)
    if [ -n "$vs_ts" ]; then
        log "waiting for snapshot cleanup: $VS_NAME"
        if ! oc wait --for=delete "volumesnapshot/$VS_NAME" -n "$NS" --timeout=120s; then
            touch "$OUT/.cleanup-failed"
            fail "snapshot cleanup did not complete: $VS_NAME"
        fi
    fi
    oc apply -f "$VS_MANIFEST" -n "$NS" >/dev/null
    ready=""; snapshot_deadline=$((SECONDS + SNAPSHOT_TIMEOUT))
    while [ "$SECONDS" -lt "$snapshot_deadline" ]; do
        ready=$(oc get volumesnapshot "$VS_NAME" -n "$NS" -o jsonpath='{.status.readyToUse}' 2>/dev/null || echo false)
        [ "$ready" = "true" ] && break
        sleep 2
    done
    [ "$ready" = "true" ] || { oc describe volumesnapshot "$VS_NAME" -n "$NS" 2>/dev/null | tail -20; fail "VolumeSnapshot readyToUse olmadı"; }

    local RESTORE_PVC
    log "(5) restore PVC uygulanıyor; Bound olduktan sonra pod başlatılıyor"
    RESTORE_PVC=$(grep -m1 '  name:' "$RESTORE_PVC_YAML" | awk '{print $2}')
    oc apply -f "$RESTORE_PVC_YAML" -n "$NS" >/dev/null
    oc wait --for=jsonpath='{.status.phase}'=Bound "pvc/$RESTORE_PVC" -n "$NS" --timeout=600s \
        || { oc describe "pvc/$RESTORE_PVC" -n "$NS"; fail "restore PVC Bound olmadı"; }
    # Exercise the CSI expansion contract as part of the restore path.  The
    # requested value is the desired total capacity (1Gi -> 2Gi); the driver
    # is responsible for converting that to the backend delta.  For filesystem
    # volumes Kubernetes cannot finish NodeExpandVolume until a pod mounts the
    # restored PVC.  Therefore prove controller-side expansion first (either
    # capacity is already 2Gi, or FileSystemResizePending is set), then start
    # the pod and require the final 2Gi status after node-side expansion.
    local expanded_capacity="" fs_resize_pending=""
    log "(5b) restore PVC genişletiliyor: 1Gi -> 2Gi"
    oc patch "pvc/$RESTORE_PVC" -n "$NS" --type=merge \
        -p '{"spec":{"resources":{"requests":{"storage":"2Gi"}}}}' >/dev/null \
        || fail "restore PVC expansion isteği reddedildi"
    local expand_deadline=$((SECONDS + 600))
    while [ "$SECONDS" -lt "$expand_deadline" ]; do
        expanded_capacity=$(oc get "pvc/$RESTORE_PVC" -n "$NS" \
            -o jsonpath='{.status.capacity.storage}' 2>/dev/null || true)
        fs_resize_pending=$(oc get "pvc/$RESTORE_PVC" -n "$NS" \
            -o 'jsonpath={.status.conditions[?(@.type=="FileSystemResizePending")].status}' 2>/dev/null || true)
        if [ "$expanded_capacity" = "2Gi" ] || [ "$fs_resize_pending" = "True" ]; then
            break
        fi
        sleep 2
    done
    if [ "$expanded_capacity" != "2Gi" ] && [ "$fs_resize_pending" != "True" ]; then
        oc describe "pvc/$RESTORE_PVC" -n "$NS" | tail -40
        fail "restore PVC controller expansion kanıtlanamadı: status.capacity=${expanded_capacity:-<empty>} fsPending=${fs_resize_pending:-<empty>}"
    fi
    log "PVC controller expansion kanıtlandı: $RESTORE_PVC status.capacity=${expanded_capacity:-<empty>} fsPending=${fs_resize_pending:-<empty>}"
    log "(6) restore pod uygulanıyor"
    oc apply -f "$RESTORE_POD_YAML" -n "$NS" >/dev/null
    wait_pod_ready "$RESTORE_POD" 300
    expand_deadline=$((SECONDS + 600))
    while [ "$SECONDS" -lt "$expand_deadline" ]; do
        expanded_capacity=$(oc get "pvc/$RESTORE_PVC" -n "$NS" \
            -o jsonpath='{.status.capacity.storage}' 2>/dev/null || true)
        [ "$expanded_capacity" = "2Gi" ] && break
        sleep 2
    done
    [ "$expanded_capacity" = "2Gi" ] || {
        oc describe "pvc/$RESTORE_PVC" -n "$NS" | tail -40
        fail "restore PVC node expansion tamamlanmadı: status.capacity=${expanded_capacity:-<empty>} (beklenen 2Gi)"
    }
    log "PVC node expansion kanıtlandı: $RESTORE_PVC status.capacity=$expanded_capacity"
    read_marker "$RESTORE_POD" "$MARKER"
    log "SNAPSHOT VERİ BÜTÜNLÜĞÜ KANITLANDI: snapshot'tan restore edilen veri eşleşti"
}

# Namespace isolation: every apply/delete in a scenario run works against a
# rendered copy of the manifests under $TMPDIR, never against the source
# fixtures. Rendering rewrites EVERY namespace-carrying reference in the copy
# to the authorized test NS (kubectl refuses to reconcile a manifest that pins
# its own namespace against `-n`) and lets tests substitute placeholders
# (worker pins) on the copy, so the source tree stays byte-identical across
# repeated runs and the user/driver diff never picks up run residue.
#
# RENDER_ID is the run identity minted by the runner (run.sh); every
# pre/run/post/cleanup process of one run shares it so they render into the
# SAME fresh directory. Repeated runs of the same TEST_NAME get distinct
# identities and never see another run's stale manifests.
# inherit the runner's minted run identity instead of resetting it — a
# pre/run/post/cleanup child that sources lib.sh must render into the SAME
# directory as every other process of the run.
RENDER_DIR="${RENDER_DIR:-}"
RENDER_ID="${RENDER_ID:-}"

# The authorized test NS (set by the runner from NAMESPACE) is the single
# choice for the run. It is NEVER re-derived from or overridden by a fixture's
# Namespace manifest. render_namespace renders every namespace-carrying
# reference in the temporary copy to $NS:
#   * namespaced metadata (`namespace:` fields, any pinned value — `default`,
#     a fixture's own `benchmark-kbench-nfs`-style pin, etc.),
#   * a shipped Namespace manifest's own `metadata.name`,
#   * SCC `users` that reference `system:serviceaccount:<ns>:<sa>`.
render_namespace() {
    local f kind
    for f in "$RENDER_DIR"/*.yaml; do
        [ -f "$f" ] || continue
        kind=$({ grep -m1 -E '^[[:space:]]*kind:' "$f" || true; } | awk '{print $2}')
        sed -i -E "s|^([[:space:]]+namespace:) +[^[:space:]]+$|\1 ${NS}|" "$f"
        sed -i -E "s|(system:serviceaccount:)[^:]+:|\1${NS}:|" "$f"
        if [ "$kind" = "Namespace" ]; then
            sed -i -E "s|^([[:space:]]+name:)[[:space:]].*$|\1 ${NS}|" "$f"
        fi
    done
}

render_test_dir() {
    # This run's scratch was already collected by cleanup; never repopulate it.
    if [ -n "${OUT:-}" ] && [ -f "$OUT/.render-collected" ]; then
        return 0
    fi
    # Already rendered for this run: keep any substitutions (worker pins).
    if [ -n "${RENDER_DIR:-}" ] && [ -f "${RENDER_DIR}/.rendered" ]; then
        return 0
    fi
    # No run identity yet (direct invocation / offline harness): mint one. The
    # runner normally exports RENDER_DIR/RENDER_ID for every scenario process.
    if [ -z "${RENDER_DIR:-}" ]; then
        RENDER_DIR="$(mktemp -d "${TMPDIR:?}/csi-scenario-render.XXXXXX")"
        RENDER_ID="$(basename "$RENDER_DIR")"
        export RENDER_DIR RENDER_ID
    fi
    local f
    for f in "$TEST_DIR"/*.yaml; do
        [ -f "$f" ] && cp -f -- "$f" "$RENDER_DIR/"
    done
    render_namespace
    touch "${RENDER_DIR}/.rendered"
    log "rendered manifests: $RENDER_DIR (run=${RENDER_ID} ns=$NS, source $TEST_DIR unchanged)"
}

# Ensure the authorized namespace exists, creating it only when this run owns
# it. A namespace that already exists (foreign or pre-existing) is adopted for
# the run but is NEVER claimed or deleted by cleanup. Ownership is recorded in
# $OUT so the separate cleanup process makes the same decision.
ensure_namespace() { # $1 = rendered namespace manifest (optional)
    # ownership requires a PROVABLE create. A GET failure that is not a
    # clean not-found (403/timeout/5xx) is fail-closed — guessing "missing"
    # and applying could adopt (and later let cleanup delete) a namespace
    # created by someone else in the race window.
    local uid rc create_out
    rc=0
    uid=$(oc get "namespace/$NS" --ignore-not-found -o jsonpath='{.metadata.uid}' 2>"$OUT/.ns-get.err") || rc=$?
    if [ $rc -ne 0 ]; then
        log "namespace durumu belirsiz — fail-closed (oc get rc=$rc: $(cat "$OUT/.ns-get.err" 2>/dev/null))" >&2
        return 1
    fi
    if [ -n "$uid" ]; then
        log "namespace hazır, sahiplenilmiyor (yabancı/önceden varolan, uid=$uid): $NS"
        return 0
    fi
    # V1: the ownership UID comes from the CREATE RESPONSE itself. A follow-up
    # GET could return a replacement's UID (deleted-and-recreated between
    # create and get) and would record ownership of an object this run never
    # created. An empty or malformed response records NOTHING and fails
    # closed.
    local create_rc=0 create_err
    if [ -n "${1:-}" ] && [ -f "${1:-}" ]; then
        create_out=$(oc create -f "$1" -o json 2>"$OUT/.ns-create.err") || create_rc=$?
    else
        create_out=$(oc create -f - -o json 2>"$OUT/.ns-create.err" <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: $NS
EOF
    ) || create_rc=$?
    fi
    if [ $create_rc -eq 0 ]; then
        # W2: parse the response as JSON and take metadata.uid ONLY — a text
        # scan could capture an annotation/label uid that precedes
        # metadata.uid in pretty-printed output. Invalid JSON, a missing or
        # mistyped uid, or a response that is not the expected Namespace all
        # fail closed with NO ownership record. No follow-up GET re-identifies
        # the object (that could observe a replacement).
        uid=$(printf '%s' "$create_out" | NS_WANT="$NS" python3 -c '
import json, os, sys
try:
    doc = json.load(sys.stdin)
except Exception:
    sys.exit(3)
meta = doc.get("metadata")
if not isinstance(meta, dict):
    sys.exit(3)
if doc.get("kind") != "Namespace" or meta.get("name") != os.environ["NS_WANT"]:
    sys.exit(5)
uid = meta.get("uid")
if not isinstance(uid, str) or not uid:
    sys.exit(4)
print(uid)
') || uid=""
        if [ -z "$uid" ]; then
            create_err=$(sed -n 1p "$OUT/.ns-create.err" 2>/dev/null)
            log "create cevabı metadata.uid taşımadı (bozuk/yetersiz cevap) — sahiplik kaydı OLUŞTURULMADI, fail-closed (${create_err:-cevap ayrıştırılamadı})" >&2
            return 1
        fi
        printf '%s\n%s\n' "$uid" "$NS" > "$OUT/.ns-owned"
        log "namespace oluşturuldu (sahiplik create cevabındaki metadata.uid ile kaydedildi, uid=$uid): $NS"
        return 0
    fi
    # Scan the whole captured error (first-line matching breaks under shell
    # trace noise and multi-line API errors).
    create_err=$(cat "$OUT/.ns-create.err" 2>/dev/null)
    case "${create_err:-}" in
        *"AlreadyExists"*|*"already exists"*)
            # Lost a create race: the namespace is foreign. Claim nothing.
            uid=$(oc get "namespace/$NS" -o jsonpath='{.metadata.uid}' 2>/dev/null) || uid=""
            if [ -n "$uid" ]; then
                log "namespace create yarışı: mevcut namespace sahiplenilmiyor (uid=$uid): $NS"
                return 0
            fi
            log "namespace create yarışı belirsiz kaldı — fail-closed: $NS" >&2
            return 1
            ;;
        *)
            log "namespace oluşturulamadı — fail-closed: ${create_err:-create hatası}" >&2
            return 1
            ;;
    esac
}

# Collect this run's render scratch at most once, unless retention (--keep /
# evidence) is requested. RENDER_ID identifies the run's own directory, so
# parallel runs and other tests' scratch are never touched.
collect_render_scratch() {
    if [ "$KEEP" = "1" ]; then
        return 0
    fi
    [ -n "${RENDER_ID:-}" ] || return 0
    [ -n "${OUT:-}" ] || return 0
    [ -f "$OUT/.render-collected" ] && return 0
    touch "$OUT/.render-collected"
    if [ -d "${RENDER_DIR:-}" ] && [ "$(basename "$RENDER_DIR")" = "$RENDER_ID" ]; then
        rm -rf -- "$RENDER_DIR"
        log "render scratch toplandı (run=$RENDER_ID): $RENDER_DIR"
    fi
    return 0
}

render_substitute() { # $1 = placeholder, $2 = replacement
    local f
    for f in "$RENDER_DIR"/*.yaml; do
        [ -f "$f" ] || continue
        sed -i "s|$1|$2|g" "$f"
    done
}

# Print the node pinned by a rendered pod manifest's kubernetes.io/hostname
# nodeSelector (empty if the manifest carries no such pin).
rendered_pin_node() { # $1 = rendered manifest
    sed -n 's/^[[:space:]]*kubernetes\.io\/hostname:[[:space:]]*//p' "$1" | head -1
}

# Require the two rendered pod manifests to pin exactly the two selected Ready
# nodes, which must be distinct. Read back the RENDERED nodeSelector rather than
# trusting the substitution: a stale/literal fixture (no __NODE_A__/__NODE_B__
# placeholder) makes render_substitute a silent no-op, and the render output
# then disagrees with the chosen placement. This fails the run on any mismatch
# so a wrong placement can never yield a silent RWX pass.
# $1..$2 = rendered pod manifests, $3..$4 = selected nodes.
verify_two_node_placement() {
    local ma="$1" mb="$2" want_a="$3" want_b="$4"
    local ra rb
    ra=$(rendered_pin_node "$ma")
    rb=$(rendered_pin_node "$mb")
    [ "$ra" = "$want_a" ] || fail "RWX placement kanıtı uyumsuz (pod-a): rendered='$ra' seçilen Ready worker='$want_a' — fixture substitution yerine literal/yanlış node taşıyor"
    [ "$rb" = "$want_b" ] || fail "RWX placement kanıtı uyumsuz (pod-b): rendered='$rb' seçilen Ready worker='$want_b' — fixture substitution yerine literal/yanlış node taşıyor"
    [ -n "$ra" ] && [ "$ra" != "$rb" ] || fail "RWX iki farklı node şartı render çıktısında doğrulanamadı: pod-a='$ra' pod-b='$rb'"
    log "RWX render nodeSelector doğrulandı: pod-a -> $ra, pod-b -> $rb (iki farklı Ready worker)"
}

# Apply every manifest in the test dir and wait for the pod that takes the
# volume; result in POD_NAME. RWOP gibi çok pod'lu testlerde volume'u alan pod
# Ready olur, diğerleri Pending kalır; Ready olan aday seçilir. STS'lerde pod
# adı <sts>-<ordinal>'dir.
# Wait for every PVC the test dir declares to reach Bound. Call this before
# waiting on any pod: a pod that starts before its PVC binds emits a transient
# FailedMount and may never become Ready within the wait window.
wait_test_pvcs_bound() {
    local f pvcname
    for f in "$RENDER_DIR"/*pvc*.yaml; do
        [ -f "$f" ] || continue
        pvcname=$(grep -m1 '  name:' "$f" | awk '{print $2}')
        [ -n "$pvcname" ] || continue
        # PVCs expose .status.phase, not .status.conditions, so
        # --for=condition=Bound never matches; wait on the phase field.
        oc wait --for=jsonpath='{.status.phase}'=Bound "pvc/$pvcname" -n "$NS" --timeout=600s >/dev/null \
            || { oc describe "pvc/$pvcname" -n "$NS" | tail -15; fail "PVC Bound olmadı: $pvcname"; }
    done
}

apply_wait_pod() {
    # The authorized test NS (set by the runner from NAMESPACE) is the single
    # choice. A fixture that ships its own Namespace manifest is rendered to
    # $NS by render_test_dir — never used to re-derive or override it.
    render_test_dir
    log "uygulanıyor: $RENDER_DIR (ns=$NS)"
    # REPLACE_WITH_WORKER_NODE pins single-node multi-writer pods; all
    # occurrences in one test go to the SAME Ready worker (CSI node plugins
    # run on workers only). The pin is rendered on the temporary copy only;
    # the source placeholder survives for the next run.
    if grep -rl "REPLACE_WITH_WORKER_NODE" "$RENDER_DIR" >/dev/null 2>&1; then
        local worker
        worker=$(oc get nodes --no-headers | awk '$2=="Ready" && /worker/{print $1}' | head -1)
        [ -n "$worker" ] || fail "Ready worker node bulunamadı"
        render_substitute "REPLACE_WITH_WORKER_NODE" "$worker"
        note_node "$worker"
    fi
    # Ensure the authorized namespace exists (adopt if present, create+own if
    # not) before any namespaced apply, whether or not the test ships a
    # Namespace manifest.
    local nsf ns_manifest=""
    for nsf in "$RENDER_DIR"/*namespace*.yaml; do
        [ -f "$nsf" ] || continue
        ns_manifest="$nsf"
        break
    done
    ensure_namespace "$ns_manifest"
    # Apply in dependency order (a pod before its PVC races a transient
    # FailedMount event that wait_pod_ready treats as fatal): StorageClasses,
    # then PVCs, then everything else.
    for f in $(find "$RENDER_DIR" -name '*.yaml' ! -name '*namespace*.yaml' ! -name '*scc*.yaml' \
        | awk '{p=9; if ($0 ~ /-sc\.yaml$/) p=1; else if ($0 ~ /-pvc.*\.yaml$/) p=2
                print p "\t" $0}' | sort -n | cut -f2-); do
        [ -f "$f" ] || continue
        oc apply -f "$f" -n "$NS" >/dev/null
    done
    wait_test_pvcs_bound
    if oc api-resources --no-headers 2>/dev/null | grep -qi securitycontextconstraints; then
        for f in "$RENDER_DIR"/*scc*.yaml; do
            [ -f "$f" ] || continue
            oc apply -f "$f" >/dev/null
        done
    fi

    local CANDIDATES candidate podf
    CANDIDATES=$(oc get pods -n "$NS" -o name | grep "$TEST_NAME" \
        | grep -v -- '-source-' | sed 's|pod/||' | head -5 || true)
    POD_NAME=""
    for candidate in $CANDIDATES; do
        if oc get "pod/$candidate" -n "$NS" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null | grep -q True; then
            POD_NAME="$candidate"
            break
        fi
    done
    if [ -z "$POD_NAME" ] && [ -n "$CANDIDATES" ]; then
        POD_NAME=$(echo "$CANDIDATES" | head -1)
    fi
    if [ -z "$POD_NAME" ]; then
        podf=$(ls "$RENDER_DIR"/*pod*.yaml 2>/dev/null | grep -v -- '-source-' | head -1)
        [ -n "$podf" ] || podf=$(ls "$RENDER_DIR"/*pod*.yaml 2>/dev/null | head -1)
        [ -n "$podf" ] || fail "testte pod manifesti yok: $TEST_DIR"
        POD_NAME=$(pod_from_manifest "$podf")
        [ "$POD_NAME" = "$TEST_NAME" ] && [ -n "$CANDIDATES" ] && POD_NAME=$(echo "$CANDIDATES" | head -1)
    fi

    wait_pod_ready "$POD_NAME" 600
}

# Plain apply → wait → marker body shared by most tests. Snapshot tests
# (dir contains *-vs.yaml) use snapshot_scenario_flow instead.
generic_scenario() {
    if ls "$TEST_DIR"/*-vs.yaml >/dev/null 2>&1 && [ -n "$(ls "$TEST_DIR"/*restore*pvc*.yaml 2>/dev/null)" ]; then
        snapshot_scenario_flow
    else
        apply_wait_pod
        case "$TEST_NAME" in
            volume-data-integrity-marker)
                local checksum_deadline checksum_ready
                checksum_deadline=$((SECONDS + 300)); checksum_ready=0
                while [ "$SECONDS" -lt "$checksum_deadline" ]; do
                    if toc exec "$POD_NAME" -n "$NS" -- test -s /data/marker.sha256 >/dev/null 2>&1; then
                        checksum_ready=1
                        break
                    fi
                    sleep 2
                done
                [ "$checksum_ready" -eq 1 ] || fail "checksum dosyası oluşmadı veya pod erken sonlandı"
                oc exec "$POD_NAME" -n "$NS" -- sh -c 'sha256sum -c /data/marker.sha256'
                log "CHECKSUM VERİ BÜTÜNLÜĞÜ OK"
                ;;
            volume-raw-block-device)
                local raw got
                raw="RAW-$MARKER"
                oc exec "$POD_NAME" -n "$NS" -- sh -c "printf '%s' '$raw' | dd of=/dev/xvda bs=1 count=${#raw} conv=notrunc 2>/dev/null"
                got=$(oc exec "$POD_NAME" -n "$NS" -- sh -c "dd if=/dev/xvda bs=1 count=${#raw} status=none 2>/dev/null" || true)
                [ "$got" = "$raw" ] || fail "raw block verisi BOZUK: okunan=$got beklenen=$raw"
                log "RAW BLOCK VERİ BÜTÜNLÜĞÜ OK"
                ;;
            benchmark-fio)
                # The pod installs fio in its startup command.  Waiting only
                # for Running races that install and causes a false
                # "fio: not found" failure.
                local fio_ready=0
                for _ in $(seq 1 60); do
                    if toc exec "$POD_NAME" -n "$NS" -- sh -c \
                        'command -v fio >/dev/null 2>&1' >/dev/null 2>&1; then
                        fio_ready=1
                        break
                    fi
                    sleep 5
                done
                [ "$fio_ready" -eq 1 ] || fail "fio readiness timeout"
                oc exec "$POD_NAME" -n "$NS" -- sh -c "echo '$MARKER' >/data/verify.txt && sync"
                oc exec "$POD_NAME" -n "$NS" -- sh -c "fio --name=csi-smoke --filename=/data/fio --rw=randrw --bs=4k --size=32M --runtime=10 --time_based --direct=1 --group_reporting" \
                    2>&1 || fail "fio benchmark başarısız"
                read_marker "$POD_NAME" "$MARKER"
                ;;
            *)
                # Fail-closed writable path: a failed marker write must fail
                # the scenario. rc=1 is the designed read-only denial (the
                # readonly case owns that explicit flow); any other non-zero
                # write result is a real I/O failure, never a silent PASS.
                local write_rc=0
                write_marker "$POD_NAME" || write_rc=$?
                if [ "$write_rc" -eq 0 ]; then
                    read_marker "$POD_NAME" "$MARKER"
                elif [ "$write_rc" -eq 1 ]; then
                    case "$TEST_NAME" in
                        *readonly*) : ;;
                        *) fail "beklenmeyen yazma reddi (rc=1): $TEST_NAME" ;;
                    esac
                else
                    fail "marker yazımı başarısız (rc=$write_rc): $TEST_NAME"
                fi
                ;;
        esac
    fi
    log "test kaynakları:"
    oc get pvc,pv -n "$NS" 2>/dev/null | grep "$TEST_NAME" || true
    log "test başarılı: $TEST_NAME"
}

# Verify one pod's CSI filesystem mount from the host side. Prints stable
# KEY=VALUE evidence so race scenarios can compare the same filesystem across
# different nodes without requiring storage tooling inside the workload image.
csi_race_first_result_field() { # $1=json file, $2=field
    python3 - "$1" "$2" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    payload = json.load(stream)
results = payload.get("results") or []
value = results[0].get(sys.argv[2], "") if results else ""
print(value)
PY
}

direct_controller_race_cleanup() {
    local snapshot_id="" volume_id="" cleanup_rc=0
    if [ -f "$OUT/.csi-race-snapshot-id" ]; then
        snapshot_id=$(cat "$OUT/.csi-race-snapshot-id" 2>/dev/null || true)
    fi
    if [ -f "$OUT/.csi-race-volume-id" ]; then
        volume_id=$(cat "$OUT/.csi-race-volume-id" 2>/dev/null || true)
    fi
    if [ -n "$snapshot_id" ] && [ -n "${CSI_RACE_CONTROLLER_POD:-}" ]; then
        if ! csi_race_exec delete-snapshot --snapshot-id "$snapshot_id" --timeout 120s >/dev/null 2>&1; then
            log "direct race cleanup: snapshot silinemedi: $snapshot_id"
            cleanup_rc=1
        fi
    fi
    if [ -n "$volume_id" ] && [ -n "${CSI_RACE_CONTROLLER_POD:-}" ]; then
        if ! csi_race_exec delete-volume --volume-id "$volume_id" --timeout 120s >/dev/null 2>&1; then
            log "direct race cleanup: volume silinemedi: $volume_id"
            cleanup_rc=1
        fi
    fi
    rm -f "$OUT/.csi-race-snapshot-id" "$OUT/.csi-race-volume-id"
    csi_race_cleanup
    if [ "$cleanup_rc" -ne 0 ]; then
        touch "$OUT/.cleanup-failed"
    fi
    return "$cleanup_rc"
}

# Direct CSI same-name CreateVolume barrier. The helper itself requires every
# call to return OK with the same non-empty volume ID; divergent duplicate
# backend objects therefore fail immediately.
race_idempotent_create_scenario() {
    local count=${RACE_CALL_COUNT:-8} name output volume_id
    case "$count" in ''|*[!0-9]*) fail "RACE_CALL_COUNT sayı olmalı: $count" ;; esac
    if [ "$count" -lt 2 ] || [ "$count" -gt 32 ]; then
        fail "RACE_CALL_COUNT 2..32 aralığında olmalı"
    fi
    csi_race_prepare
    name="plan11-${PROTOCOL}-create-${STAMP}-$$"
    output="$OUT/csi-race-create-volume.json"
    csi_race_exec create-volume --name "$name" --capacity-bytes $((1<<30)) \
        --count "$count" --timeout 180s >"$output" \
        || fail "direct concurrent CreateVolume başarısız; evidence=$output"
    volume_id=$(csi_race_first_result_field "$output" volume_id)
    [ -n "$volume_id" ] || fail "CreateVolume race response volume ID boş"
    printf '%s' "$volume_id" > "$OUT/.csi-race-volume-id"
    log "IDEMPOTENT CREATE RACE PASS: protocol=$PROTOCOL calls=$count volume=$volume_id"
}

# Direct CSI same-name CreateSnapshot barrier. A fresh direct CSI source volume
# is created first; all snapshot calls must converge on one snapshot ID.
race_snapshot_create_scenario() {
    local count=${RACE_CALL_COUNT:-8} source_json snapshot_json volume_id snapshot_id name
    case "$count" in ''|*[!0-9]*) fail "RACE_CALL_COUNT sayı olmalı: $count" ;; esac
    if [ "$count" -lt 2 ] || [ "$count" -gt 32 ]; then
        fail "RACE_CALL_COUNT 2..32 aralığında olmalı"
    fi
    csi_race_prepare
    source_json="$OUT/csi-race-snapshot-source.json"
    csi_race_exec create-volume --name "plan11-${PROTOCOL}-snap-src-${STAMP}-$$" \
        --capacity-bytes $((1<<30)) --count 1 --timeout 180s >"$source_json" \
        || fail "snapshot race source CreateVolume başarısız"
    volume_id=$(csi_race_first_result_field "$source_json" volume_id)
    [ -n "$volume_id" ] || fail "snapshot race source volume ID boş"
    printf '%s' "$volume_id" > "$OUT/.csi-race-volume-id"

    name="plan11-${PROTOCOL}-snapshot-${STAMP}-$$"
    snapshot_json="$OUT/csi-race-create-snapshot.json"
    csi_race_exec create-snapshot --name "$name" --source-volume-id "$volume_id" \
        --count "$count" --timeout 180s >"$snapshot_json" \
        || fail "direct concurrent CreateSnapshot başarısız; evidence=$snapshot_json"
    snapshot_id=$(csi_race_first_result_field "$snapshot_json" snapshot_id)
    [ -n "$snapshot_id" ] || fail "CreateSnapshot race response snapshot ID boş"
    printf '%s' "$snapshot_id" > "$OUT/.csi-race-snapshot-id"
    log "SNAPSHOT CREATE RACE PASS: protocol=$PROTOCOL calls=$count snapshot=$snapshot_id source=$volume_id"
}

# The block backend expand API is additive, so two concurrent 1Gi -> 2Gi
# requests must finish at exactly 2Gi, never 3Gi. csi-race performs a final
# ControllerGetVolume readback and enforces exact capacity equality.
controller_restart_cleanup() {
    local cleanup_rc=0 controller_ns="" controller_deploy="" original_replicas=""
    local vs="" pod="" pvc="" pv=""
    if [ -f "$OUT/.controller-restart-state" ]; then
        read -r controller_ns controller_deploy original_replicas < "$OUT/.controller-restart-state" || true
    fi
    [ -f "$OUT/.controller-restart-vs" ] && vs=$(cat "$OUT/.controller-restart-vs" 2>/dev/null || true)
    [ -f "$OUT/.controller-restart-pod" ] && pod=$(cat "$OUT/.controller-restart-pod" 2>/dev/null || true)
    [ -f "$OUT/.controller-restart-pvc" ] && pvc=$(cat "$OUT/.controller-restart-pvc" 2>/dev/null || true)
    [ -f "$OUT/.controller-restart-pv" ] && pv=$(cat "$OUT/.controller-restart-pv" 2>/dev/null || true)

    if [ -n "$vs" ]; then
        timeout "$CLEANUP_TIMEOUT" "$KUBE_BIN" delete "volumesnapshot/$vs" -n "$NS" --ignore-not-found --wait=true >/dev/null 2>&1 || cleanup_rc=1
    fi
    if [ -n "$pod" ]; then
        timeout "$CLEANUP_TIMEOUT" "$KUBE_BIN" delete "pod/$pod" -n "$NS" --ignore-not-found --wait=true >/dev/null 2>&1 || cleanup_rc=1
    fi
    if [ -n "$pvc" ]; then
        timeout "$CLEANUP_TIMEOUT" "$KUBE_BIN" delete "pvc/$pvc" -n "$NS" --ignore-not-found --wait=true >/dev/null 2>&1 || cleanup_rc=1
    fi
    if [ -n "$pv" ]; then
        oc wait --for=delete "pv/$pv" --timeout=300s >/dev/null 2>&1 || cleanup_rc=1
    fi

    if [ -n "$controller_ns" ] && [ -n "$controller_deploy" ] && [ -n "$original_replicas" ]; then
        if oc get "deployment/$controller_deploy" -n "$controller_ns" >/dev/null 2>&1; then
            oc scale "deployment/$controller_deploy" -n "$controller_ns" --replicas="$original_replicas" >/dev/null 2>&1 || cleanup_rc=1
            if [ "$original_replicas" -gt 0 ]; then
                oc rollout status "deployment/$controller_deploy" -n "$controller_ns" --timeout=300s >/dev/null 2>&1 || cleanup_rc=1
            fi
        fi
    fi

    rm -f "$OUT/.controller-restart-state" "$OUT/.controller-restart-vs" \
        "$OUT/.controller-restart-pod" "$OUT/.controller-restart-pvc" "$OUT/.controller-restart-pv"
    if [ "$cleanup_rc" -ne 0 ]; then
        touch "$OUT/.cleanup-failed"
    fi
    return "$cleanup_rc"
}

wait_controller_ready_replicas() { # $1=namespace $2=deployment $3=ready count $4=timeout seconds
    local namespace="$1" deployment="$2" wanted="$3" timeout_s="${4:-300}" ready available
    local deadline=$((SECONDS + timeout_s))
    while [ "$SECONDS" -lt "$deadline" ]; do
        read -r ready available < <(oc get "deployment/$deployment" -n "$namespace" -o json 2>/dev/null | python3 -c '
import json,sys
data=json.load(sys.stdin)
status=data.get("status", {})
print(status.get("readyReplicas", 0) or 0, status.get("availableReplicas", 0) or 0)
' 2>/dev/null || echo "0 0")
        if [ "$ready" -eq "$wanted" ] && [ "$available" -eq "$wanted" ]; then
            return 0
        fi
        sleep 2
    done
    oc get "deployment/$deployment" -n "$namespace" -o wide 2>/dev/null || true
    oc get pod -n "$namespace" -l "app.kubernetes.io/name=${PROTOCOL}-csi-ngxstorage,app.kubernetes.io/component=controller" -o wide 2>/dev/null || true
    return 1
}

controller_leader_snapshot() { # $1=namespace $2=output json
    local namespace="$1" output="$2" pods_file="$OUT/controller-pods.json" leases_file="$OUT/controller-leases.json"
    oc get pods -n "$namespace" -l "app.kubernetes.io/name=${PROTOCOL}-csi-ngxstorage,app.kubernetes.io/component=controller" -o json > "$pods_file"
    oc get leases.coordination.k8s.io -n "$namespace" -o json > "$leases_file"
    python3 - "$pods_file" "$leases_file" "$output" <<'PY'
import json, sys

pods = json.load(open(sys.argv[1], encoding="utf-8"))
leases = json.load(open(sys.argv[2], encoding="utf-8"))
ready = set()
for pod in pods.get("items", []):
    meta = pod.get("metadata", {})
    if meta.get("deletionTimestamp"):
        continue
    conditions = {c.get("type"): c.get("status") for c in pod.get("status", {}).get("conditions", [])}
    if conditions.get("Ready") == "True":
        ready.add(meta.get("name", ""))

entries = []
for lease in leases.get("items", []):
    name = lease.get("metadata", {}).get("name", "")
    if "-operation-lock-" in name:
        continue
    spec = lease.get("spec", {})
    holder = spec.get("holderIdentity") or ""
    pod_name = next((pod for pod in sorted(ready) if pod and pod in holder), "")
    if not pod_name:
        continue
    entries.append({
        "name": name,
        "holder": holder,
        "pod": pod_name,
        "renew_time": spec.get("renewTime") or "",
        "resource_version": lease.get("metadata", {}).get("resourceVersion", ""),
    })

payload = {"ready_pods": sorted(ready), "leases": sorted(entries, key=lambda x: x["name"])}
with open(sys.argv[3], "w", encoding="utf-8") as stream:
    json.dump(payload, stream, indent=2, sort_keys=True)
PY
}

# Scale the real controller Deployment to two replicas, prove sidecar Lease
# leaders exist, kill a pod holding at least one leader, and require affected
# leases to move to a different Ready controller pod. Afterwards perform a
# fresh provision -> attach/mount -> online resize -> snapshot flow so all four
# leader-elected sidecars are exercised after the transfer. The original
# replica count is always restored by controller_restart_cleanup.
controller_restart_leader_election_scenario() {
    local selector="app.kubernetes.io/name=${PROTOCOL}-csi-ngxstorage,app.kubernetes.io/component=controller"
    local discovery controller_ns controller_deploy original_replicas before after victim affected_count
    local sc_file vsc_file sc_name vsc_name pvc pod pv vs capacity deadline

    discovery=$(oc get deployment -A -l "$selector" -o json | python3 -c '
import json,sys
items=json.load(sys.stdin).get("items", [])
if len(items) != 1:
    raise SystemExit(f"expected exactly one controller deployment, got {len(items)}")
item=items[0]
print(item["metadata"]["namespace"], item["metadata"]["name"], item.get("spec", {}).get("replicas", 1))
') || fail "${PROTOCOL} controller Deployment tekil olarak keşfedilemedi"
    read -r controller_ns controller_deploy original_replicas <<<"$discovery"
    if [ -z "$controller_ns" ] || [ -z "$controller_deploy" ]; then
        fail "controller namespace/name boş"
    fi
    case "$original_replicas" in ''|*[!0-9]*) fail "controller replica sayısı geçersiz: $original_replicas" ;; esac
    [ "$original_replicas" -ge 1 ] || fail "controller en az bir replica ile çalışmalı"
    printf '%s %s %s\n' "$controller_ns" "$controller_deploy" "$original_replicas" > "$OUT/.controller-restart-state"

    log "controller leader-election: $controller_ns/$controller_deploy replicas $original_replicas -> 2"
    oc scale "deployment/$controller_deploy" -n "$controller_ns" --replicas=2 >/dev/null
    wait_controller_ready_replicas "$controller_ns" "$controller_deploy" 2 300 \
        || fail "controller iki Ready replica'ya çıkmadı"

    before="$OUT/controller-leaders-before.json"
    controller_leader_snapshot "$controller_ns" "$before"
    python3 - "$before" <<'PY' || fail "leader-election Lease contractı bulunamadı"
import json, sys
data=json.load(open(sys.argv[1], encoding="utf-8"))
leases=data.get("leases", [])
if len(data.get("ready_pods", [])) != 2:
    raise SystemExit("expected two Ready controller pods")
if len(leases) < 4:
    raise SystemExit(f"expected at least four sidecar leader Leases, got {len(leases)}")
PY
    victim=$(python3 - "$before" <<'PY'
import collections, json, sys
data=json.load(open(sys.argv[1], encoding="utf-8"))
counts=collections.Counter(item["pod"] for item in data.get("leases", []))
print(counts.most_common(1)[0][0] if counts else "")
PY
)
    [ -n "$victim" ] || fail "leader holder controller pod seçilemedi"
    affected_count=$(python3 - "$before" "$victim" <<'PY'
import json,sys
data=json.load(open(sys.argv[1], encoding="utf-8"))
print(sum(1 for item in data.get("leases", []) if item.get("pod") == sys.argv[2]))
PY
)
    [ "$affected_count" -ge 1 ] || fail "victim pod hiçbir sidecar leader Lease'i tutmuyor"
    log "controller leader-election: victim=$victim held_leases=$affected_count"

    oc delete "pod/$victim" -n "$controller_ns" --wait=true >/dev/null
    wait_controller_ready_replicas "$controller_ns" "$controller_deploy" 2 300 \
        || fail "leader pod delete sonrası controller iki Ready replica'ya dönmedi"

    deadline=$((SECONDS + 180)); after="$OUT/controller-leaders-after.json"
    while [ "$SECONDS" -lt "$deadline" ]; do
        controller_leader_snapshot "$controller_ns" "$after"
        if python3 - "$before" "$after" "$victim" <<'PY'
import json, sys
before=json.load(open(sys.argv[1], encoding="utf-8"))
after=json.load(open(sys.argv[2], encoding="utf-8"))
victim=sys.argv[3]
ready=set(after.get("ready_pods", []))
if victim in ready or len(ready) != 2:
    raise SystemExit(1)
old={item["name"]: item for item in before.get("leases", []) if item.get("pod") == victim}
new={item["name"]: item for item in after.get("leases", [])}
if not old:
    raise SystemExit(1)
for name, previous in old.items():
    current=new.get(name)
    if not current:
        raise SystemExit(1)
    if current.get("holder") == previous.get("holder"):
        raise SystemExit(1)
    if current.get("pod") not in ready:
        raise SystemExit(1)
    if previous.get("renew_time") and current.get("renew_time") <= previous.get("renew_time"):
        raise SystemExit(1)
raise SystemExit(0)
PY
        then
            break
        fi
        sleep 2
    done
    python3 - "$before" "$after" "$victim" <<'PY' || fail "affected sidecar leader Lease'leri yeni podlara transfer olmadı"
import json, sys
before=json.load(open(sys.argv[1], encoding="utf-8"))
after=json.load(open(sys.argv[2], encoding="utf-8"))
victim=sys.argv[3]
ready=set(after.get("ready_pods", []))
old={item["name"]: item for item in before.get("leases", []) if item.get("pod") == victim}
new={item["name"]: item for item in after.get("leases", [])}
assert len(ready) == 2 and victim not in ready
for name, previous in old.items():
    current=new[name]
    assert current["holder"] != previous["holder"]
    assert current["pod"] in ready
    if previous.get("renew_time"):
        assert current.get("renew_time", "") > previous["renew_time"]
PY
    log "LEADER TRANSFER OK: victim=$victim affected_leases=$affected_count"

    render_test_dir
    sc_file=$(find "$RENDER_DIR" -maxdepth 1 -type f -name '*-sc.yaml' -print | head -1)
    vsc_file=$(find "$RENDER_DIR" -maxdepth 1 -type f -name '*-vsc.yaml' -print | head -1)
    if [ -z "$sc_file" ] || [ -z "$vsc_file" ]; then
        fail "leader-election scenario SC/VSC manifestleri eksik"
    fi
    sc_name=$(awk '$1=="name:"{print $2; exit}' "$sc_file")
    vsc_name=$(awk '$1=="name:"{print $2; exit}' "$vsc_file")
    oc apply -f "$sc_file" >/dev/null
    oc apply -f "$vsc_file" >/dev/null

    pvc="${TEST_NAME}-${PROTOCOL}-${STAMP}-pvc"
    pod="${TEST_NAME}-${PROTOCOL}-${STAMP}-pod"
    vs="${TEST_NAME}-${PROTOCOL}-${STAMP}-vs"
    printf '%s' "$pvc" > "$OUT/.controller-restart-pvc"
    printf '%s' "$pod" > "$OUT/.controller-restart-pod"
    printf '%s' "$vs" > "$OUT/.controller-restart-vs"

    oc apply -n "$NS" -f - >/dev/null <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $pvc
spec:
  accessModes: [ReadWriteOnce]
  volumeMode: Filesystem
  resources:
    requests: {storage: 1Gi}
  storageClassName: $sc_name
EOF
    oc wait --for=jsonpath='{.status.phase}'=Bound "pvc/$pvc" -n "$NS" --timeout=600s >/dev/null \
        || fail "leader transfer sonrası PVC Bound olmadı"
    pv=$(pv_for_pvc "$pvc")
    [ -n "$pv" ] || fail "leader transfer sonrası PV çözülemedi"
    printf '%s' "$pv" > "$OUT/.controller-restart-pv"

    oc apply -n "$NS" -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $pod
spec:
  restartPolicy: Never
  containers:
  - name: test
    image: nginx
    command: ["sh", "-c", "sleep 3600"]
    volumeMounts:
    - {name: data, mountPath: /data}
  volumes:
  - name: data
    persistentVolumeClaim: {claimName: $pvc}
EOF
    wait_pod_ready "$pod" 600
    write_marker "$pod" || fail "leader transfer sonrası marker yazılamadı"
    read_marker "$pod" "$MARKER"

    oc patch "pvc/$pvc" -n "$NS" --type=merge -p '{"spec":{"resources":{"requests":{"storage":"2Gi"}}}}' >/dev/null
    deadline=$((SECONDS + 600)); capacity=""
    while [ "$SECONDS" -lt "$deadline" ]; do
        capacity=$(oc get "pvc/$pvc" -n "$NS" -o jsonpath='{.status.capacity.storage}' 2>/dev/null || true)
        [ "$capacity" = "2Gi" ] && break
        sleep 2
    done
    [ "$capacity" = "2Gi" ] || fail "leader transfer sonrası online resize tamamlanmadı: capacity=${capacity:-<empty>}"

    oc apply -n "$NS" -f - >/dev/null <<EOF
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: $vs
spec:
  volumeSnapshotClassName: $vsc_name
  source:
    persistentVolumeClaimName: $pvc
EOF
    deadline=$((SECONDS + SNAPSHOT_TIMEOUT)); local ready=false
    while [ "$SECONDS" -lt "$deadline" ]; do
        ready=$(oc get "volumesnapshot/$vs" -n "$NS" -o jsonpath='{.status.readyToUse}' 2>/dev/null || echo false)
        [ "$ready" = true ] && break
        sleep 2
    done
    [ "$ready" = true ] || fail "leader transfer sonrası snapshot readyToUse olmadı"
    read_marker "$pod" "$MARKER"
    log "CONTROLLER RESTART/LEADER-ELECTION PASS: protocol=$PROTOCOL provision+attach+resize+snapshot healthy"
}

# Delete namespaced manifests first (kubectl refuses deletes into a
# terminating namespace, and the namespace can linger on backend reclaim
# finalizers), then drop the namespace unwaited.
run_test_cleanup() {
    local cleanup_rc=0 f
    if [ "$KEEP" = "1" ]; then
        log "temizlik atlandı (--keep) — test kaynakları kaldı"
        return 0
    fi
    render_test_dir
    log "temizlik: $TEST_DIR kaynakları (rendered copy: $RENDER_DIR)"

    # Delete workload objects before PVC/PV/StorageClass resources.  Do not
    # infer resource type from the filename: many scenarios intentionally use
    # names such as *-reader.yaml or *-writer.yaml for kind: Pod.  Deleting a
    # PVC first while such a pod still mounts it blocks on pvc-protection until
    # CLEANUP_TIMEOUT and turns an otherwise valid scenario into a false FAIL.
    manifest_holds_volume() {
        awk '
            $1 == "kind:" && $2 ~ /^(Pod|Job|CronJob|StatefulSet|Deployment|ReplicaSet|DaemonSet)$/ { found=1 }
            END { exit(found ? 0 : 1) }
        ' "$1"
    }

    cleanup_delete_manifest() {
        local manifest="$1" out rc
        out=$(timeout "$CLEANUP_TIMEOUT" "$KUBE_BIN" delete -f "$manifest" -n "$NS" --ignore-not-found --wait=true 2>&1)
        rc=$?
        [ "$rc" -eq 0 ] && return 0

        # A scenario may intentionally SKIP when the target cluster does not
        # serve an optional API (for example VolumeAttributesClass on OCP 4.20).
        # Such a manifest was never created, so an API-discovery error during
        # cleanup means "nothing to clean", not a leaked resource.
        case "$out" in
            *"no matches for kind"*|*"resource mapping not found"*|*"the server doesn't have a resource type"*)
                log "cleanup atlandı (API serve edilmiyor): $manifest"
                return 0
                ;;
        esac

        if [ "$rc" -eq 124 ]; then
            log "cleanup timeout: $manifest"
        else
            log "cleanup hata: $manifest (rc=$rc): ${out//$'\n'/ }"
        fi
        return "$rc"
    }

    # Namespaced objects only: workloads first (PVC-protection aware), then the
    # rest. Cluster-scoped manifests (SC/VSC/SCC/PV) are excluded here and
    # handled explicitly below so their scope and ownership stay unambiguous.
    while IFS= read -r -d '' f; do
        manifest_holds_volume "$f" || continue
        cleanup_delete_manifest "$f" || cleanup_rc=1
    done < <(find "$RENDER_DIR" -maxdepth 1 -type f -name '*.yaml' ! -name '*namespace*.yaml' ! -name '*scc*.yaml' ! -name '*-sc.yaml' ! -name '*-vsc.yaml' -print0)

    while IFS= read -r -d '' f; do
        manifest_holds_volume "$f" && continue
        awk '$1 == "kind:" && $2 == "PersistentVolume" { found=1 } END { exit(found ? 0 : 1) }' "$f" && continue
        cleanup_delete_manifest "$f" || cleanup_rc=1
    done < <(find "$RENDER_DIR" -maxdepth 1 -type f -name '*.yaml' ! -name '*namespace*.yaml' ! -name '*scc*.yaml' ! -name '*-sc.yaml' ! -name '*-vsc.yaml' -print0)

    # Cluster-scoped resources: name- and scope-owned by the run, deleted by
    # name (kubectl ignores -n for cluster-scoped kinds). PersistentVolume,
    # StorageClass and VolumeSnapshotClass never carry a namespace.
    while IFS= read -r -d '' f; do
        awk '$1 == "kind:" && $2 == "PersistentVolume" { found=1 } END { exit(found ? 0 : 1) }' "$f" || continue
        log "cleanup cluster-scoped PV: $(basename "$f")"
        cleanup_delete_manifest "$f" || cleanup_rc=1
    done < <(find "$RENDER_DIR" -maxdepth 1 -type f -name '*.yaml' ! -name '*namespace*.yaml' ! -name '*scc*.yaml' ! -name '*-sc.yaml' ! -name '*-vsc.yaml' -print0)
    for f in "$RENDER_DIR"/*-sc.yaml "$RENDER_DIR"/*-vsc.yaml; do
        [ -f "$f" ] || continue
        log "cleanup cluster-scoped: $(basename "$f")"
        cleanup_delete_manifest "$f" || cleanup_rc=1
    done
    # An SCC grants access to a namespace's serviceaccount. It is run-scoped
    # only when this run owns the namespace; a foreign/adopted namespace's SCC
    # is never revoked.
    for f in "$RENDER_DIR"/*scc*.yaml; do
        [ -f "$f" ] || continue
        if [ -f "$OUT/.ns-owned" ]; then
            log "cleanup cluster-scoped SCC (NS sahipliği bu koşumda): $(basename "$f")"
            cleanup_delete_manifest "$f" || cleanup_rc=1
        else
            log "cleanup SCC korunuyor (yabancı/adopt NS: $NS; sahiplik devralınmadı): $(basename "$f")"
        fi
    done
    # deleting the authorized namespace does NOT depend on a namespace
    # fixture file existing — the stdin-created case must be cleaned too.
    # U2/the UID precondition travels IN the DELETE request body
    # (DeleteOptions.preconditions.uid) — a client-side GET-then-compare is
    # not atomic: a replacement created between the check and the delete
    # would be removed by name. The API server enforces the precondition;
    # a precondition conflict means the run's namespace was replaced and is
    # kept, surfaced as cleanup-failed. The uid rides on stdin, never on the
    # command line or the log.
    if [ -f "$OUT/.ns-owned" ]; then
        local owner_uid
        owner_uid=$(sed -n '1p' "$OUT/.ns-owned" 2>/dev/null)
        if [ -n "$owner_uid" ]; then
            log "cleanup namespace (bu koşum yarattı, precondition uid taşınan DELETE): $NS"
            # : the supported raw-DELETE path is `delete --raw=<path> -f -`
            # (kubectl v0.33+ RunDelete / rawhttp: filename "-" carries the
            # stdin body). A bare `delete` drops the body, and `--request` is
            # not a delete-subcommand flag on real clients — the UID
            # precondition must travel in DeleteOptions.preconditions.uid.
            # The uid rides on stdin, never on the command line or the log.
            printf '{"kind":"DeleteOptions","apiVersion":"v1","preconditions":{"uid":"%s"}}' "$owner_uid" \
                | "$KUBE_BIN" delete --raw="/api/v1/namespaces/$NS" -f - >/dev/null 2>"$OUT/.ns-delete.err" \
                || { log "cleanup namespace silinemedi (precondition/timeout: $(sed -n 1p "$OUT/.ns-delete.err" 2>/dev/null))"; cleanup_rc=1; }
        else
            log "cleanup namespace silinmiyor (sahiplik kaydı UID taşımiyor): $NS"
            cleanup_rc=1
        fi
    else
        log "cleanup namespace silinmiyor (yabancı/önceden varolan NS: $NS)"
    fi
    if [ "$cleanup_rc" -ne 0 ]; then
        # Individual scenario traps intentionally preserve the original test
        # rc, but cleanup failure must still be visible to the public runner.
        # Leave an explicit evidence marker for run.sh to promote to FAIL.
        touch "$OUT/.cleanup-failed"
    fi
    # Collect only this run's render scratch (retention via --keep keeps it).
    collect_render_scratch
    return "$cleanup_rc"
}

# Delete a backend LUN with recovery: retries until the deadline and, when
# the array refuses with 534 (LUN still mapped to an iSCSI target), unmaps
# the reported target and removes it before retrying. Returns 0 on success.
collect_logs() {
    local now pod ns
    now=$(date -u +%FT%TZ)
    mkdir -p "$OUT"/{driver,cluster,host,backend}
    for ns in "${CSI_DRIVER_NAMESPACE:-ngxstorage-${PROTOCOL}}" nfs-csi-ngxstorage ngx-storage; do
        oc get pod -n "$ns" --no-headers 2>/dev/null | awk '{print $1}' | while read -r pod; do
            case "$pod" in *csi-ngxstorage*) ;; *) continue ;; esac
            oc logs "$pod" -n "$ns" --all-containers --since-time="$START" --timestamps \
                > "$OUT/driver/${pod}.log" 2>/dev/null || true
        done
    done
    oc get events -A --sort-by=.lastTimestamp --no-headers > "$OUT/cluster/events.txt" 2>/dev/null || true
    oc get va -o wide --no-headers > "$OUT/cluster/volumeattachments.txt" 2>/dev/null || true
    oc get pv --no-headers > "$OUT/cluster/pv.txt" 2>/dev/null || true
    oc get pvc -A --no-headers > "$OUT/cluster/pvc.txt" 2>/dev/null || true
    oc get pod -A --no-headers > "$OUT/cluster/pods.txt" 2>/dev/null || true
    backend_snapshots end
    if [ -f "$OUT/.nodes" ]; then
        local node ip
        while read -r node; do
            [ -n "$node" ] || continue
            ip=$(oc get "node/$node" -o 'jsonpath={.status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null | awk '{print $1}')
            [ -n "$ip" ] || continue
            node_exec "$node" "journalctl -u kubelet -u iscsid -u multipathd --since '$START' --until '$now' --no-pager" \
                > "$OUT/host/${node}.journal.log" 2>/dev/null || true
        done < "$OUT/.nodes"
    fi
}

backend_snapshots() {
    # $1 = start|end; dumps backend state for the bench's array (best effort).
    local marker=${1:-end}
    mkdir -p "$OUT/backend"
    {
        echo "# controllers: $NGX_CONTROLLER pool: $NGX_POOL_NAME at $(date -u +%FT%TZ)"
        "$NGX_SDK_CLI" --controllers "$NGX_CONTROLLER" --api-key "$NGX_API_KEY" --pool-name "$NGX_POOL_NAME" luns 2>/dev/null
        "$NGX_SDK_CLI" --controllers "$NGX_CONTROLLER" --api-key "$NGX_API_KEY" --pool-name "$NGX_POOL_NAME" shares 2>/dev/null
        "$NGX_SDK_CLI" --controllers "$NGX_CONTROLLER" --api-key "$NGX_API_KEY" --pool-name "$NGX_POOL_NAME" fctargets 2>/dev/null
        curl -sk -H "Authorization: Bearer $NGX_API_KEY" "https://${NGX_CONTROLLER%%,*}/api/v2/initiators" 2>/dev/null
    } > "$OUT/backend/${marker}.json" 2>/dev/null || true
}
