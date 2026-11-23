#!/usr/bin/env bash
source "$CSI_TEST_ROOT/lib.sh"
set -euo pipefail
trap 'rc=$?; run_test_cleanup || true; exit $rc' EXIT
# NS is the single authorized selection (set by the runner from NAMESPACE); the
# shipped Namespace manifest is rendered to it by render_test_dir, never used
# to override it. Apply from the rendered copy so the source fixtures stay
# untouched.
render_test_dir
nsf=$(ls "$RENDER_DIR"/*namespace*.yaml 2>/dev/null | head -1 || true)
ensure_namespace "$nsf"
oc apply -f "$RENDER_DIR"/*serviceaccount*.yaml -n "$NS" >/dev/null
for f in $(find "$RENDER_DIR" -name '*.yaml' ! -name '*scc*.yaml' \
    ! -name '*namespace*.yaml' ! -name '*serviceaccount*.yaml'); do
    oc apply -f "$f" -n "$NS" >/dev/null
done
# OpenShift: a dedicated SCC because the namespace UID range moves on every
# recreation and cannot be pinned in the pod spec; the grant needs a minute
# or two to propagate to admission, so sleep before waiting.
if oc api-resources --no-headers 2>/dev/null | grep -qi securitycontextconstraints; then
    oc apply -f "$RENDER_DIR"/*scc*.yaml >/dev/null
    sleep 90
fi
oc wait --for=jsonpath='{.status.phase}'=Bound "pvc/benchmark-kbench-nfs-pvc" -n "$NS" --timeout=600s \
    || { oc describe "pvc/benchmark-kbench-nfs-pvc" -n "$NS" | tail -15; fail "kbench PVC Bound olmadı"; }
oc wait --for=jsonpath='{.status.phase}'=Succeeded "pod/benchmark-kbench-nfs" -n "$NS" --timeout="${KBENCH_TIMEOUT:-1200}s" \
    || { oc describe "pod/benchmark-kbench-nfs" -n "$NS" | tail -40; fail "kbench pod tamamlanmadı"; }
oc logs "pod/benchmark-kbench-nfs" -n "$NS" --tail=30
