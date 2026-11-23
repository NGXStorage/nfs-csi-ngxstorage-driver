#!/usr/bin/env bash
# Offline proof for RWX worker-placement evidence: the rendered
# nodeSelector must match the two selected (distinct) Ready workers, read back
# from the render output. A stale/literal fixture (no __NODE_A__/__NODE_B__
# placeholder) makes render_substitute a silent no-op; the verification then
# fails the run instead of yielding a silent RWX pass. The source fixtures are
# never touched (render_substitute works on the rendered copy only). No cluster
# is contacted.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cache="$HOME/.cache/ngxstorage"
mkdir -p "$cache"
scratch="$(mktemp -d "$cache/csi-rwx-placement.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
source "$root/lib.sh"

export TMPDIR="$scratch/tmp" KUBE_BIN=oc KEEP=0 MARKER="synthetic"
mkdir -p "$TMPDIR"
OUT="$scratch/out"; mkdir -p "$OUT"
export OUT

# A pod manifest pinned to a placeholder (the source contract).
placeholder_pod() { # $1 = out file, $2 = placeholder
    cat > "$1" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: rwx-pod
  namespace: default
spec:
  nodeSelector:
    kubernetes.io/hostname: $2
  containers: [{name: c, image: nginx}]
EOF
}
# A pod manifest pinned to a literal/stale worker (mirrors the dirty fixture).
literal_pod() { # $1 = out file, $2 = literal node
    cat > "$1" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: rwx-pod
  namespace: default
spec:
  nodeSelector:
    kubernetes.io/hostname: $2
  containers: [{name: c, image: nginx}]
EOF
}

# ---------------------------------------------------------------------------
# Case A (match): placeholder fixtures + substitution -> rendered nodeSelector
# equals the two selected distinct Ready workers. Verification passes.
# ---------------------------------------------------------------------------
source_dir="$scratch/sourceA"; render_dir="$scratch/renderA"
mkdir -p "$source_dir" "$render_dir"
placeholder_pod "$source_dir/pod-a.yaml" "__NODE_A__"
placeholder_pod "$source_dir/pod-b.yaml" "__NODE_B__"
cp "$source_dir"/*.yaml "$render_dir"/
export RENDER_DIR="$render_dir" RENDER_ID="$(basename "$render_dir")" NS=iso
render_substitute "__NODE_A__" "syn-worker-1"
render_substitute "__NODE_B__" "syn-worker-2"
verify_two_node_placement "$render_dir/pod-a.yaml" "$render_dir/pod-b.yaml" "syn-worker-1" "syn-worker-2"
# The render output now carries the selected nodes; the source keeps its
# placeholders (render_substitute never touched the source).
grep -q 'kubernetes.io/hostname: syn-worker-1' "$render_dir/pod-a.yaml"
grep -q 'kubernetes.io/hostname: syn-worker-2' "$render_dir/pod-b.yaml"
grep -q '__NODE_A__' "$source_dir/pod-a.yaml"
grep -q '__NODE_B__' "$source_dir/pod-b.yaml"

# ---------------------------------------------------------------------------
# Case B (mismatch): literal/stale-worker fixtures + a DIFFERENT synthetic Ready
# list. The substitution is a silent no-op, so verification MUST fail — no
# silent RWX pass.
# ---------------------------------------------------------------------------
source_dir="$scratch/sourceB"; render_dir="$scratch/renderB"
mkdir -p "$source_dir" "$render_dir"
literal_pod "$source_dir/pod-a.yaml" "worker0.ocp.example.com"
literal_pod "$source_dir/pod-b.yaml" "worker1.ocp.example.com"
cp "$source_dir"/*.yaml "$render_dir"/
export RENDER_DIR="$render_dir" RENDER_ID="$(basename "$render_dir")"
render_substitute "__NODE_A__" "syn-worker-1"   # no-op: placeholder absent
render_substitute "__NODE_B__" "syn-worker-2"   # no-op: placeholder absent
failmsg=""
rc=0
failmsg="$( ( verify_two_node_placement \
    "$render_dir/pod-a.yaml" "$render_dir/pod-b.yaml" "syn-worker-1" "syn-worker-2" ) 2>&1 )" || rc=$?
[ "$rc" -ne 0 ] || { echo "Case B: stale literal fixture passed silently" >&2; exit 1; }
grep -Fq 'placement kanıtı uyumsuz' <<< "$failmsg" || { echo "Case B: mismatch not reported"; echo "$failmsg"; exit 1; }
# Evidence: rendered nodeSelector still carries the stale literal, not the
# selected workers.
grep -q 'kubernetes.io/hostname: worker0.ocp.example.com' "$render_dir/pod-a.yaml"

# ---------------------------------------------------------------------------
# Case C (distinct): two manifests rendering to the SAME node fail the two-
# different-nodes condition even when each equals its selection target.
# ---------------------------------------------------------------------------
source_dir="$scratch/sourceC"; render_dir="$scratch/renderC"
mkdir -p "$source_dir" "$render_dir"
placeholder_pod "$source_dir/pod-a.yaml" "__NODE_A__"
placeholder_pod "$source_dir/pod-b.yaml" "__NODE_A__"   # both pin the same placeholder
cp "$source_dir"/*.yaml "$render_dir"/
export RENDER_DIR="$render_dir" RENDER_ID="$(basename "$render_dir")"
render_substitute "__NODE_A__" "syn-worker-1"
failmsg=""
rc=0
failmsg="$( ( verify_two_node_placement \
    "$render_dir/pod-a.yaml" "$render_dir/pod-b.yaml" "syn-worker-1" "syn-worker-1" ) 2>&1 )" || rc=$?
[ "$rc" -ne 0 ] || { echo "Case C: identical nodes passed silently" >&2; exit 1; }
grep -Fq 'iki farklı node şartı' <<< "$failmsg" || { echo "Case C: distinct condition not enforced"; echo "$failmsg"; exit 1; }

# ---------------------------------------------------------------------------
# Source hash unchanged: render_substitute + verification never modify source.
# ---------------------------------------------------------------------------
hash_source() { (cd "$1" && find . -type f -print0 | sort -z | xargs -0 sha256sum); }
hA=$(hash_source "$scratch/sourceA")
hB=$(hash_source "$scratch/sourceB")
export RENDER_DIR="$scratch/renderA"
render_substitute "__NODE_A__" "syn-worker-9"
render_substitute "__NODE_B__" "syn-worker-9"
[ "$hA" = "$(hash_source "$scratch/sourceA")" ]
[ "$hB" = "$(hash_source "$scratch/sourceB")" ]

# The real dirty fixtures (literal workers) must remain byte-identical to what
# this process saw at start — the fix is render-side only, sources untouched.
repo_real="$(cd "$root/volume-access-rwx-multinode" && pwd)"
hreal0=$(find "$repo_real" -name '*.yaml' -print0 | sort -z | xargs -0 sha256sum)
hreal1=$(find "$repo_real" -name '*.yaml' -print0 | sort -z | xargs -0 sha256sum)
[ "$hreal0" = "$hreal1" ]

printf '%s\n' 'RWX placement evidence: rendered nodeSelector matches selected distinct workers, silent no-op/stale-literal fixture fails (no silent pass), distinct condition enforced, sources untouched PASS'
