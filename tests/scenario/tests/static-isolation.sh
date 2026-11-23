#!/usr/bin/env bash
# Offline negative proof for NFS static run-owned isolation (review P1-3):
#  - pre-test creates a UNIQUE run-owned share and binds the CREATE-RESPONSE
#    ID into the rendered PV handle (copy only);
#  - a create failure (e.g. AlreadyExists) ABORTS — no reuse-by-name, no
#    adoption/mutation/deletion of a pre-existing baseline/foreign share;
#  - post-test deletes ONLY the run-owned ID;
#  - a baseline/foreign share is never mutated (readonly flip) or deleted.
# No cluster/array is contacted; the SDK CLI is a recording fake.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cache="$HOME/.cache/ngxstorage"
mkdir -p "$cache"
scratch="$(mktemp -d "$cache/csi-static-isolation.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT

CALLS="$scratch/calls"
: > "$CALLS"
export CALLS
# Fake CLI: create-share succeeds with a run-owned ID unless the name is the
# reserved foreign/baseline name (then AlreadyExists). delete-share refuses
# the foreign ID. Everything is recorded.
cat > "$scratch/ngxstorage-cli" <<'EOF'
#!/usr/bin/env bash
# strip leading --controllers/--api-key/--pool-name pairs
args=()
while [ $# -gt 0 ]; do
  case "$1" in
    --controllers|--api-key|--pool-name) shift 2 ;;
    *) args+=("$1"); shift ;;
  esac
done
cmd="${args[0]:-}"
printf '%s\n' "${args[*]}" >> "$CALLS"
case "$cmd" in
  create-share)
    name="${args[1]}"
    if [ "$name" = "volume-existing-static_test" ]; then
      echo 'Error: share already exists (AlreadyExists)' >&2; exit 1
    fi
    if [ "${CREATE_FAIL:-0}" = 1 ]; then
      echo 'Error: simulated create failure' >&2; exit 1
    fi
    printf '{"id":"run-owned-id-123","name":"%s"}\n' "$name" ;;
  shares)
    printf '[{"id":"baseline-foreign-id-999","name":"volume-existing-static_test"}]\n' ;;
  set-share-readonly)
    [ "${args[1]}" != "baseline-foreign-id-999" ] || { echo 'refused: foreign share' >&2; exit 1; }
    printf '{"success":true}\n' ;;
  delete-share)
    [ "${args[1]}" != "baseline-foreign-id-999" ] || { echo 'refused: foreign share' >&2; exit 1; }
    printf '{"success":true}\n' ;;
  *) echo "unexpected cmd $cmd" >&2; exit 99 ;;
esac
EOF
chmod +x "$scratch/ngxstorage-cli"

export NGX_SDK_CLI="$scratch/ngxstorage-cli"
export NGX_CONTROLLER=fixture NGX_API_KEY=fixture NGX_POOL_NAME=csitest3
export STAMP="2026-01-01-000000" KEEP=0
export CSI_TEST_ROOT="$root"

setup_render() { # $1 = scenario dir name, $2 = label (default $1)
  local label="${2:-$1}"
  RENDER_DIR="$scratch/render-$label"
  OUT="$scratch/out-$label"
  mkdir -p "$RENDER_DIR" "$OUT"
  cp "$root/$1/"*pv.yaml "$RENDER_DIR/"
  export RENDER_DIR OUT
}

fail() { echo "FAIL: $*" >&2; exit 1; }

# --- writable static: happy path -------------------------------------------
setup_render volume-existing-static
bash "$root/volume-existing-static/pre-test.sh" 2>"$scratch/pre.err" || { cat "$scratch/pre.err"; fail "pre-test happy path"; }
[ "$(cat "$OUT/existing-volume.id")" = "run-owned-id-123" ] || fail "create-response ID not bound"
NAME=$(cat "$OUT/existing-volume.name")
case "$NAME" in static-run-*) : ;; *) fail "share name not run-unique: $NAME" ;; esac
grep -q "nfs.csi.ngxstorage.com.$NAME" "$RENDER_DIR/"*pv.yaml || fail "rendered handle not bound to run-owned name"
grep -q "__STATIC_SHARE_NAME__" "$RENDER_DIR/"*pv.yaml && fail "placeholder left in rendered handle"
# source fixture must keep the placeholder (byte-preservation of the source)
grep -q "__STATIC_SHARE_NAME__" "$root/volume-existing-static/"*pv.yaml || fail "source fixture handle was modified"
# no foreign mutation during pre
grep -q "baseline-foreign-id-999" "$CALLS" && fail "foreign share touched during pre"
bash "$root/volume-existing-static/post-test.sh" 2>"$scratch/post.err" || { cat "$scratch/post.err"; fail "post-test happy path"; }
grep -q "delete-share run-owned-id-123" "$CALLS" || fail "run-owned share not deleted by post"
grep -q "delete-share baseline-foreign-id-999" "$CALLS" && fail "foreign share deleted"

# --- writable static: create failure must abort, never adopt ---------------
setup_render volume-existing-static static-fail
: > "$CALLS"
CREATE_FAIL=1 bash "$root/volume-existing-static/pre-test.sh" 2>"$scratch/pre-fail.err" && fail "pre-test survived create failure"
grep -q "no adoption" "$scratch/pre-fail.err" || fail "abort message missing"
[ ! -f "$OUT/existing-volume.id" ] || fail "ID recorded despite failed create"
grep -q "shares" "$CALLS" && fail "reuse-by-name lookup attempted after create failure"
grep -q "set-share-readonly\|delete-share" "$CALLS" && fail "mutation attempted after create failure"

# --- readonly static: pre creates writable run-owned share, no mutation ----
# The RO flip belongs to run-test (phase A needs a writable publish); pre-test
# must only create. run-test must flip ONLY the run-owned ID from the create
# response — verified at source level below.
setup_render volume-existing-readonly-static
: > "$CALLS"
bash "$root/volume-existing-readonly-static/pre-test.sh" 2>"$scratch/pre-ro.err" || { cat "$scratch/pre-ro.err"; fail "readonly pre-test"; }
[ "$(cat "$OUT/existing-volume.id")" = "run-owned-id-123" ] || fail "readonly create-response ID not bound"
grep -q "set-share-readonly" "$CALLS" && fail "pre-test flipped RO early (phase A needs writable publish)"
grep -q "baseline-foreign-id-999" "$CALLS" && fail "foreign share touched in readonly pre"
grep -q "nfs.csi.ngxstorage.com.$(cat "$OUT/existing-volume.name")" "$RENDER_DIR/"*pv.yaml || fail "readonly rendered handle not bound"
# run-test flips RO only on the run-owned ID read from the create-response file
grep -q 'EX_ID=$(cat "$OUT/existing-volume.id")' "$root/volume-existing-readonly-static/run-test.sh" || fail "run-test does not bind the create-response ID"
grep -q 'sdk set-share-readonly "$EX_ID" true' "$root/volume-existing-readonly-static/run-test.sh" || fail "run-test RO flip does not target the run-owned ID"
bash "$root/volume-existing-readonly-static/post-test.sh" 2>"$scratch/post-ro.err" || { cat "$scratch/post-ro.err"; fail "readonly post-test"; }
grep -q "delete-share run-owned-id-123" "$CALLS" || fail "readonly run-owned share not deleted"
grep -q "baseline-foreign-id-999" "$CALLS" && fail "foreign share touched in readonly flow"

printf '%s\n' 'NFS static run-owned isolation (create-response binding, no adoption, foreign never mutated/deleted, source preserved) PASS'
