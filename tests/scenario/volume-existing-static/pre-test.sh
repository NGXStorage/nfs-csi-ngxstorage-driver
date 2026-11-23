#!/usr/bin/env bash
source "$CSI_TEST_ROOT/lib.sh"
set -euo pipefail
# Run-owned static isolation: create a UNIQUE share for this run and bind the
# CREATE-RESPONSE ID to the rendered PV handle. A pre-existing/baseline share
# (e.g. the checked-in fixture's old fixed name) is NEVER adopted, mutated or
# deleted — a name collision is not ownership proof. If create fails for any
# reason (including AlreadyExists), abort; do not fall back to reuse-by-name.
mkdir -p "$OUT"
# Ensure the render copy exists before binding the handle: run-test renders
# later, but pre-test needs the PV copy NOW. render_test_dir is idempotent
# within the run (.rendered marker), so run-test keeps this substitution.
if ! ls "$RENDER_DIR"/*pv.yaml >/dev/null 2>&1; then
    render_test_dir
fi
EX_NAME="static-run-${STAMP//-/}-$$"
CREATE_OUT=$("$NGX_SDK_CLI" --controllers "$NGX_CONTROLLER" --api-key "$NGX_API_KEY" \
    --pool-name "$NGX_POOL_NAME" create-share "$EX_NAME" 1 2>"$OUT/create-share.err") || {
    echo "[ERROR] run-owned share create failed (no adoption of existing shares): $(cat "$OUT/create-share.err")" >&2
    exit 1; }
EX_ID=$(printf '%s' "$CREATE_OUT" | jq -r '.id // empty')
[ -n "$EX_ID" ] || { echo "[ERROR] create response carried no share ID" >&2; exit 1; }
printf '%s\n' "$EX_ID" > "$OUT/existing-volume.id"
printf '%s\n' "$EX_NAME" > "$OUT/existing-volume.name"
# Bind the create-response identity into the RENDERED PV handle (copy only).
for f in "$RENDER_DIR"/*pv.yaml; do
    [ -f "$f" ] || continue
    sed -i "s|__STATIC_SHARE_NAME__|$EX_NAME|g" "$f"
done
echo "[pre] run-owned share created id=$EX_ID name=$EX_NAME (handle rendered)" >&2
