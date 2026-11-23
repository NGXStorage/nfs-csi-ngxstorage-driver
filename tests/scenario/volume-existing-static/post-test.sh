#!/usr/bin/env bash
source "$CSI_TEST_ROOT/lib.sh"
set -uo pipefail
# Test-owned cleanup: delete ONLY the share this run created (ID from the
# create response). Never touch any other share. --keep retains it.
[ -f "$OUT/existing-volume.id" ] || exit 0
EX_ID=$(cat "$OUT/existing-volume.id")
[ "$KEEP" = "1" ] && { echo "[post] keeping run-owned share id=$EX_ID (--keep)" >&2; exit 0; }
if "$NGX_SDK_CLI" --controllers "$NGX_CONTROLLER" --api-key "$NGX_API_KEY" \
    --pool-name "$NGX_POOL_NAME" delete-share "$EX_ID" >/dev/null 2>"$OUT/delete-share.err"; then
    echo "[post] run-owned share deleted id=$EX_ID" >&2
else
    echo "[post] DRIVER CLEANUP FAILURE: run-owned share delete failed id=$EX_ID: $(cat "$OUT/delete-share.err")" >&2
    exit 1
fi
