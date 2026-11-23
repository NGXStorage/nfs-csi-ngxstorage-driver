#!/usr/bin/env bash
# Offline negative tests for the single open runtime profile contract:
# missing/partial profiles must fail preflight; a complete profile (config.env
# and/or exported environment) must pass. Synthetic values only — no real
# credential is ever read or attempted.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cache="$HOME/.cache/ngxstorage"
mkdir -p "$cache"
scratch="$(mktemp -d "$cache/csi-sanity-profile.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
config="$scratch/config.env"

clean_env() {
    env -u STORAGE_API_KEY -u STORAGE_IPS -u STORAGE_POOL_NAME -u STORAGE_DATA_IP "$@"
}

expect_fail() { # $1 = name, $2... = keys that must be reported missing
    local name="$1"; shift
    local msg rc=0
    msg="$(clean_env bash -c "source '$root/local/runtime-profile.sh'; sanity_profile_preflight '$config'" 2>&1)" || rc=$?
    [ "$rc" -eq 2 ] || { echo "$name: expected rc=2, got rc=$rc"; echo "$msg"; exit 1; }
    for key in "$@"; do
        grep -Fq "$key" <<< "$msg" || { echo "$name: $key not reported as missing"; echo "$msg"; exit 1; }
    done
}

expect_pass() { # $1 = name
    clean_env bash -c "source '$root/local/runtime-profile.sh'; sanity_profile_preflight '$config' || exit 2" \
        || { echo "$1: complete profile was rejected"; exit 1; }
}

# 1. Missing pool: credential/endpoints/data-IP present, pool absent.
cat > "$config" <<EOF
STORAGE_API_KEY="synthetic-not-a-real-key"
STORAGE_IPS="192.0.2.1"
STORAGE_DATA_IP="198.51.100.2"
EOF
expect_fail "missing-pool" STORAGE_POOL_NAME

# 2. Missing credential.
cat > "$config" <<EOF
STORAGE_IPS="192.0.2.1"
STORAGE_POOL_NAME="synthetic-pool"
STORAGE_DATA_IP="198.51.100.2"
EOF
expect_fail "missing-credential" STORAGE_API_KEY

# 3. Fragmented profile: key present, address/pool/data-IP absent.
printf 'STORAGE_API_KEY="synthetic-not-a-real-key"\n' > "$config"
expect_fail "fragmented-profile" STORAGE_IPS STORAGE_POOL_NAME STORAGE_DATA_IP

# 4. No profile at all: neither file nor environment.
rm -f "$config"
expect_fail "absent-profile" STORAGE_API_KEY STORAGE_IPS STORAGE_POOL_NAME STORAGE_DATA_IP

# 5. Complete config.env passes.
cat > "$config" <<EOF
STORAGE_API_KEY="synthetic-not-a-real-key"
STORAGE_IPS="192.0.2.1,192.0.2.2"
STORAGE_POOL_NAME="synthetic-pool"
STORAGE_DATA_IP="198.51.100.2"
EOF
expect_pass "complete-config-env"

# 6. Environment-only profile passes without any config file.
rm -f "$config"
clean_env bash -c '
    export STORAGE_API_KEY="synthetic" STORAGE_IPS="192.0.2.1" \
        STORAGE_POOL_NAME="synthetic-pool" STORAGE_DATA_IP="198.51.100.2"
    source "$1"
    sanity_profile_preflight /nonexistent/config.env || exit 2
' _ "$root/local/runtime-profile.sh" || { echo "env-only profile was rejected"; exit 1; }

# 7. Regression pins: the markdown-scraping fallback channel and the silent
# stale-stack defaults must stay gone from the local entry point.
! grep -q 'V6BGWYPA_A' "$root/run.sh"
! grep -q 'test-infrastructure.md' "$root/run.sh"
! grep -q 'infra_file' "$root/run.sh"
! grep -qE 'STORAGE_(IPS|POOL_NAME|DATA_IP)=\$\{STORAGE_(IPS|POOL_NAME|DATA_IP):-' "$root/run.sh"

# 8. THE regression (file mode is authoritative): a PARTIAL config.env masked
# by a FULL inherited environment must be REJECTED with rc=2. The old code
# sourced the file on top of the env and only checked non-empty, so this
# mixture passed preflight and could connect with a credential for one array
# against the endpoints of another. preflight makes no live call — it is pure
# file/env logic, so a rejection here guarantees zero live access.
expect_reject_inherited() { # $1 = name; uses $root/$config globals from the test
    local name="$1"
    local rc=0 msg
    msg="$(bash -c '
        export STORAGE_IPS="192.0.2.9" STORAGE_POOL_NAME="stale-pool" \
            STORAGE_DATA_IP="198.51.100.9" STORAGE_API_KEY="stale-key"
        source "'"$root"'/local/runtime-profile.sh"
        sanity_profile_preflight "'"$config"'"
    ' 2>&1)" || rc=$?
    [ "$rc" -eq 2 ] || { echo "$name: expected rc=2, got rc=$rc"; echo "$msg"; exit 1; }
    grep -Fq 'incomplete' <<< "$msg" || { echo "$name: no rejection message"; echo "$msg"; exit 1; }
    # Credential values must never be logged — only key names.
    ! grep -Fq 'stale-key' <<< "$msg" || { echo "$name: credential value leaked into the log"; echo "$msg"; exit 1; }
}
printf 'STORAGE_API_KEY="synthetic-only-key"\n' > "$config"
expect_reject_inherited "partial-file-full-env"

# 9. A complete config.env wins over a full stale env (file mode authoritative):
# the file's values are exported, not the environment's.
cat > "$config" <<EOF
STORAGE_API_KEY="synthetic-not-a-real-key"
STORAGE_IPS="192.0.2.1"
STORAGE_POOL_NAME="synthetic-pool"
STORAGE_DATA_IP="198.51.100.2"
EOF
got="$(bash -c '
    export STORAGE_IPS="192.0.2.9" STORAGE_POOL_NAME="stale" \
        STORAGE_DATA_IP="198.51.100.9" STORAGE_API_KEY="stale"
    source "'"$root"'/local/runtime-profile.sh"
    sanity_profile_preflight "'"$config"'" || exit 2
    printf "%s" "$STORAGE_POOL_NAME"
' _)" || { echo "complete-file-over-env: full file was rejected"; exit 1; }
[ "$got" = "synthetic-pool" ] || { echo "complete-file-over-env: env leaked in, got '$got'"; exit 1; }

# 10. A ${VAR:-} template line in config.env inherits NOTHING in file mode:
# the file must supply literal values.
printf 'STORAGE_API_KEY="${STORAGE_API_KEY:-}"\n' > "$config"
expect_reject_inherited "template-inherits-nothing"

# 11. ENV MODE: no file, partial env (one key missing) must be rejected.
rm -f "$config"
rc=0 msg=""
msg="$(clean_env bash -c '
    export STORAGE_API_KEY="synthetic" STORAGE_IPS="192.0.2.1" STORAGE_POOL_NAME="synthetic-pool"
    source "'"$root"'/local/runtime-profile.sh"
    sanity_profile_preflight /nonexistent/config.env
' _ 2>&1)" || rc=$?
[ "$rc" -eq 2 ] || { echo "env-mode-partial: expected rc=2, got rc=$rc"; echo "$msg"; exit 1; }
grep -Fq 'STORAGE_DATA_IP' <<< "$msg" || { echo "env-mode-partial: STORAGE_DATA_IP not reported"; echo "$msg"; exit 1; }
! grep -Fq 'synthetic' <<< "$msg" || { echo "env-mode-partial: credential value leaked"; echo "$msg"; exit 1; }

# 12. a ${VAR:-fallback} template file validates in the cleared scope but
# the OLD implementation re-sourced it in the parent environment, where the
# template picked the STALE inherited values — loading something different
# from what was verified. Load and validate must happen in the same cleared
# scope: all four loaded fields equal the FILE values, none inherited.
cat > "$config" <<EOF
STORAGE_API_KEY="\${STORAGE_API_KEY:-file-key}"
STORAGE_IPS="\${STORAGE_IPS:-192.0.2.1}"
STORAGE_POOL_NAME="\${STORAGE_POOL_NAME:-file-pool}"
STORAGE_DATA_IP="\${STORAGE_DATA_IP:-198.51.100.2}"
EOF
loaded="$(bash -c '
    export STORAGE_IPS="192.0.2.9" STORAGE_POOL_NAME="stale-pool" \
        STORAGE_DATA_IP="198.51.100.9" STORAGE_API_KEY="stale-key"
    source "'"$root"'/local/runtime-profile.sh"
    sanity_profile_preflight "'"$config"'" || exit 2
    printf "%s|%s|%s|%s" "$STORAGE_IPS" "$STORAGE_POOL_NAME" "$STORAGE_DATA_IP" "$STORAGE_API_KEY"
' _)" || { echo "fallback-file: complete template file was rejected"; exit 1; }
[ "$loaded" = "192.0.2.1|file-pool|198.51.100.2|file-key" ] \
    || { echo "fallback-file: loaded profile is not the validated file profile: $loaded"; exit 1; }

# 13. a config.env that fails to source (syntax error / early exit) must
# be rejected before any live call — never converted to success.
printf 'STORAGE_API_KEY="synthetic"\nthis is not valid shell ((\n' > "$config"
rc=0 msg=""
msg="$(clean_env bash -c "source '$root/local/runtime-profile.sh'; sanity_profile_preflight '$config'" 2>&1)" || rc=$?
[ "$rc" -eq 2 ] || { echo "broken-config: syntax-failing file was accepted (rc=$rc)"; echo "$msg"; exit 1; }

# 14. V3: the config file is sourced EXACTLY ONCE. A synthetic config counts
# its own executions; extra stdout must not shift the four transferred
# values.
counter="$scratch/count"
cat > "$config" <<EOF
echo "executed" >> "$counter"
STORAGE_API_KEY="file-key"
STORAGE_IPS="192.0.2.1"
STORAGE_POOL_NAME="file-pool"
STORAGE_DATA_IP="198.51.100.2"
EOF
: > "$counter"
got="$(bash -c '
    export STORAGE_IPS="192.0.2.9" STORAGE_POOL_NAME="stale" \
        STORAGE_DATA_IP="198.51.100.9" STORAGE_API_KEY="stale"
    source "'"$root"'/local/runtime-profile.sh"
    sanity_profile_preflight "'"$config"'" || exit 2
    printf "%s|%s|%s|%s" "$STORAGE_IPS" "$STORAGE_POOL_NAME" "$STORAGE_DATA_IP" "$STORAGE_API_KEY"
' _)" || { echo "once-source: complete file was rejected"; exit 1; }
[ "$got" = "192.0.2.1|file-pool|198.51.100.2|file-key" ] \
    || { echo "once-source: config stdout shifted the profile: $got"; exit 1; }
[ "$(wc -l < "$counter")" -eq 1 ] \
    || { echo "once-source: config executed $(wc -l < "$counter") times, want 1"; exit 1; }

# 15. V3: a failing config that writes its API key to stderr must not leak
# the value into the diagnostic — the fixed message names keys, never values.
leaky="$scratch/leaky-value-synthetic"
printf 'STORAGE_API_KEY="%s"\nSTORAGE_IPS="x"\nSTORAGE_POOL_NAME="y"\nSTORAGE_DATA_IP="z"\necho "key=%s" >&2\nexit 1\n' "$leaky" "$leaky" > "$config"
rc=0 msg=""
msg="$(clean_env bash -c "source '$root/local/runtime-profile.sh'; sanity_profile_preflight '$config'" 2>&1)" || rc=$?
[ "$rc" -eq 2 ] || { echo "stderr-leak: expected rc=2, got rc=$rc"; exit 1; }
if grep -Fq "$leaky" <<< "$msg"; then
    echo "stderr-leak: config stderr content reached the diagnostic"; echo "$msg"; exit 1
fi

# 16. V3: syntax-failure and early-exit configs both fail with rc=2 and a
# safe diagnostic (no raw bash error text from the file).
printf 'STORAGE_API_KEY="synthetic"\nthis is (( not valid shell\n' > "$config"
rc=0 msg=""
msg="$(clean_env bash -c "source '$root/local/runtime-profile.sh'; sanity_profile_preflight '$config'" 2>&1)" || rc=$?
[ "$rc" -eq 2 ] || { echo "syntax-fail: expected rc=2, got rc=$rc"; exit 1; }
grep -Fq 'not valid shell' <<< "$msg" && { echo "syntax-fail: raw config error leaked"; exit 1; }

printf '%s\n' 'Sanity runtime-profile preflight (single cleared-scope source, data/diagnostic channel separation, template/stale-env and broken-file rejection, no credential logging) PASS'
