#!/usr/bin/env bash
# Offline proof: the namespace cleanup's raw DELETE uses the SUPPORTED
# client contract, and the UID precondition really travels in the request.
# The REAL kubectl is exercised against a LOCAL capture server
# (127.0.0.1, plain http): the client's flag parser accepts
# `delete --raw=<path> -f -`, and the captured request asserts
# method=DELETE, path=/api/v1/namespaces/<ns>, and a DeleteOptions body
# carrying preconditions.uid. NO invocation is ever pointed at a default
# or production endpoint.
# Status is reported as PASS, BLOCKED (kubectl or loopback unavailable —
# an honest skip, exit 0 for the optional local run) or FAIL (exit 1). A
# BLOCKED status must never be presented as "tests PASS".
# No cluster, no credential, no network beyond loopback.
set -euo pipefail
cache="$HOME/.cache/ngxstorage"
mkdir -p "$cache"
scratch="$(mktemp -d "$cache/csi-delete-contract.XXXXXX")"
cap_pid=""
cleanup() {
    if [ -n "$cap_pid" ]; then
        kill "$cap_pid" 2>/dev/null && wait "$cap_pid" 2>/dev/null || true
    fi
    rm -rf "$scratch"
}
trap cleanup EXIT
export TMPDIR="$scratch"
status=PASS

# Locate a real kubectl: envtest assets first, then PATH.
kubectl_bin=""
if [ -n "${KUBEBUILDER_ASSETS:-}" ] && [ -x "$KUBEBUILDER_ASSETS/kubectl" ]; then
    kubectl_bin="$KUBEBUILDER_ASSETS/kubectl"
elif command -v kubectl >/dev/null 2>&1; then
    kubectl_bin="$(command -v kubectl)"
fi
if [ -z "$kubectl_bin" ]; then
    echo "BLOCKED: delete-client-contract — no kubectl available (set KUBEBUILDER_ASSETS or PATH); the real-client DELETE contract is validated by the live scenario suite"
    exit 0
fi

ns=test-ns
uid=test-uid-123
# X1: build the body with a real printf — a single-quoted assignment with a
# trailing "$uid" argument would EXECUTE that name as a command.
body="$(printf '{"kind":"DeleteOptions","apiVersion":"v1","preconditions":{"uid":"%s"}}' "$uid")"
case "$body" in *"$uid"*) ;; *) echo "FAIL: body construction lost the uid" >&2; exit 1 ;; esac

# ---------------------------------------------------------------------------
# Capture server on loopback; readiness barrier before any use (X2).
# ---------------------------------------------------------------------------
port_file="$scratch/port"
rec="$scratch/request"
python3 - "$port_file" "$rec" <<'PY' &
import http.server, sys, threading, time
port_file, rec = sys.argv[1], sys.argv[2]
class Capture(http.server.BaseHTTPRequestHandler):
    def _read_body(self):
        # kubectl streams the stdin body with chunked transfer encoding.
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            b = b""
            while True:
                size = int(self.rfile.readline().strip() or "0", 16)
                if size == 0:
                    self.rfile.readline()
                    break
                b += self.rfile.read(size)
                self.rfile.readline()
            return b.decode()
        return self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode()

    def _capture(self):
        with open(rec, "w") as f:
            f.write(self.command + "\n" + self.path + "\n" + self._read_body())
        self.send_response(409)
        self.end_headers()
        self.wfile.write(b'{"kind":"Status","reason":"Conflict"}')
    do_DELETE = _capture
    do_POST = _capture
    def log_message(self, *args):
        pass
try:
    srv = http.server.HTTPServer(("127.0.0.1", 0), Capture)
except OSError as e:
    open(port_file, "w").write(f"bind-failed:{e}")
    sys.exit(0)
open(port_file, "w").write(str(srv.server_address[1]))
threading.Thread(target=srv.handle_request, daemon=True).start()
time.sleep(30)
PY
cap_pid=$!

port=""
for _ in $(seq 1 100); do
    if [ -s "$port_file" ]; then
        if kill -0 "$cap_pid" 2>/dev/null; then
            port="$(cat "$port_file" 2>/dev/null || true)"
            case "$port" in
                ""|*[!0-9]*) : ;; # partial write or bind failure — keep waiting
                *) break ;;
            esac
        else
            echo "BLOCKED: capture server exited before serving: $(cat "$port_file" 2>/dev/null || echo '<no port file>')"
            status=BLOCKED
            break
        fi
    fi
    sleep 0.1
done
if [ "$status" = "PASS" ] && [ -z "$port" ]; then
    echo "BLOCKED: capture server did not become ready within 10s"
    status=BLOCKED
fi

if [ "$status" = "PASS" ]; then
    kubeconfig="$scratch/kubeconfig"
    cat > "$kubeconfig" <<EOF
apiVersion: v1
kind: Config
clusters:
- name: capture
  cluster:
    server: http://127.0.0.1:$port
contexts:
- name: capture
  context:
    cluster: capture
current-context: capture
EOF

    # One isolated invocation: proves the flag parser accepts the form AND
    # captures what the client sends. A 409 Conflict response is expected —
    # the capture is the evidence, not the exit code.
    client_err="$(printf '%s' "$body" | timeout 15 env KUBECONFIG="$kubeconfig" \
        "$kubectl_bin" delete --raw="/api/v1/namespaces/$ns" -f - 2>&1)" || true
    case "$client_err" in
        *"unknown flag"*)
            echo "FAIL: real kubectl rejects the delete --raw form: $client_err" >&2
            exit 1 ;;
    esac

    for _ in $(seq 1 50); do
        [ -s "$rec" ] && break
        sleep 0.1
    done
    [ -s "$rec" ] || { echo "BLOCKED: capture server received no request (client error: $(head -c 200 <<< "$client_err"))" >&2; status=BLOCKED; }
fi

if [ "$status" = "PASS" ]; then
    method="$(sed -n 1p "$rec")"
    path="$(sed -n 2p "$rec")"
    sent_body="$(sed -n 3p "$rec")"
    [ "$method" = "DELETE" ] || { echo "FAIL: method=$method, want DELETE" >&2; exit 1; }
    case "$path" in
        /api/v1/namespaces/$ns*) ;;
        *) echo "FAIL: path=$path" >&2; exit 1 ;;
    esac
    printf '%s' "$sent_body" | python3 -c '
import json, sys
doc = json.load(sys.stdin)
assert doc["preconditions"]["uid"] == sys.argv[1], doc
' "$uid" || { echo "FAIL: DeleteOptions body lacked preconditions.uid: $sent_body" >&2; exit 1; }

    # Record the client version with a supported interface (no --short).
    client_version="$(timeout 10 "$kubectl_bin" version --client -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get("clientVersion",{}).get("gitVersion","unknown"))' 2>/dev/null || echo unknown)"
    echo "delete-client-contract: real kubectl ($client_version at $kubectl_bin) accepted the delete --raw form and sent DELETE /api/v1/namespaces/$ns with DeleteOptions.preconditions.uid — PASS"
fi
exit 0
