package helper

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

type snapshotRoundTripper func(*http.Request) (*http.Response, error)

func (f snapshotRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// newSnapshotTestReconciler builds a Reconciler whose REAL SDK client points
// at a fake NGX API, so SnapshotByID is exercised through the actual
// helper/SDK error-and-decode path.
func newSnapshotTestReconciler(t *testing.T, handler http.HandlerFunc) *Reconciler {
	t.Helper()
	wrap := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/status/cluster"):
			w.Write([]byte(`{"connected":1,"status":"Master","peers":[]}`))
		case strings.HasSuffix(r.URL.Path, "/api/v2/pool"):
			w.Write([]byte(`[{"id":"pool1","name":"pool1","owner":"o"}]`))
		case strings.HasSuffix(r.URL.Path, "/api/v2/pool/pool1"):
			w.Write([]byte(`{"id":"pool1","name":"pool1","owner":"o"}`))
		default:
			handler(w, r)
		}
	})
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: wrap}}
	server.Start()
	t.Cleanup(server.Close)
	transport := snapshotRoundTripper(func(req *http.Request) (*http.Response, error) {
		req.URL.Scheme = "http"
		req.URL.Host = server.Listener.Addr().String()
		return http.DefaultTransport.RoundTrip(req)
	})
	client, err := ngxstorage.NewClient(ngxstorage.Config{
		Controllers: []string{"127.0.0.1"},
		APIKey:      "test-key",
		PoolName:    "pool1",
		HTTPClient:  &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatalf("ngxstorage.NewClient: %v", err)
	}
	return &Reconciler{Client: client}
}

// TestSnapshotByIDValidation pins GetSnapshot's record validation to the same
// surface as ListSnapshots/CreateSnapshot: a malformed canonical record is an
// Internal error, and a valid record carries SizeBytes and CreationTime.
func TestSnapshotByIDValidation(t *testing.T) {
	cases := []struct{ name, body, wantErr string }{
		{"complete record", `[{"id":"snap-1","name":"s","volume_id":"vol-1","reserved":"1073741824","created":"1700000000"}]`, ""},
		{"missing reserved", `[{"id":"snap-1","name":"s","volume_id":"vol-1","reserved":"","created":"1700000000"}]`, "invalid reserved"},
		{"negative reserved", `[{"id":"snap-1","name":"s","volume_id":"vol-1","reserved":"-5","created":"1700000000"}]`, "invalid reserved"},
		{"invalid created", `[{"id":"snap-1","name":"s","volume_id":"vol-1","reserved":"1073741824","created":""}]`, "invalid created"},
		{"missing volume_id", `[{"id":"snap-1","name":"s","volume_id":"","reserved":"1073741824","created":"1700000000"}]`, "missing volume_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newSnapshotTestReconciler(t, func(w http.ResponseWriter, req *http.Request) {
				w.Write([]byte(tc.body))
			})
			snap, err := r.SnapshotByID(context.Background(), "snap-1")
			if tc.wantErr != "" {
				if err == nil || status.Code(err) != codes.Internal || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("want Internal %q, got snap=%v err=%v", tc.wantErr, snap, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("SnapshotByID: %v", err)
			}
			if snap.GetSizeBytes() != 1073741824 || snap.GetCreationTime() == nil || !snap.GetReadyToUse() {
				t.Fatalf("snapshot surface = %+v, want SizeBytes+CreationTime+ReadyToUse", snap)
			}
		})
	}
	t.Run("absent snapshot is nil without error", func(t *testing.T) {
		r := newSnapshotTestReconciler(t, func(w http.ResponseWriter, req *http.Request) {
			w.Write([]byte(`[]`))
		})
		snap, err := r.SnapshotByID(context.Background(), "snap-1")
		if err != nil || snap != nil {
			t.Fatalf("want nil,nil got snap=%v err=%v", snap, err)
		}
	})
}

// newShareVerifyTestReconciler builds a Reconciler whose REAL SDK client
// points at a fake NGX API. failTransport, when non-nil, injects a transport
// error for matching requests before they reach the server, so the
// helper/SDK error path is exercised end to end.
func newShareVerifyTestReconciler(t *testing.T, timeout, interval time.Duration, failTransport func(*http.Request) bool, handler http.HandlerFunc) *Reconciler {
	t.Helper()
	wrap := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/status/cluster"):
			w.Write([]byte(`{"connected":1,"status":"Master","peers":[]}`))
		case strings.HasSuffix(r.URL.Path, "/api/v2/pool"):
			w.Write([]byte(`[{"id":"pool1","name":"pool1","owner":"o"}]`))
		case strings.HasSuffix(r.URL.Path, "/api/v2/pool/pool1"):
			w.Write([]byte(`{"id":"pool1","name":"pool1","owner":"o"}`))
		default:
			handler(w, r)
		}
	})
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: wrap}}
	server.Start()
	t.Cleanup(server.Close)
	transport := snapshotRoundTripper(func(req *http.Request) (*http.Response, error) {
		if failTransport != nil && failTransport(req) {
			return nil, fmt.Errorf("injected transport failure for %s", req.URL.Path)
		}
		req.URL.Scheme = "http"
		req.URL.Host = server.Listener.Addr().String()
		return http.DefaultTransport.RoundTrip(req)
	})
	client, err := ngxstorage.NewClient(ngxstorage.Config{
		Controllers: []string{"127.0.0.1"},
		APIKey:      "test-key",
		PoolName:    "pool1",
		HTTPClient:  &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatalf("ngxstorage.NewClient: %v", err)
	}
	return &Reconciler{Client: client, DeleteVerifyTimeout: timeout, DeleteVerifyInterval: interval}
}

// TestVerifyShareDeletedHTTPErrorNotAbsence pins the narrow safety fix on the
// REAL helper/SDK path (SDK client against a fake NGX API): an HTTP error
// status must never be converted into absence-success through the NotFound
// code path. Only an authoritative HTTP 404 carrying the canonical NotFound
// code (5011) or no code proves absence. A 404 with a different explicit
// code, any non-404 error status (even one carrying code 5011), or a
// transport failure stays ambiguous and fails closed.
func TestVerifyShareDeletedHTTPErrorNotAbsence(t *testing.T) {
	notAbsence := []struct {
		name   string
		status int
		body   string
	}{
		{"404 with AlreadyExists code", 404, `{"code":710,"error":"exists"}`},
		{"404 with AttachedBusy code", 404, `{"code":534,"error":"busy"}`},
		{"404 with unknown explicit code", 404, `{"code":9999,"error":"other"}`},
		{"403 carrying NotFound code", 403, `{"code":5011,"error":"forbidden"}`},
		{"403 forbidden", 403, `{"code":5013,"error":"forbidden"}`},
		{"500 carrying NotFound code", 500, `{"code":5011,"error":"internal"}`},
		{"500 internal", 500, `{"code":5000,"error":"internal"}`},
		{"503 unavailable", 503, `{"error":"unavailable"}`},
	}
	for _, tc := range notAbsence {
		t.Run(tc.name, func(t *testing.T) {
			gets := 0
			r := newShareVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond, nil,
				func(w http.ResponseWriter, req *http.Request) {
					gets++
					w.WriteHeader(tc.status)
					w.Write([]byte(tc.body))
				})
			err := r.VerifyShareDeleted(context.Background(), "share-1")
			if err == nil {
				t.Fatalf("HTTP %d body %s was converted to absence-success", tc.status, tc.body)
			}
			if status.Code(err) != codes.DeadlineExceeded || !strings.Contains(err.Error(), "absence NOT proven") {
				t.Fatalf("want fail-closed DeadlineExceeded 'absence NOT proven', got %v", err)
			}
			if gets < 1 {
				t.Fatalf("expected bounded verification GETs, got %d", gets)
			}
		})
	}
	t.Run("persistent transport failure is not absence", func(t *testing.T) {
		r := newShareVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond,
			func(req *http.Request) bool {
				return req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/api/v2/share/share-1")
			},
			func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"code":5011,"error":"not found"}`))
			})
		err := r.VerifyShareDeleted(context.Background(), "share-1")
		if err == nil || status.Code(err) != codes.DeadlineExceeded || !strings.Contains(err.Error(), "absence NOT proven") {
			t.Fatalf("persistent transport must fail closed, got %v", err)
		}
	})
	t.Run("authoritative 404 controls still succeed", func(t *testing.T) {
		for _, body := range []string{`{"code":5011,"error":"not found"}`, `{"error":"not found"}`} {
			r := newShareVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond, nil,
				func(w http.ResponseWriter, req *http.Request) {
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(body))
				})
			if err := r.VerifyShareDeleted(context.Background(), "share-1"); err != nil {
				t.Fatalf("authoritative absence %s: %v", body, err)
			}
		}
	})
}

// TestShareAbsenceProvenPredicate pins the authoritative-absence predicate
// directly: only an HTTP 404 carrying the canonical NotFound code (5011) or
// no code proves absence. A contradictory status/code pair, a non-404 error,
// a pool/controller discovery error, or a transport failure is NEVER absence
// (another object's or discovery error must not count as share absence).
func TestShareAbsenceProvenPredicate(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"404 canonical NotFound", &ngxstorage.APIError{StatusCode: 404, Code: ngxstorage.CodeNotFound}, true},
		{"404 code-less", &ngxstorage.APIError{StatusCode: 404, Code: 0}, true},
		{"wrapped 404 canonical NotFound", fmt.Errorf("get share: %w", &ngxstorage.APIError{StatusCode: 404, Code: ngxstorage.CodeNotFound}), true},
		{"404 with AlreadyExists code", &ngxstorage.APIError{StatusCode: 404, Code: ngxstorage.CodeAlreadyExists}, false},
		{"403 carrying NotFound code", &ngxstorage.APIError{StatusCode: 403, Code: ngxstorage.CodeNotFound}, false},
		{"500 carrying NotFound code", &ngxstorage.APIError{StatusCode: 500, Code: ngxstorage.CodeNotFound}, false},
		{"503 code-less", &ngxstorage.APIError{StatusCode: 503, Code: 0}, false},
		{"pool discovery error", ngxstorage.ErrPoolNotFound, false},
		{"cluster not ready", ngxstorage.ErrClusterNotReady, false},
		{"transport error", &ngxstorage.TransportError{}, false},
		{"plain error", fmt.Errorf("boom"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shareAbsenceProven(tc.err); got != tc.want {
				t.Fatalf("shareAbsenceProven(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
