package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coord "github.com/NGXStorage/csi-ngxstorage-utils/coord"
	"github.com/container-storage-interface/spec/lib/go/csi"
	helper "github.com/ngxstorage/nfs-csi-ngxstorage-driver/pkg/helper"
	"github.com/ngxstorage/nfs-csi-ngxstorage-driver/pkg/utils"
	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type controllerBackendResponse struct {
	status int
	body   string
}

type controllerBackendRequest struct {
	method string
	path   string
	body   []byte
}

// controllerBackend replaces only HTTP transport in controller unit tests.
// Production URL construction, request encoding, response parsing, retry
// classification, and CSI orchestration remain exercised by every RPC call.
type controllerBackend struct {
	mu        sync.Mutex
	responses map[string][]controllerBackendResponse
	requests  []controllerBackendRequest
	// beforeResponse is an optional concurrency-test seam. It runs after the
	// request is recorded and its queued response is selected, with the backend
	// mutex released so another goroutine can make progress independently.
	beforeResponse func(controllerBackendRequest)
}

func (b *controllerBackend) RoundTrip(req *http.Request) (*http.Response, error) {
	var requestBody []byte
	if req.Body != nil {
		requestBody, _ = io.ReadAll(req.Body)
	}

	recorded := controllerBackendRequest{
		method: req.Method,
		path:   req.URL.Path,
		body:   append([]byte(nil), requestBody...),
	}
	b.mu.Lock()
	b.requests = append(b.requests, recorded)

	key := req.Method + " " + req.URL.Path
	var response controllerBackendResponse
	queue := b.responses[key]
	if len(queue) != 0 {
		response = queue[0]
		b.responses[key] = queue[1:]
	} else {
		switch key {
		case http.MethodGet + " /api/v2/status/cluster":
			response = controllerBackendResponse{body: `{"status":"Ready","connected":1}`}
		case http.MethodGet + " /api/v2/pool":
			response = controllerBackendResponse{body: `[{"id":"pool-id","name":"pool","owner":"controller-a"}]`}
		default:
			if req.Method == http.MethodPatch && strings.HasPrefix(req.URL.Path, "/api/v2/share/") {
				response = controllerBackendResponse{body: `{"success":true}`}
			} else {
				response = controllerBackendResponse{
					status: http.StatusNotFound,
					body:   `{"code":5011,"error":"test route not found"}`,
				}
			}
		}
	}
	b.mu.Unlock()
	if b.beforeResponse != nil {
		b.beforeResponse(recorded)
	}
	if response.status == 0 {
		response.status = http.StatusOK
	}
	return &http.Response{
		StatusCode: response.status,
		Body:       io.NopCloser(strings.NewReader(response.body)),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

func (b *controllerBackend) RequestsFor(method, path string) []controllerBackendRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	var matching []controllerBackendRequest
	for _, request := range b.requests {
		if request.method == method && request.path == path {
			matching = append(matching, request)
		}
	}
	return matching
}

func InstallControllerBackend(t *testing.T, responses map[string][]controllerBackendResponse) (*Driver, *controllerBackend) {
	t.Helper()

	backend := &controllerBackend{responses: responses}
	client, err := ngxstorage.NewClient(ngxstorage.Config{
		Controllers:        []string{"127.0.0.1"},
		APIKey:             "test-key",
		PoolName:           "pool",
		InsecureSkipVerify: true,
		HTTPClient:         &http.Client{Transport: backend},
		Logger:             ngxstorage.NopLogger{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &Driver{
		NodeId:       "node-1",
		client:       client,
		dataIP:       "192.0.2.20",
		dataPath:     helper.DataPath,
		AccessModes:  nfsTestModes,
		coordination: newFakeCoordinator(),
	}, backend
}

func TestCreateSnapshotCanonicalIdempotencyAndCreate(t *testing.T) {
	const snapshotListRoute = "GET /api/v2/snapshot/list"
	for _, test := range []struct {
		name          string
		listBody      string
		createBody    string
		wantCode      codes.Code
		wantSnapshot  string
		wantCreateRPC bool
	}{
		{
			name:         "same name and source returns existing snapshot",
			listBody:     `[{"id":"snap-1","name":"snapshot","volume_id":"vol-1","created":"1700000000","reserved":"1073741824"}]`,
			wantSnapshot: "snap-1",
		},
		{
			name:     "same name and different source is rejected",
			listBody: `[{"id":"snap-2","name":"snapshot","volume_id":"vol-2","created":"1700000000","reserved":"1073741824"}]`,
			wantCode: codes.AlreadyExists,
		},
		{
			name:     "existing snapshot requires canonical creation time",
			listBody: `[{"id":"snap-1","name":"snapshot","volume_id":"vol-1","reserved":"1073741824"}]`,
			wantCode: codes.Internal,
		},
		{
			name:     "existing snapshot requires canonical id",
			listBody: `[{"name":"snapshot","volume_id":"vol-1","created":"1700000000","reserved":"1073741824"}]`,
			wantCode: codes.Internal,
		},
		{
			name:     "existing snapshot requires canonical source volume",
			listBody: `[{"id":"snap-1","name":"snapshot","created":"1700000000","reserved":"1073741824"}]`,
			wantCode: codes.Internal,
		},
		{
			name:     "existing snapshot requires canonical reserved bytes",
			listBody: `[{"id":"snap-1","name":"snapshot","volume_id":"vol-1","created":"1700000000"}]`,
			wantCode: codes.Internal,
		},
		{
			name:          "unused name creates snapshot",
			listBody:      `[]`,
			createBody:    `{"id":"snap-new","name":"snapshot","volume_id":"vol-1","created":"1700000000","reserved":"1073741824"}`,
			wantSnapshot:  "snap-new",
			wantCreateRPC: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			routes := map[string][]controllerBackendResponse{
				snapshotListRoute: {{body: test.listBody}},
			}
			if test.createBody != "" {
				routes["POST /api/v2/snapshot"] = []controllerBackendResponse{{body: test.createBody}}
			}
			driver, backend := InstallControllerBackend(t, routes)

			response, err := driver.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
				Name:           "snapshot",
				SourceVolumeId: "vol-1",
			})
			if status.Code(err) != test.wantCode {
				t.Fatalf("CreateSnapshot() code = %s, want %s: %v", status.Code(err), test.wantCode, err)
			}
			if test.wantCode == codes.OK {
				if response.GetSnapshot().GetSnapshotId() != test.wantSnapshot {
					t.Fatalf("snapshot ID = %q, want %q", response.GetSnapshot().GetSnapshotId(), test.wantSnapshot)
				}
				if response.GetSnapshot().GetCreationTime() == nil {
					t.Fatal("creation time is nil")
				}
			}

			createRequests := backend.RequestsFor(http.MethodPost, "/api/v2/snapshot")
			if got := len(createRequests) != 0; got != test.wantCreateRPC {
				t.Fatalf("create request present = %t, want %t", got, test.wantCreateRPC)
			}
			if test.wantCreateRPC {
				var payload map[string]interface{}
				if err := json.Unmarshal(createRequests[0].body, &payload); err != nil {
					t.Fatal(err)
				}
				if payload["name"] != "snapshot" || payload["volume_id"] != "vol-1" {
					t.Fatalf("create payload = %#v", payload)
				}
			}
		})
	}
}

func TestCreateVolumeCanonicalIdempotencyContext(t *testing.T) {
	for _, test := range []struct {
		name      string
		softQuota int64
		wantCode  codes.Code
	}{
		{
			name:      "same name and quota returns canonical context",
			softQuota: utils.UnitGiB,
		},
		{
			name:      "same name and different quota is rejected",
			softQuota: 2 * utils.UnitGiB,
			wantCode:  codes.AlreadyExists,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			shareBody := `[{` +
				`"id":"vol-1",` +
				`"name":"pvcname",` +
				`"capacity":{"soft_quota":` + strconv.FormatInt(test.softQuota, 10) + `}` +
				`}]`
			driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
				"GET /api/v2/status/services": {{body: `{"nfs":"running"}`}},
				"GET /api/v2/share/list":      {{body: shareBody}},
			})

			response, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name:               "pvc-name",
				CapacityRange:      &csi.CapacityRange{RequiredBytes: utils.UnitGiB},
				VolumeCapabilities: []*csi.VolumeCapability{NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)},
			})
			if status.Code(err) != test.wantCode {
				t.Fatalf("CreateVolume() code = %s, want %s: %v", status.Code(err), test.wantCode, err)
			}
			if test.wantCode == codes.OK {
				context := response.GetVolume().GetVolumeContext()
				if context["share_id"] != "vol-1" ||
					context["share_name"] != "pvcname" ||
					context["pool_name"] != "pool" {
					t.Fatalf("volume context = %#v", context)
				}
				if id, ok := driver.VolumeHashIDByName("pvcname"); !ok || id != "vol-1" {
					t.Fatalf("hash map tracked name lookup = %q, %t", id, ok)
				}
			}
			if requests := backend.RequestsFor(http.MethodPost, "/api/v2/share"); len(requests) != 0 {
				t.Fatalf("idempotent request created a share: %#v", requests)
			}
		})
	}
}

func TestCreateVolumeRejectsMalformedExistingCanonicalShare(t *testing.T) {
	for _, test := range []struct {
		name     string
		listBody string
	}{
		{
			name:     "missing id",
			listBody: `[{"name":"pvcname","capacity":{"soft_quota":1073741824}}]`,
		},
		{
			name:     "missing capacity",
			listBody: `[{"id":"vol-1","name":"pvcname"}]`,
		},
		{
			name:     "empty capacity quota",
			listBody: `[{"id":"vol-1","name":"pvcname","capacity":{}}]`,
		},
		{
			name:     "malformed soft quota",
			listBody: `[{"id":"vol-1","name":"pvcname","capacity":{"soft_quota":"invalid"}}]`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
				"GET /api/v2/status/services": {{body: `{"nfs":"running"}`}},
				"GET /api/v2/share/list":      {{body: test.listBody}},
			})

			_, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name:               "pvc-name",
				CapacityRange:      &csi.CapacityRange{RequiredBytes: utils.UnitGiB},
				VolumeCapabilities: []*csi.VolumeCapability{NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)},
			})
			if status.Code(err) != codes.Internal {
				t.Fatalf("CreateVolume() code = %s, want %s: %v", status.Code(err), codes.Internal, err)
			}
			if requests := backend.RequestsFor(http.MethodPost, "/api/v2/share"); len(requests) != 0 {
				t.Fatalf("malformed idempotency record caused a create request: %#v", requests)
			}
		})
	}
}

func TestCreateVolumeValidatesCapacityRangeInline(t *testing.T) {
	driver := &Driver{}
	for _, test := range []struct {
		name     string
		capacity *csi.CapacityRange
	}{
		{name: "negative required", capacity: &csi.CapacityRange{RequiredBytes: -1}},
		{name: "negative limit", capacity: &csi.CapacityRange{LimitBytes: -1}},
		{name: "required exceeds limit", capacity: &csi.CapacityRange{RequiredBytes: 2, LimitBytes: 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name:          "pvc",
				CapacityRange: test.capacity,
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("CreateVolume() code = %s, want %s: %v", status.Code(err), codes.InvalidArgument, err)
			}
		})
	}
}

func TestDeleteVolumeUsesCanonicalBackendState(t *testing.T) {
	for _, test := range []struct {
		name            string
		detailResponse  controllerBackendResponse
		deleteResponse  *controllerBackendResponse
		wantDeleteCalls int
		wantDetailCalls int // detail + read-after-delete verification GETs
	}{
		{
			name:            "existing share is deleted",
			detailResponse:  controllerBackendResponse{body: `{"id":"vol-1","name":"share","pool_name":"pool"}`},
			deleteResponse:  &controllerBackendResponse{body: `{"success":true}`},
			wantDeleteCalls: 1,
			wantDetailCalls: 2, // canonical detail + verified-absence GET (queue-exhausted 404)
		},
		{
			name: "already absent share is idempotent",
			detailResponse: controllerBackendResponse{
				status: http.StatusNotFound,
				body:   `{"code":5011,"error":"share not found"}`,
			},
			wantDetailCalls: 1, // no delete, no verification
		},
		{
			name:           "delete race is idempotent",
			detailResponse: controllerBackendResponse{body: `{"id":"vol-1","name":"share","pool_name":"pool"}`},
			deleteResponse: &controllerBackendResponse{
				status: http.StatusNotFound,
				body:   `{"code":5011,"error":"share not found"}`,
			},
			wantDeleteCalls: 1,
			wantDetailCalls: 2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			routes := map[string][]controllerBackendResponse{
				"GET /api/v2/share/vol-1": {test.detailResponse},
			}
			if test.deleteResponse != nil {
				routes["DELETE /api/v2/share/vol-1"] = []controllerBackendResponse{*test.deleteResponse}
			}
			driver, backend := InstallControllerBackend(t, routes)

			response, err := driver.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-1"})
			if err != nil {
				t.Fatal(err)
			}
			if response == nil {
				t.Fatal("DeleteVolume() response is nil")
			}
			if calls := len(backend.RequestsFor(http.MethodGet, "/api/v2/share/vol-1")); calls != test.wantDetailCalls {
				t.Fatalf("detail calls = %d, want %d", calls, test.wantDetailCalls)
			}
			if calls := len(backend.RequestsFor(http.MethodDelete, "/api/v2/share/vol-1")); calls != test.wantDeleteCalls {
				t.Fatalf("delete calls = %d, want %d", calls, test.wantDeleteCalls)
			}
		})
	}
}

// repeatResponse builds a response queue long enough to outlast the test's
// verification budget (the fake backend returns 404 once a queue is exhausted,
// which would fabricate absence).
func repeatResponse(r controllerBackendResponse, n int) []controllerBackendResponse {
	out := make([]controllerBackendResponse, n)
	for i := range out {
		out[i] = r
	}
	return out
}

// TestDeleteVolumeReadAfterDeleteVerification covers the read-after-delete
// contract: a 200-success delete response is not proof of backend absence.
// Regression for the observed orphan-share mechanism (array acknowledges a
// delete it cannot perform while a snapshot clone depends on the share).
func TestDeleteVolumeReadAfterDeleteVerification(t *testing.T) {
	const detailRoute = "GET /api/v2/share/vol-1"
	const deleteRoute = "DELETE /api/v2/share/vol-1"
	present := controllerBackendResponse{body: `{"id":"vol-1","name":"share","pool_name":"pool"}`}
	absent := controllerBackendResponse{status: http.StatusNotFound, body: `{"code":5011,"error":"share not found"}`}
	okDelete := controllerBackendResponse{body: `{"success":true}`}

	t.Run("delete success but share persists is an error", func(t *testing.T) {
		// Queue: detail present, then verification GETs always present.
		routes := map[string][]controllerBackendResponse{
			detailRoute: repeatResponse(present, 30),
			deleteRoute: {okDelete},
		}
		driver, backend := InstallControllerBackend(t, routes)
		driver.deleteVerifyTimeout = 150 * time.Millisecond
		driver.deleteVerifyInterval = 20 * time.Millisecond
		response, err := driver.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-1"})
		if err == nil {
			t.Fatal("DeleteVolume() succeeded while the backend share still exists; want error")
		}
		if response != nil {
			t.Fatal("DeleteVolume() returned a response alongside an error")
		}
		if code := status.Code(err); code != codes.DeadlineExceeded && code != codes.Internal {
			t.Fatalf("DeleteVolume() code = %s, want DeadlineExceeded/Internal: %v", code, err)
		}
		// No snapshot/clone cascade: only the share DELETE was attempted.
		for _, r := range backend.requests {
			if r.method == http.MethodDelete && r.path != "/api/v2/share/vol-1" {
				t.Fatalf("unexpected delete of dependent object: %s %s", r.method, r.path)
			}
		}
	})

	t.Run("delayed absence converges to success", func(t *testing.T) {
		routes := map[string][]controllerBackendResponse{
			detailRoute: {present, present, present, absent},
			deleteRoute: {okDelete},
		}
		driver, backend := InstallControllerBackend(t, routes)
		driver.deleteVerifyTimeout = 2 * time.Second
		driver.deleteVerifyInterval = 10 * time.Millisecond
		response, err := driver.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-1"})
		if err != nil {
			t.Fatalf("DeleteVolume() failed while absence eventually became authoritative: %v", err)
		}
		if response == nil {
			t.Fatal("DeleteVolume() response is nil")
		}
		if calls := len(backend.RequestsFor(http.MethodGet, "/api/v2/share/vol-1")); calls != 4 {
			t.Fatalf("detail+verify calls = %d, want 4", calls)
		}
	})

	t.Run("verification GET 403 is never absence", func(t *testing.T) {
		forbidden := controllerBackendResponse{status: http.StatusForbidden, body: `{"code":403,"error":"forbidden"}`}
		routes := map[string][]controllerBackendResponse{
			detailRoute: append([]controllerBackendResponse{present}, repeatResponse(forbidden, 30)...),
			deleteRoute: {okDelete},
		}
		driver, _ := InstallControllerBackend(t, routes)
		driver.deleteVerifyTimeout = 150 * time.Millisecond
		driver.deleteVerifyInterval = 20 * time.Millisecond
		response, err := driver.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-1"})
		if err == nil || response != nil {
			t.Fatal("DeleteVolume() treated a 403 verification GET as absence; want error")
		}
	})

	t.Run("verification GET 5xx is never absence", func(t *testing.T) {
		serverErr := controllerBackendResponse{status: http.StatusInternalServerError, body: `{"code":500,"error":"internal"}`}
		routes := map[string][]controllerBackendResponse{
			detailRoute: append([]controllerBackendResponse{present}, repeatResponse(serverErr, 30)...),
			deleteRoute: {okDelete},
		}
		driver, _ := InstallControllerBackend(t, routes)
		driver.deleteVerifyTimeout = 150 * time.Millisecond
		driver.deleteVerifyInterval = 20 * time.Millisecond
		response, err := driver.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-1"})
		if err == nil || response != nil {
			t.Fatal("DeleteVolume() treated a 5xx verification GET as absence; want error")
		}
	})

	t.Run("verification GET timeout budget is never absence", func(t *testing.T) {
		// The fake transport ignores context cancellation, so a hanging GET is
		// modeled by the verification budget expiring while the share is still
		// reported present: absence must NOT be proven and CSI must not succeed.
		routes := map[string][]controllerBackendResponse{
			detailRoute: repeatResponse(present, 30),
			deleteRoute: {okDelete},
		}
		driver, _ := InstallControllerBackend(t, routes)
		driver.deleteVerifyTimeout = 100 * time.Millisecond
		driver.deleteVerifyInterval = 30 * time.Millisecond
		response, err := driver.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-1"})
		if err == nil || response != nil {
			t.Fatal("DeleteVolume() succeeded without authoritative absence; want error")
		}
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("DeleteVolume() code = %s, want DeadlineExceeded: %v", status.Code(err), err)
		}
	})

	t.Run("dependent clone is preserved (no cascade)", func(t *testing.T) {
		// Absence verified after delete; assert the driver never issues any
		// snapshot/clone/share-other delete — dependent objects stay untouched.
		routes := map[string][]controllerBackendResponse{
			detailRoute: {present, absent},
			deleteRoute: {okDelete},
		}
		driver, backend := InstallControllerBackend(t, routes)
		driver.deleteVerifyTimeout = time.Second
		driver.deleteVerifyInterval = 10 * time.Millisecond
		if _, err := driver.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-1"}); err != nil {
			t.Fatal(err)
		}
		deletes := backend.RequestsFor(http.MethodDelete, "/api/v2/share/vol-1")
		if len(deletes) != 1 {
			t.Fatalf("share delete calls = %d, want 1", len(deletes))
		}
		for _, r := range backend.requests {
			if r.method == http.MethodDelete && r.path != "/api/v2/share/vol-1" {
				t.Fatalf("driver deleted a dependent object: %s", r.path)
			}
		}
	})

	t.Run("repeat call after verified delete is idempotent", func(t *testing.T) {
		routes := map[string][]controllerBackendResponse{
			detailRoute: {present, absent, absent},
			deleteRoute: {okDelete},
		}
		driver, backend := InstallControllerBackend(t, routes)
		driver.deleteVerifyTimeout = time.Second
		driver.deleteVerifyInterval = 10 * time.Millisecond
		if _, err := driver.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-1"}); err != nil {
			t.Fatal(err)
		}
		// Second call: canonical detail GET is already NotFound -> idempotent
		// success with NO further delete.
		response, err := driver.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-1"})
		if err != nil || response == nil {
			t.Fatalf("repeat DeleteVolume() = %v, want idempotent success", err)
		}
		if calls := len(backend.RequestsFor(http.MethodDelete, "/api/v2/share/vol-1")); calls != 1 {
			t.Fatalf("delete calls = %d, want 1 (no re-delete)", calls)
		}
	})
}

func TestGetCapacityReturnsConfiguredPoolBytes(t *testing.T) {
	driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/status/capacity": {{
			body: `[{"name":"pool","logical":{"available":"3221225472","reserved":"1073741824"}}]`,
		}},
	})

	response, err := driver.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetAvailableCapacity() != 3*utils.UnitGiB {
		t.Fatalf("available capacity = %d, want %d", response.GetAvailableCapacity(), 3*utils.UnitGiB)
	}
}

func TestControllerGetCapabilitiesReturnsConfiguredCapabilities(t *testing.T) {
	driver := &Driver{}
	for _, capability := range supportedControllerCapabilities {
		driver.csCap = append(driver.csCap, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{Type: capability},
			},
		})
	}

	response, err := driver.ControllerGetCapabilities(context.Background(), &csi.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.GetCapabilities()) != len(supportedControllerCapabilities) {
		t.Fatalf("controller capabilities = %#v", response.GetCapabilities())
	}
	for index, expected := range supportedControllerCapabilities {
		if actual := response.GetCapabilities()[index].GetRpc().GetType(); actual != expected {
			t.Fatalf("controller capability %d = %s, want %s", index, actual, expected)
		}
	}
}

func TestListVolumesUsesStableCanonicalPagination(t *testing.T) {
	driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/share/list": {
			{body: `[
				{"id":"vol-b","capacity":{"soft_quota":"2147483648"}},
				{"id":"vol-a","capacity":{"soft_quota":"1073741824"}}
			]`},
			{body: `[
				{"id":"vol-b","capacity":{"soft_quota":"2147483648"}},
				{"id":"vol-a","capacity":{"soft_quota":"1073741824"}}
			]`},
		},
	})

	first, err := driver.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.GetEntries()) != 1 ||
		first.GetEntries()[0].GetVolume().GetVolumeId() != "vol-a" ||
		first.GetNextToken() != "v1:vol-a" {
		t.Fatalf("first page = %#v", first)
	}

	second, err := driver.ListVolumes(context.Background(), &csi.ListVolumesRequest{
		MaxEntries:    1,
		StartingToken: first.GetNextToken(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.GetEntries()) != 1 ||
		second.GetEntries()[0].GetVolume().GetVolumeId() != "vol-b" ||
		second.GetNextToken() != "" {
		t.Fatalf("second page = %#v", second)
	}
}

func TestListVolumesContinuesAfterPageAnchorIsDeletedAndVolumeIsAdded(t *testing.T) {
	driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		"GET /api/v2/share/list": {
			{body: `[
				{"id":"vol-c","capacity":{"soft_quota":"3221225472"}},
				{"id":"vol-a","capacity":{"soft_quota":"1073741824"}},
				{"id":"vol-b","capacity":{"soft_quota":"2147483648"}}
			]`},
			// vol-a and the cursor anchor vol-b were deleted; vol-d appeared.
			{body: `[
				{"id":"vol-d","capacity":{"soft_quota":"4294967296"}},
				{"id":"vol-c","capacity":{"soft_quota":"3221225472"}}
			]`},
		},
	})

	first, err := driver.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.GetEntries()) != 2 ||
		first.GetEntries()[0].GetVolume().GetVolumeId() != "vol-a" ||
		first.GetEntries()[1].GetVolume().GetVolumeId() != "vol-b" ||
		first.GetNextToken() != "v1:vol-b" {
		t.Fatalf("first page = %#v", first)
	}

	second, err := driver.ListVolumes(context.Background(), &csi.ListVolumesRequest{
		MaxEntries:    2,
		StartingToken: first.GetNextToken(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.GetEntries()) != 2 ||
		second.GetEntries()[0].GetVolume().GetVolumeId() != "vol-c" ||
		second.GetEntries()[1].GetVolume().GetVolumeId() != "vol-d" ||
		second.GetNextToken() != "" {
		t.Fatalf("second page after mutation = %#v", second)
	}
}

func TestListVolumesSkipsMalformedCanonicalRecords(t *testing.T) {
	for _, test := range []struct {
		name     string
		listBody string
	}{
		{name: "missing id", listBody: `[{"capacity":{"soft_quota":1073741824}}]`},
		{name: "non-string id", listBody: `[{"id":1,"capacity":{"soft_quota":1073741824}}]`},
		{name: "missing capacity", listBody: `[{"id":"vol-1"}]`},
		{name: "non-object capacity", listBody: `[{"id":"vol-1","capacity":"invalid"}]`},
		{name: "empty capacity quota", listBody: `[{"id":"vol-1","capacity":{}}]`},
		{name: "malformed soft quota", listBody: `[{"id":"vol-1","capacity":{"soft_quota":"invalid"}}]`},
		{name: "zero soft quota", listBody: `[{"id":"vol-1","capacity":{"soft_quota":0}}]`},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
				"GET /api/v2/share/list": {{body: test.listBody}},
			})

			response, err := driver.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if len(response.GetEntries()) != 0 {
				t.Fatalf("entries = %#v, want none", response.GetEntries())
			}
		})
	}
}

func TestListVolumesSkipsMalformedRecordsAlongsideCanonicalOnes(t *testing.T) {
	driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		"GET /api/v2/share/list": {{
			body: `[
				{"id":"bad-quota","capacity":{"soft_quota":"invalid"}},
				{"id":"vol-a","capacity":{"soft_quota":"1073741824"}},
				{"capacity":{"soft_quota":1073741824}}
			]`,
		}},
	})

	response, err := driver.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.GetEntries()) != 1 || response.GetEntries()[0].GetVolume().GetVolumeId() != "vol-a" {
		t.Fatalf("entries = %#v", response.GetEntries())
	}
}

func TestDeleteSnapshotIsIdempotentForMissingBackendSnapshot(t *testing.T) {
	const snapshotRoute = "/api/v2/snapshot/snap-1"
	for _, test := range []struct {
		name string
		body string
	}{
		{name: "deleted", body: `{"success":true}`},
		{name: "already absent", body: `{"code":5011,"error":"not found"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
				http.MethodDelete + " " + snapshotRoute: {{body: test.body}},
			})
			_, err := driver.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
				SnapshotId: "snap-1",
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestListSnapshotsCanonicalPagination(t *testing.T) {
	driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		"GET /api/v2/snapshot/list": {{
			body: `[
				{"id":"snap-b","name":"b","volume_id":"vol-1","created":"1700000002","reserved":"2"},
				{"id":"snap-a","name":"a","volume_id":"vol-1","created":"1700000001","reserved":"1"}
			]`,
		}},
	})

	response, err := driver.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.GetEntries()) != 1 || response.GetEntries()[0].GetSnapshot().GetSnapshotId() != "snap-a" {
		t.Fatalf("entries = %#v", response.GetEntries())
	}
	if response.GetNextToken() != "s1:snap-a" {
		t.Fatalf("next token = %q, want %q", response.GetNextToken(), "s1:snap-a")
	}
}

func TestListSnapshotsNonCanonicalSourceReturnsEmpty(t *testing.T) {
	driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		"GET /api/v2/snapshot/list": {{body: `[{"id":"snap-1","volume_id":"vol-1","created":"1700000000","reserved":"1"}]`}},
	})

	response, err := driver.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		SourceVolumeId: "fake-vol-id-9125bd59-e",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.GetEntries()) != 0 {
		t.Fatalf("entries = %#v, want empty", response.GetEntries())
	}
	if len(backend.RequestsFor(http.MethodGet, "/api/v2/snapshot/list")) != 0 {
		t.Fatal("backend must not be called for a non-canonical source volume id")
	}
}

func TestListSnapshotsRejectsMalformedCanonicalRecord(t *testing.T) {
	for _, test := range []struct {
		name     string
		listBody string
	}{
		{name: "missing id", listBody: `[{"volume_id":"vol-1","created":"1700000000","reserved":"1"}]`},
		{name: "missing volume id", listBody: `[{"id":"snap-1","created":"1700000000","reserved":"1"}]`},
		{name: "missing created", listBody: `[{"id":"snap-1","volume_id":"vol-1","reserved":"1"}]`},
		{name: "malformed created", listBody: `[{"id":"snap-1","volume_id":"vol-1","created":"invalid","reserved":"1"}]`},
		{name: "missing reserved", listBody: `[{"id":"snap-1","volume_id":"vol-1","created":"1700000000"}]`},
		{name: "malformed reserved", listBody: `[{"id":"snap-1","volume_id":"vol-1","created":"1700000000","reserved":"bad"}]`},
		{name: "negative reserved", listBody: `[{"id":"snap-1","volume_id":"vol-1","created":"1700000000","reserved":"-1"}]`},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
				"GET /api/v2/snapshot/list": {{body: test.listBody}},
			})

			_, err := driver.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{})
			if status.Code(err) != codes.Internal {
				t.Fatalf("ListSnapshots() code = %s, want %s: %v", status.Code(err), codes.Internal, err)
			}
		})
	}
}

func TestControllerExpandVolumeCanonicalQuota(t *testing.T) {
	const shareRoute = "/api/v2/share/vol-1"
	for _, test := range []struct {
		name          string
		currentBody   string
		requiredBytes int64
		wantCode      codes.Code
		wantPatch     bool
	}{
		{
			name:          "equal capacity is idempotent",
			currentBody:   `{"id":"vol-1","capacity":{"soft_quota":1073741824}}`,
			requiredBytes: utils.UnitGiB,
		},
		{
			name:          "larger capacity patches soft quota bytes",
			currentBody:   `{"id":"vol-1","capacity":{"soft_quota":1073741824}}`,
			requiredBytes: 2 * utils.UnitGiB,
			wantPatch:     true,
		},
		{
			name:          "shrink is rejected",
			currentBody:   `{"id":"vol-1","capacity":{"soft_quota":2147483648}}`,
			requiredBytes: utils.UnitGiB,
			wantCode:      codes.InvalidArgument,
		},
		{
			name:          "missing quota is rejected",
			currentBody:   `{"id":"vol-1","capacity":{}}`,
			requiredBytes: 2 * utils.UnitGiB,
			wantCode:      codes.Internal,
		},
		{
			name:          "mismatched canonical id is rejected",
			currentBody:   `{"id":"other-volume","capacity":{"soft_quota":1073741824}}`,
			requiredBytes: 2 * utils.UnitGiB,
			wantCode:      codes.Internal,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
				http.MethodGet + " " + shareRoute:   {{body: test.currentBody}},
				http.MethodPatch + " " + shareRoute: {{body: `{"success":true}`}},
			})

			response, err := driver.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
				VolumeId:      "vol-1",
				CapacityRange: &csi.CapacityRange{RequiredBytes: test.requiredBytes},
			})
			if status.Code(err) != test.wantCode {
				t.Fatalf("ControllerExpandVolume() code = %s, want %s: %v", status.Code(err), test.wantCode, err)
			}
			if test.wantCode == codes.OK && response.GetCapacityBytes() != test.requiredBytes {
				t.Fatalf("capacity = %d, want %d", response.GetCapacityBytes(), test.requiredBytes)
			}

			patchRequests := backend.RequestsFor(http.MethodPatch, shareRoute)
			if got := len(patchRequests) != 0; got != test.wantPatch {
				t.Fatalf("patch request present = %t, want %t", got, test.wantPatch)
			}
			if test.wantPatch {
				var payload map[string]interface{}
				if err := json.Unmarshal(patchRequests[0].body, &payload); err != nil {
					t.Fatal(err)
				}
				if payload["soft_quota"] != float64(test.requiredBytes) {
					t.Fatalf("soft_quota payload = %#v, want %d", payload["soft_quota"], test.requiredBytes)
				}
			}
		})
	}
}

func TestControllerGetVolumeCanonicalResponse(t *testing.T) {
	const shareRoute = "GET /api/v2/share/vol-1"
	for _, test := range []struct {
		name     string
		response controllerBackendResponse
		wantCode codes.Code
	}{
		{
			name:     "canonical volume",
			response: controllerBackendResponse{body: `{"id":"vol-1","capacity":{"soft_quota":1073741824}}`},
		},
		{
			name:     "missing canonical quota",
			response: controllerBackendResponse{body: `{"id":"vol-1","capacity":{}}`},
			wantCode: codes.Internal,
		},
		{
			name:     "mismatched canonical id",
			response: controllerBackendResponse{body: `{"id":"other-volume","capacity":{"soft_quota":1073741824}}`},
			wantCode: codes.Internal,
		},
		{
			name:     "backend not found",
			response: controllerBackendResponse{body: `{"code":5011,"error":"not found"}`},
			wantCode: codes.NotFound,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
				shareRoute: {test.response},
			})

			response, err := driver.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{VolumeId: "vol-1"})
			if status.Code(err) != test.wantCode {
				t.Fatalf("ControllerGetVolume() code = %s, want %s: %v", status.Code(err), test.wantCode, err)
			}
			if test.wantCode == codes.OK {
				if response.GetVolume().GetVolumeId() != "vol-1" ||
					response.GetVolume().GetCapacityBytes() != utils.UnitGiB {
					t.Fatalf("volume = %#v", response.GetVolume())
				}
			}
		})
	}
}

func TestControllerModifyVolumeWritesWhitelistedFields(t *testing.T) {
	driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/share/vol-1":   {{body: `{"id":"vol-1","name":"share","pool_name":"pool"}`}},
		http.MethodPatch + " /api/v2/share/vol-1": {{body: `{"success":true}`}},
	})
	response, err := driver.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId: "vol-1",
		MutableParameters: map[string]string{
			"compression":     "true",
			"permissions":     "111111111",
			"dram_cache":      "false",
			"flash_cache":     "true",
			"flashtier_limit": "1k",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response == nil {
		t.Fatal("response is nil")
	}
	var patched map[string]interface{}
	if err := json.Unmarshal(backend.requests[len(backend.requests)-1].body, &patched); err != nil {
		t.Fatal(err)
	}
	if patched["compression"] != true {
		t.Fatalf("compression = %#v, want true", patched["compression"])
	}
	if patched["permissions"] != "111111111" {
		t.Fatalf("permissions = %#v, want 111111111", patched["permissions"])
	}
	if patched["flashtier_limit"] != "1k" {
		t.Fatalf("flashtier_limit = %#v, want 1k", patched["flashtier_limit"])
	}
}

func TestControllerModifyVolumeRejectsWithoutBackendCall(t *testing.T) {
	driver, backend := InstallControllerBackend(t, nil)
	for _, req := range []*csi.ControllerModifyVolumeRequest{
		{}, // no volume id
		{VolumeId: "vol-1", MutableParameters: map[string]string{"XXX_FakeKey": "X"}},
		{VolumeId: "vol-1", MutableParameters: map[string]string{"soft_quota": "2147483648"}},
	} {
		rsp, err := driver.ControllerModifyVolume(context.Background(), req)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("code = %s, want %s: %v", status.Code(err), codes.InvalidArgument, err)
		}
		if rsp != nil {
			t.Fatal("response must be nil on error")
		}
	}
	if len(backend.requests) != 0 {
		t.Fatalf("backend called %d times, want 0", len(backend.requests))
	}
}

func TestControllerModifyVolumeNotFound(t *testing.T) {
	driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodPatch + " /api/v2/share/nope": {{status: http.StatusNotFound, body: `{"code":5011}`}},
	})
	_, err := driver.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          "nope",
		MutableParameters: map[string]string{"compression": "true"},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %s, want %s: %v", status.Code(err), codes.NotFound, err)
	}
}

func TestCreateVolumeRejectsDirectVolumeClone(t *testing.T) {
	driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/status/services": {{body: `{"nfs":"running"}`}},
		http.MethodGet + " /api/v2/share/list":      {{body: `[]`}},
	})
	response, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "clone",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: utils.UnitGiB},
		VolumeCapabilities: []*csi.VolumeCapability{NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Volume{
				Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "source-volume"},
			},
		},
	})
	if response != nil || status.Code(err) != codes.Unimplemented {
		t.Fatalf("CreateVolume() direct clone = %#v, %v", response, err)
	}
	if requests := backend.RequestsFor(http.MethodPost, "/api/v2/share/"); len(requests) != 0 {
		t.Fatalf("unsupported clone created a share: %#v", requests)
	}
}

func TestControllerPublishVolumeCanonicalExportState(t *testing.T) {
	const shareRoute = "/api/v2/share/vol-1"
	for _, test := range []struct {
		name        string
		shareStatus int
		shareBody   string
		wantCode    codes.Code
		wantPatch   bool
	}{
		{
			name:      "disabled dynamic export is enabled",
			shareBody: `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":false}}}`,
			wantPatch: true,
		},
		{
			name:      "enabled dynamic export is idempotent",
			shareBody: `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":true}}}`,
		},
		{
			name:      "malformed export state is rejected",
			shareBody: `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":"maybe"}}}`,
			wantCode:  codes.Internal,
		},
		{
			name:        "semantic missing share over HTTP 500 remains NotFound",
			shareStatus: http.StatusInternalServerError,
			shareBody:   `{"code":5011,"error":"not found"}`,
			wantCode:    codes.NotFound,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
				http.MethodGet + " " + shareRoute:   {{status: test.shareStatus, body: test.shareBody}},
				http.MethodPatch + " " + shareRoute: {{body: `{"success":true}`}},
			})

			response, err := driver.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
				VolumeId:         "vol-1",
				NodeId:           "node-1",
				VolumeCapability: NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
			})
			if status.Code(err) != test.wantCode {
				t.Fatalf("ControllerPublishVolume() code = %s, want %s: %v", status.Code(err), test.wantCode, err)
			}
			if test.wantCode == codes.OK {
				if response.GetPublishContext()["server"] != "192.0.2.20" {
					t.Fatalf("publish server = %q", response.GetPublishContext()["server"])
				}
				if response.GetPublishContext()["path"] != "/exports/pool/share" {
					t.Fatalf("publish path = %q, want /exports/pool/share", response.GetPublishContext()["path"])
				}
			}

			patchRequests := backend.RequestsFor(http.MethodPatch, shareRoute)
			if got := len(patchRequests) != 0; got != test.wantPatch {
				t.Fatalf("patch request present = %t, want %t", got, test.wantPatch)
			}
			if test.wantPatch {
				// The disabled path sends one atomic patch carrying both the
				// export flag and the (default-false) read-only mode.
				want := []byte(`{"nfs_export":true,"nfs_read_only":false}`)
				if !bytes.Equal(patchRequests[0].body, want) {
					t.Fatalf("patch body = %s, want %s", patchRequests[0].body, want)
				}
			}
		})
	}
}

func TestControllerPublishVolumeValidatesRequiredFieldsInline(t *testing.T) {
	driver := &Driver{AccessModes: nfsTestModes}
	for _, test := range []struct {
		name    string
		request *csi.ControllerPublishVolumeRequest
	}{
		{name: "nil request"},
		{name: "missing IDs", request: &csi.ControllerPublishVolumeRequest{}},
		{
			name: "missing node ID",
			request: &csi.ControllerPublishVolumeRequest{
				VolumeId: "volume",
			},
		},
		{
			name: "missing capability",
			request: &csi.ControllerPublishVolumeRequest{
				VolumeId: "volume",
				NodeId:   "node",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := driver.ControllerPublishVolume(context.Background(), test.request)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("ControllerPublishVolume() code = %s, want %s: %v", status.Code(err), codes.InvalidArgument, err)
			}
		})
	}
}

func TestControllerPublishVolumeRejectsUnknownNode(t *testing.T) {
	const shareRoute = "/api/v2/share/vol-1"
	driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " " + shareRoute: {{
			body: `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":true}}}`,
		}},
	})
	driver.NodeId = "node-1"
	driver.Role = "node"

	_, err := driver.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId:         "vol-1",
		NodeId:           "node-2",
		VolumeCapability: NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("ControllerPublishVolume() code = %s, want %s: %v", status.Code(err), codes.NotFound, err)
	}
	if requests := backend.RequestsFor(http.MethodGet, shareRoute); len(requests) != 0 {
		t.Fatalf("unknown node still triggered backend lookup: %#v", requests)
	}
}

func TestControllerPublishVolumeAcceptsOwnNode(t *testing.T) {
	const shareRoute = "/api/v2/share/vol-1"
	driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " " + shareRoute: {{
			body: `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":true}}}`,
		}},
	})
	driver.NodeId = "node-1"
	driver.Role = "node"

	_, err := driver.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId:         "vol-1",
		NodeId:           "node-1",
		VolumeCapability: NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestControllerPublishVolumeAcceptsAnyNodeInControllerMode(t *testing.T) {
	const shareRoute = "/api/v2/share/vol-1"
	driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " " + shareRoute: {{
			body: `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":true}}}`,
		}},
	})
	driver.Role = "controller"

	_, err := driver.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId:         "vol-1",
		NodeId:           "node-2",
		VolumeCapability: NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestControllerPublishVolumeProtectsStaticExportOwnership(t *testing.T) {
	const shareListRoute = "GET /api/v2/share/list"
	for _, test := range []struct {
		name     string
		listBody string
		wantCode codes.Code
		wantPath string
	}{
		{
			name:     "enabled static share is returned without mutation",
			listBody: `[{"id":"backend-id","name":"existing","pool_name":"pool","exports":{"nfs":{"enabled":true}}}]`,
			wantPath: "/exports/pool/existing",
		},
		{
			name:     "disabled static share is administrator owned",
			listBody: `[{"id":"backend-id","name":"existing","pool_name":"pool","exports":{"nfs":{"enabled":false}}}]`,
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "missing static share remains NotFound",
			listBody: `[]`,
			wantCode: codes.NotFound,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
				shareListRoute: {{body: test.listBody}},
			})
			response, err := driver.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
				VolumeId:         DriverName + ".existing",
				NodeId:           "node-1",
				VolumeCapability: NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
			})
			if status.Code(err) != test.wantCode {
				t.Fatalf("ControllerPublishVolume() code = %s, want %s: %v", status.Code(err), test.wantCode, err)
			}
			if test.wantCode == codes.OK && response.GetPublishContext()["path"] != test.wantPath {
				t.Fatalf("publish path = %q, want %q", response.GetPublishContext()["path"], test.wantPath)
			}
			if requests := backend.RequestsFor(http.MethodPatch, "/api/v2/share/backend-id"); len(requests) != 0 {
				t.Fatalf("static share was mutated: %#v", requests)
			}
		})
	}
}

func TestControllerUnpublishVolumeCanonicalExportState(t *testing.T) {
	const shareRoute = "/api/v2/share/vol-1"
	for _, test := range []struct {
		name      string
		shareBody string
		wantCode  codes.Code
		wantPatch bool
	}{
		{
			name:      "enabled export is disabled",
			shareBody: `{"id":"vol-1","exports":{"nfs":{"enabled":true}}}`,
			wantPatch: true,
		},
		{
			name:      "disabled export is idempotent",
			shareBody: `{"id":"vol-1","exports":{"nfs":{"enabled":false}}}`,
		},
		{
			name:      "missing share is idempotent",
			shareBody: `{"code":5011,"error":"not found"}`,
		},
		{
			name:      "malformed export state is rejected",
			shareBody: `{"id":"vol-1","exports":{"nfs":{"enabled":"maybe"}}}`,
			wantCode:  codes.Internal,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
				http.MethodGet + " " + shareRoute:   {{body: test.shareBody}},
				http.MethodPatch + " " + shareRoute: {{body: `{"success":true}`}},
			})
			_, err := driver.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
				VolumeId: "vol-1",
			})
			if status.Code(err) != test.wantCode {
				t.Fatalf("ControllerUnpublishVolume() code = %s, want %s: %v", status.Code(err), test.wantCode, err)
			}

			patchRequests := backend.RequestsFor(http.MethodPatch, shareRoute)
			if got := len(patchRequests) != 0; got != test.wantPatch {
				t.Fatalf("patch request present = %t, want %t", got, test.wantPatch)
			}
			if test.wantPatch {
				want := []byte(`{"nfs_export":false}`)
				if !bytes.Equal(patchRequests[0].body, want) {
					t.Fatalf("patch body = %s, want %s", patchRequests[0].body, want)
				}
			}
		})
	}
}

func TestControllerUnpublishVolumeRetainsExportForNodeSpecificRequest(t *testing.T) {
	driver, backend := InstallControllerBackend(t, nil)
	_, err := driver.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "vol-1",
		NodeId:   "node-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(backend.RequestsFor(http.MethodGet, "/api/v2/share/vol-1")) != 0 ||
		len(backend.RequestsFor(http.MethodPatch, "/api/v2/share/vol-1")) != 0 {
		t.Fatal("node-specific unpublish changed share-global export state")
	}
}

func TestControllerUnpublishVolumeDoesNotUseDriverMutationLock(t *testing.T) {
	const shareRoute = "/api/v2/share/vol-1"
	driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " " + shareRoute: {{
			body: `{"id":"vol-1","exports":{"nfs":{"enabled":true}}}`,
		}},
		http.MethodPatch + " " + shareRoute: {{
			body: `{"success":true}`,
		}},
	})

	// ControllerUnpublishVolume performs one idempotent enabled=false backend
	// mutation and must not depend on the unrelated process-local driver lock.
	driver.nodeState.Lock()
	defer driver.nodeState.Unlock()

	_, err := driver.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "vol-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if requests := backend.RequestsFor(http.MethodPatch, shareRoute); len(requests) != 1 {
		t.Fatalf("PATCH request count = %d, want 1", len(requests))
	}
}

func TestControllerPublishVolumeEnforcesReadOnlyFlag(t *testing.T) {
	const shareRoute = "/api/v2/share/vol-1"
	for _, test := range []struct {
		name        string
		shareBody   string
		readonly    bool
		wantCode    codes.Code
		wantPatches []string
	}{
		{
			name:        "disabled dynamic export is enabled with read-only flag in one patch",
			shareBody:   `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":false,"read_only":false}}}`,
			readonly:    true,
			wantPatches: []string{`{"nfs_export":true,"nfs_read_only":true}`},
		},
		{
			name:        "disabled dynamic export is enabled writable in one patch",
			shareBody:   `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":false}}}`,
			readonly:    false,
			wantPatches: []string{`{"nfs_export":true,"nfs_read_only":false}`},
		},
		{
			// An all-node unpublish disables the export but leaves the stale
			// read-only flag; a subsequent writable publish must reset it in the
			// same atomic patch or the re-enabled share stays read-only.
			name:        "disabled export with stale read-only is re-enabled writable",
			shareBody:   `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":false,"read_only":true}}}`,
			readonly:    false,
			wantPatches: []string{`{"nfs_export":true,"nfs_read_only":false}`},
		},
		{
			name:      "writable republish of read-only export is rejected",
			shareBody: `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":true,"read_only":true}}}`,
			readonly:  false,
			wantCode:  codes.AlreadyExists,
		},
		{
			// The export read-only state is fixed at publish time; a republish
			// requesting a different state is incompatible with the existing
			// publish (csi-sanity "already published but incompatible").
			name:      "read-only republish of writable export is rejected",
			shareBody: `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":true,"read_only":false}}}`,
			readonly:  true,
			wantCode:  codes.AlreadyExists,
		},
		{
			name:      "read-only republish of read-only export is idempotent",
			shareBody: `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":true,"read_only":true}}}`,
			readonly:  true,
		},
		{
			name:      "writable republish of writable export is idempotent",
			shareBody: `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":true,"read_only":false}}}`,
			readonly:  false,
		},
		{
			name:      "canonical string read-only state is accepted",
			shareBody: `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":true,"read_only":"1"}}}`,
			readonly:  true,
		},
		{
			name:        "malformed string read-only state is rejected",
			shareBody:   `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":true,"read_only":"maybe"}}}`,
			readonly:    true,
			wantCode:    codes.Internal,
			wantPatches: nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
				http.MethodGet + " " + shareRoute:   {{body: test.shareBody}},
				http.MethodPatch + " " + shareRoute: {{body: `{"success":true}`}, {body: `{"success":true}`}},
			})

			response, err := driver.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
				VolumeId:         "vol-1",
				NodeId:           "node-1",
				VolumeCapability: NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
				Readonly:         test.readonly,
			})
			if status.Code(err) != test.wantCode {
				t.Fatalf("ControllerPublishVolume() code = %s, want %s: %v", status.Code(err), test.wantCode, err)
			}
			if test.wantCode == codes.OK && response == nil {
				t.Fatal("ControllerPublishVolume() response is nil")
			}

			patchRequests := backend.RequestsFor(http.MethodPatch, shareRoute)
			var patchBodies []string
			for _, request := range patchRequests {
				patchBodies = append(patchBodies, string(request.body))
			}
			if got, want := len(patchBodies), len(test.wantPatches); got != want {
				t.Fatalf("patch count = %d, want %d: %#v", got, want, patchBodies)
			}
			for index, want := range test.wantPatches {
				if patchBodies[index] != want {
					t.Fatalf("patch %d = %s, want %s", index, patchBodies[index], want)
				}
			}
		})
	}
}

func TestControllerPublishVolumeDoesNotUseDriverMutationLock(t *testing.T) {
	const shareRoute = "/api/v2/share/vol-1"
	driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " " + shareRoute: {{
			body: `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":false}}}`,
		}},
		http.MethodPatch + " " + shareRoute: {{
			body: `{"success":true}`,
		}},
	})

	// ControllerPublishVolume relies on the canonical idempotent export PATCH
	// and must not depend on the unrelated process-local node mutation lock.
	driver.nodeState.Lock()
	defer driver.nodeState.Unlock()

	_, err := driver.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId:         "vol-1",
		NodeId:           "node-1",
		VolumeCapability: NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
		VolumeContext: map[string]string{
			"share_id":   "vol-1",
			"share_name": "share",
			"pool_name":  "pool",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if requests := backend.RequestsFor(http.MethodPatch, shareRoute); len(requests) != 1 {
		t.Fatalf("PATCH request count = %d, want 1", len(requests))
	}
}

func TestControllerUnpublishVolumeRetainsStaticShare(t *testing.T) {
	driver, backend := InstallControllerBackend(t, nil)
	_, err := driver.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: DriverName + ".existing",
		NodeId:   "node-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(backend.RequestsFor(http.MethodGet, "/api/v2/share/list")) != 0 ||
		len(backend.RequestsFor(http.MethodPatch, "/api/v2/share/backend-id")) != 0 {
		t.Fatal("static unpublish contacted backend")
	}
}

func TestCreateVolumeAcceptsMutableParameters(t *testing.T) {
	driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/status/cluster":  {{body: `{"status":"Ready","connected":1}`}},
		http.MethodGet + " /api/v2/status/services": {{body: `{"nfs":"running"}`}},
		http.MethodGet + " /api/v2/status/capacity": {{body: `[{"name":"pool","logical":{"available":"2147483648000","reserved":"0"},"physical":{"total":"2147483648000"}}]`}},
		http.MethodGet + " /api/v2/share/list":      {{body: `[]`}},
		http.MethodPost + " /api/v2/share/":         {{body: `{"success":true}`}},
		http.MethodGet + " /api/v2/share":           {{body: `[{"id":"vol-1","name":"pvcvol"}]`}},
	})
	rsp, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "vol",
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		}},
		CapacityRange:     &csi.CapacityRange{RequiredBytes: 1073741824},
		MutableParameters: map[string]string{"compression": "true"},
	})
	if err != nil {
		t.Fatalf("CreateVolume() error = %v", err)
	}
	if rsp.GetVolume() == nil {
		t.Fatal("response volume is nil")
	}
	createRequests := backend.RequestsFor(http.MethodPost, "/api/v2/share/")
	if len(createRequests) != 1 {
		t.Fatalf("POST /api/v2/share/ calls = %d, want 1", len(createRequests))
	}
	var body map[string]interface{}
	if err := json.Unmarshal(createRequests[0].body, &body); err != nil {
		t.Fatal(err)
	}
	if body["compression"] != true {
		t.Fatalf("compression = %#v, want true", body["compression"])
	}
}

func TestCreateVolumeRejectsInvalidMutableParameters(t *testing.T) {
	driver, backend := InstallControllerBackend(t, nil)
	_, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "vol",
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		}},
		CapacityRange:     &csi.CapacityRange{RequiredBytes: 1073741824},
		MutableParameters: map[string]string{"XXX_FakeKey": "X"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %s, want %s: %v", status.Code(err), codes.InvalidArgument, err)
	}
	if len(backend.requests) != 0 {
		t.Fatalf("backend called %d times, want 0", len(backend.requests))
	}
}

func TestCreateVolumeAppliesDefaultsWithoutParams(t *testing.T) {
	driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/status/cluster":  {{body: `{"status":"Ready","connected":1}`}},
		http.MethodGet + " /api/v2/status/services": {{body: `{"nfs":"running"}`}},
		http.MethodGet + " /api/v2/status/capacity": {{body: `[{"name":"pool","logical":{"available":"2147483648000","reserved":"0"},"physical":{"total":"2147483648000"}}]`}},
		http.MethodGet + " /api/v2/share/list":      {{body: `[]`}},
		http.MethodPost + " /api/v2/share/":         {{body: `{"success":true}`}},
		http.MethodGet + " /api/v2/share":           {{body: `[{"id":"vol-1","name":"pvcvol"}]`}},
	})
	rsp, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "vol",
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		}},
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1073741824},
	})
	if err != nil {
		t.Fatalf("CreateVolume() error = %v", err)
	}
	if rsp.GetVolume() == nil {
		t.Fatal("response volume is nil")
	}
	createRequests := backend.RequestsFor(http.MethodPost, "/api/v2/share/")
	if len(createRequests) != 1 {
		t.Fatalf("POST /api/v2/share/ calls = %d, want 1", len(createRequests))
	}
	var body map[string]interface{}
	if err := json.Unmarshal(createRequests[0].body, &body); err != nil {
		t.Fatal(err)
	}
	if body["compression"] != false {
		t.Fatalf("compression = %#v, want default false", body["compression"])
	}
	for _, key := range []string{"flash_cache", "dram_cache", "flashtier_limit", "permissions"} {
		if _, ok := body[key]; ok {
			t.Fatalf("create payload must omit modify-only field %s: %#v", key, body)
		}
	}
	if body["deduplication"] != false {
		t.Fatalf("deduplication = %#v, want default false", body["deduplication"])
	}
	if body["blocksize"] != helper.DefaultBlockSize {
		t.Fatalf("blocksize = %#v, want default %q", body["blocksize"], helper.DefaultBlockSize)
	}
	if _, ok := body["reserved"]; ok {
		t.Fatalf("reserved = %#v, want absent", body["reserved"])
	}
	patches := backend.RequestsFor(http.MethodPatch, "/api/v2/share/vol-1")
	if len(patches) != 2 {
		t.Fatalf("modify PATCH calls = %d, want cache settings and permissions", len(patches))
	}
	createIndex, patchIndex := -1, -1
	for i, request := range backend.requests {
		if request.method == http.MethodPost && request.path == "/api/v2/share/" {
			createIndex = i
		}
		if request.method == http.MethodPatch && request.path == "/api/v2/share/vol-1" {
			patchIndex = i
		}
	}
	if createIndex < 0 || patchIndex <= createIndex {
		t.Fatalf("permissions PATCH must follow share POST: create index=%d patch index=%d", createIndex, patchIndex)
	}
	var cachePatch map[string]interface{}
	if err := json.Unmarshal(patches[0].body, &cachePatch); err != nil {
		t.Fatal(err)
	}
	if cachePatch["flash_cache"] != true || cachePatch["dram_cache"] != true || cachePatch["flashtier_blocksize_limit"] != "0" {
		t.Fatalf("default cache PATCH body = %#v", cachePatch)
	}
	var permissionPatch map[string]interface{}
	if err := json.Unmarshal(patches[1].body, &permissionPatch); err != nil {
		t.Fatal(err)
	}
	if len(permissionPatch) != 1 || permissionPatch["permissions"] != "111111111" {
		t.Fatalf("permissions PATCH body = %#v, want only permissions=111111111", permissionPatch)
	}
}

func TestCreateVolumeAcceptsCanonicalStorageClassParameters(t *testing.T) {
	driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/status/cluster":  {{body: `{"status":"Ready","connected":1}`}},
		http.MethodGet + " /api/v2/status/services": {{body: `{"nfs":"running"}`}},
		http.MethodGet + " /api/v2/status/capacity": {{body: `[{"name":"pool","logical":{"available":"2147483648000","reserved":"0"},"physical":{"total":"2147483648000"}}]`}},
		http.MethodGet + " /api/v2/share/list":      {{body: `[]`}},
		http.MethodPost + " /api/v2/share/":         {{body: `{"success":true}`}},
		http.MethodGet + " /api/v2/share":           {{body: `[{"id":"vol-1","name":"pvcvol"}]`}},
	})
	_, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "vol",
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		}},
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1073741824},
		Parameters: map[string]string{
			"blocksize":     "64k",
			"flash_cache":   "off",
			"deduplication": "on",
			"dram_cache":    "off",
			"compression":   "on",
			"permissions":   "111111111",
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume() error = %v", err)
	}
	createRequests := backend.RequestsFor(http.MethodPost, "/api/v2/share/")
	if len(createRequests) != 1 {
		t.Fatalf("POST /api/v2/share/ calls = %d, want 1", len(createRequests))
	}
	var body map[string]interface{}
	if err := json.Unmarshal(createRequests[0].body, &body); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]interface{}{
		"blocksize":     "64k",
		"deduplication": true,
		"compression":   true,
	} {
		if got := body[key]; got != want {
			t.Errorf("%s = %#v, want %#v", key, got, want)
		}
	}
	if _, ok := body["reserved"]; ok {
		t.Fatalf("reserved = %#v, want absent", body["reserved"])
	}
	patches := backend.RequestsFor(http.MethodPatch, "/api/v2/share/vol-1")
	if len(patches) != 2 {
		t.Fatalf("modify PATCH calls = %d, want cache settings and permissions", len(patches))
	}
	var cachePatch map[string]interface{}
	if err := json.Unmarshal(patches[0].body, &cachePatch); err != nil {
		t.Fatal(err)
	}
	if cachePatch["flash_cache"] != false || cachePatch["dram_cache"] != false || cachePatch["flashtier_blocksize_limit"] != "0" {
		t.Fatalf("explicit disabled cache PATCH body = %#v", cachePatch)
	}
	var permissionPatch map[string]interface{}
	if err := json.Unmarshal(patches[1].body, &permissionPatch); err != nil {
		t.Fatal(err)
	}
	if len(permissionPatch) != 1 || permissionPatch["permissions"] != "111111111" {
		t.Fatalf("permissions PATCH body = %#v, want only permissions=111111111", permissionPatch)
	}
}

func TestCreateVolumeRejectsUnsupportedStorageClassParameters(t *testing.T) {
	for _, parameter := range []string{
		"blockSize",
		"flashcache",
		"dramcache",
		"thinProvision",
		"volDedup",
		"volCompress",
		"flashtier_limit",
		"unknown",
	} {
		t.Run(parameter, func(t *testing.T) {
			driver, backend := InstallControllerBackend(t, nil)
			_, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name: "vol",
				VolumeCapabilities: []*csi.VolumeCapability{{
					AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
					AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
				}},
				CapacityRange: &csi.CapacityRange{RequiredBytes: 1073741824},
				Parameters:    map[string]string{parameter: "on"},
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %s, want %s: %v", status.Code(err), codes.InvalidArgument, err)
			}
			if len(backend.requests) != 0 {
				t.Fatalf("backend called %d times, want 0", len(backend.requests))
			}
		})
	}
}

func TestCreateVolumeIgnoresCapacityRangeLimitBytes(t *testing.T) {
	driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/status/cluster":  {{body: `{"status":"Ready","connected":1}`}},
		http.MethodGet + " /api/v2/status/services": {{body: `{"nfs":"running"}`}},
		http.MethodGet + " /api/v2/status/capacity": {{body: `[{"name":"pool","logical":{"available":"2147483648000","reserved":"0"},"physical":{"total":"2147483648000"}}]`}},
		http.MethodGet + " /api/v2/share/list":      {{body: `[]`}},
		http.MethodPost + " /api/v2/share/":         {{body: `{"success":true}`}},
		http.MethodGet + " /api/v2/share":           {{body: `[{"id":"vol-1","name":"pvcvol"}]`}},
	})
	_, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "vol",
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		}},
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1073741824, LimitBytes: 2147483648},
	})
	if err != nil {
		t.Fatalf("CreateVolume() error = %v", err)
	}
	createRequests := backend.RequestsFor(http.MethodPost, "/api/v2/share/")
	if len(createRequests) != 1 {
		t.Fatalf("POST /api/v2/share/ calls = %d, want 1", len(createRequests))
	}
	var body map[string]interface{}
	if err := json.Unmarshal(createRequests[0].body, &body); err != nil {
		t.Fatal(err)
	}
	if body["soft_quota"] != float64(1073741824) {
		t.Fatalf("soft_quota = %#v, want 1073741824", body["soft_quota"])
	}
	// LimitBytes must never reach the backend: the external provisioner never
	// populates it (kubernetes-csi/external-provisioner#49), so hard_quota is
	// not part of the NGX share create contract.
	if _, ok := body["hard_quota"]; ok {
		t.Fatalf("hard_quota = %#v, want absent from create body", body["hard_quota"])
	}
}

func TestGetSnapshotCanonicalById(t *testing.T) {
	driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		"GET /api/v2/snapshot/list": {{
			body: `[{"id":"snap-1","name":"target","volume_id":"vol-1","reserved":"1073741824","created":"1785499878"}]`,
		}},
	})
	rsp, err := driver.GetSnapshot(context.Background(), &csi.GetSnapshotRequest{SnapshotId: "snap-1"})
	if err != nil {
		t.Fatal(err)
	}
	if rsp.GetSnapshot().GetSnapshotId() != "snap-1" {
		t.Fatalf("snapshot id = %q", rsp.GetSnapshot().GetSnapshotId())
	}
	if rsp.GetSnapshot().GetSourceVolumeId() != "vol-1" {
		t.Fatalf("source volume id = %q", rsp.GetSnapshot().GetSourceVolumeId())
	}
	if rsp.GetSnapshot().GetCreationTime() == nil {
		t.Fatal("creation time is nil")
	}
	if rsp.GetSnapshot().GetSizeBytes() != 1073741824 {
		t.Fatalf("size bytes = %d, want 1073741824", rsp.GetSnapshot().GetSizeBytes())
	}
}

func TestGetSnapshotInvalidArgumentAndNotFound(t *testing.T) {
	driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		"GET /api/v2/snapshot/list": {{body: `[{"id":"snap-1","volume_id":"vol-1"}]`}},
	})
	if _, err := driver.GetSnapshot(context.Background(), &csi.GetSnapshotRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty id code = %s, want %s", status.Code(err), codes.InvalidArgument)
	}
	_, err := driver.GetSnapshot(context.Background(), &csi.GetSnapshotRequest{SnapshotId: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("missing id code = %s, want %s: %v", status.Code(err), codes.NotFound, err)
	}
	// Typed CSI surface + context cancellation reach the real RPC.
	var srv csi.ControllerServer = driver
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := srv.GetSnapshot(ctx, &csi.GetSnapshotRequest{SnapshotId: "snap-1"}); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled ctx code = %s, want Canceled", status.Code(err))
	}
}

func TestControllerGetVolumeHealth(t *testing.T) {
	driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/share/vol-1": {{body: `{"id":"vol-1","pool_name":"pool"}`}},
	})
	rsp, err := driver.ControllerGetVolumeHealth(context.Background(), &csi.ControllerGetVolumeHealthRequest{VolumeId: "vol-1"})
	if err != nil {
		t.Fatal(err)
	}
	if rsp.GetVolumeHealth().GetVolumeId() != "vol-1" {
		t.Fatalf("volume id = %q", rsp.GetVolumeHealth().GetVolumeId())
	}
	if len(rsp.GetVolumeHealth().GetHealthStatuses()) != 0 {
		t.Fatalf("expected zero health statuses, got %d", len(rsp.GetVolumeHealth().GetHealthStatuses()))
	}
}

func TestControllerGetVolumeHealthInvalid(t *testing.T) {
	driver, _ := InstallControllerBackend(t, nil)
	if _, err := driver.ControllerGetVolumeHealth(context.Background(), &csi.ControllerGetVolumeHealthRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %s, want %s", status.Code(err), codes.InvalidArgument)
	}
}

func TestControllerListVolumeHealth(t *testing.T) {
	driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/share/list": {{body: `[{"id":"vol-1","pool_name":"pool"},{"id":"vol-2","pool_name":"pool"}]`}},
	})
	rsp, err := driver.ControllerListVolumeHealth(context.Background(), &csi.ControllerListVolumeHealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rsp.GetEntries()) != 2 {
		t.Fatalf("entries = %d, want 2", len(rsp.GetEntries()))
	}
}

var nfsTestModes = []csi.VolumeCapability_AccessMode_Mode{
	csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
	csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
	csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
	csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
	csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
	csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
	csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER,
}

func NFSMountCapability(mode csi.VolumeCapability_AccessMode_Mode) *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode},
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
	}
}

func TestDeleteVolumeRetainsStaticExistingShare(t *testing.T) {
	driver := &Driver{}
	response, err := driver.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: DriverName + ".existing-share",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response == nil {
		t.Fatal("DeleteVolume returned a nil response")
	}
}

func TestValidateVolumeCapabilitiesChecksRequiredFieldsWithoutLock(t *testing.T) {
	driver := &Driver{AccessModes: nfsTestModes}
	driver.nodeState.Lock()
	defer driver.nodeState.Unlock()

	for _, request := range []*csi.ValidateVolumeCapabilitiesRequest{
		nil,
		{},
		{VolumeId: "volume-id"},
	} {
		_, err := driver.ValidateVolumeCapabilities(context.Background(), request)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("ValidateVolumeCapabilities(%v) code = %s, want %s", request, status.Code(err), codes.InvalidArgument)
		}
	}
}

func TestValidateVolumeCapabilitiesUsesCanonicalBackendIdentity(t *testing.T) {
	capability := NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)

	t.Run("dynamic canonical share is confirmed", func(t *testing.T) {
		driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
			"GET /api/v2/share/vol-1": {{body: `{"id":"vol-1","name":"share"}`}},
		})
		request := &csi.ValidateVolumeCapabilitiesRequest{
			VolumeId:           "vol-1",
			VolumeCapabilities: []*csi.VolumeCapability{capability},
			VolumeContext:      map[string]string{"share_id": "vol-1"},
			Parameters:         map[string]string{"blocksize": "128k"},
			MutableParameters:  map[string]string{"policy": "unchanged"},
		}

		response, err := driver.ValidateVolumeCapabilities(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if response.GetConfirmed() == nil ||
			response.GetConfirmed().GetVolumeContext()["share_id"] != "vol-1" ||
			response.GetConfirmed().GetParameters()["blocksize"] != "128k" ||
			response.GetConfirmed().GetMutableParameters()["policy"] != "unchanged" {
			t.Fatalf("confirmed response = %#v", response.GetConfirmed())
		}
	})

	for _, test := range []struct {
		name     string
		volumeID string
		route    string
		body     string
		wantCode codes.Code
	}{
		{
			name:     "dynamic mismatched id",
			volumeID: "vol-1",
			route:    "GET /api/v2/share/vol-1",
			body:     `{"id":"other","name":"share"}`,
			wantCode: codes.Internal,
		},
		{
			name:     "dynamic missing name",
			volumeID: "vol-1",
			route:    "GET /api/v2/share/vol-1",
			body:     `{"id":"vol-1"}`,
			wantCode: codes.Internal,
		},
		{
			name:     "static canonical share",
			volumeID: DriverName + ".existing",
			route:    "GET /api/v2/share/list",
			body:     `[{"id":"backend-id","name":"existing"}]`,
		},
		{
			name:     "static missing id",
			volumeID: DriverName + ".existing",
			route:    "GET /api/v2/share/list",
			body:     `[{"name":"existing"}]`,
			wantCode: codes.Internal,
		},
		{
			name:     "static missing share",
			volumeID: DriverName + ".existing",
			route:    "GET /api/v2/share/list",
			body:     `[]`,
			wantCode: codes.NotFound,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
				test.route: {{body: test.body}},
			})

			response, err := driver.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId:           test.volumeID,
				VolumeCapabilities: []*csi.VolumeCapability{capability},
			})
			if status.Code(err) != test.wantCode {
				t.Fatalf("ValidateVolumeCapabilities() code = %s, want %s: %v", status.Code(err), test.wantCode, err)
			}
			if test.wantCode == codes.OK && response.GetConfirmed() == nil {
				t.Fatalf("ValidateVolumeCapabilities() did not confirm canonical share: %#v", response)
			}
		})
	}

	t.Run("unsupported capability is not confirmed", func(t *testing.T) {
		driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
			"GET /api/v2/share/vol-1": {{body: `{"id":"vol-1","name":"share"}`}},
		})
		blockCapability := &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}

		response, err := driver.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
			VolumeId:           "vol-1",
			VolumeCapabilities: []*csi.VolumeCapability{blockCapability},
		})
		if err != nil {
			t.Fatal(err)
		}
		if response.GetConfirmed() != nil || response.GetMessage() == "" {
			t.Fatalf("unsupported capability response = %#v", response)
		}
	})
}

func TestLocalSanityLookupNodeExists(t *testing.T) {
	lookup := fakeNodeLookup{exists: true, nodeID: "local-sanity"}
	for _, nodeID := range []string{"local-sanity", "unknown-node", ""} {
		t.Run(nodeID, func(t *testing.T) {
			exists, err := lookup.NodeExists(context.Background(), nodeID)
			want := nodeID == "local-sanity"
			if err != nil || exists != want {
				t.Fatalf("NodeExists(%q) = %v, %v; want %v, nil", nodeID, exists, err, want)
			}
		})
	}
}

type fakeNodeLookup struct {
	nodeID string
	exists bool
	err    error
}

func (f fakeNodeLookup) NodeExists(_ context.Context, nodeID string) (bool, error) {
	if f.nodeID != "" {
		return f.exists && nodeID == f.nodeID, f.err
	}
	return f.exists, f.err
}

// An unknown node must be NotFound before any backend state is read or changed.
func TestControllerPublishVolumeUnknownNodeReturnsNotFound(t *testing.T) {
	const shareRoute = "/api/v2/share/vol-1"
	driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " " + shareRoute:   {{body: `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":false}}}`}},
		http.MethodPatch + " " + shareRoute: {{body: `{"success":true}`}},
	})
	driver.nodeLookup = fakeNodeLookup{exists: false}

	_, err := driver.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId:         "vol-1",
		NodeId:           "ghost-node",
		VolumeCapability: NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("ControllerPublishVolume() code = %s, want NotFound: %v", status.Code(err), err)
	}
	if got := len(backend.RequestsFor(http.MethodGet, shareRoute)); got != 0 {
		t.Fatalf("backend GET count = %d, want 0 (node rejected before backend resolution)", got)
	}
	if got := len(backend.RequestsFor(http.MethodPatch, shareRoute)); got != 0 {
		t.Fatalf("backend PATCH count = %d, want 0", got)
	}
}

// A node lookup failure is Unavailable, never a false NotFound.
func TestControllerPublishVolumeNodeLookupFailureReturnsUnavailable(t *testing.T) {
	driver, _ := InstallControllerBackend(t, nil)
	driver.nodeLookup = fakeNodeLookup{err: errors.New("apiserver down")}

	_, err := driver.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId:         "vol-1",
		NodeId:           "node-1",
		VolumeCapability: NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("ControllerPublishVolume() code = %s, want Unavailable: %v", status.Code(err), err)
	}
}

// ---------------------------------------------------------------------------
// Coordination fakes, fail-closed behavior, and concurrent lock exclusion
// (merged from coordination_test.go and controller_coordination_test.go)
// ---------------------------------------------------------------------------

// fakeCoordinator deliberately serializes all keys through one gate. That is
// stricter than production sharding but gives controller tests deterministic
// exclusion while still recording the exact keys requested by each RPC.
type fakeCoordinator struct {
	gate         chan struct{}
	preflightErr error
	recordMu     sync.Mutex
	acquireKeys  []string
	multiKeys    [][]string
	attempts     chan string
	acquireCount atomic.Int32
}

func newFakeCoordinator() *fakeCoordinator {
	return &fakeCoordinator{
		gate:     make(chan struct{}, 1),
		attempts: make(chan string, 128),
	}
}

// TestLocalSanityBackendPreflight checks live backend readiness before the
// runner asks for root privileges to perform NFS mounts.
func TestLocalSanityBackendPreflight(t *testing.T) {
	if os.Getenv("NFS_LIVE_MOCK_PREFLIGHT") != "1" {
		t.Skip("explicit backend preflight opt-in required")
	}
	_, err := NewDriver(InputParameters{
		Endpoint: "unix:///sanity-preflight.sock", Role: "all", NodeId: "local-sanity",
		Ip: os.Getenv("STORAGE_IPS"), PoolName: os.Getenv("STORAGE_POOL_NAME"),
		ApiKey: os.Getenv("STORAGE_API_KEY"), DataIP: os.Getenv("STORAGE_DATA_IP"),
	})
	if err != nil {
		t.Fatalf("live NFS backend preflight: %v", err)
	}
}

// TestLocalSanityWithMockCoordination runs a single explicitly selected sanity
// spec against a real backend with test-only, process-local coordination.
// It must not be used as evidence of Kubernetes Lease coordination.
func TestLocalSanityWithMockCoordination(t *testing.T) {
	if os.Getenv("NFS_LIVE_MOCK_SANITY") != "1" {
		t.Skip("explicit live mock sanity opt-in required")
	}
	focus := os.Getenv("SANITY_FOCUS_FILE")
	fullSuite := os.Getenv("SANITY_FULL") == "1"
	if focus == "" && !fullSuite {
		t.Fatal("SANITY_FOCUS_FILE must select one sanity spec")
	}
	if focus != "" && fullSuite {
		t.Fatal("set either SANITY_FOCUS_FILE or SANITY_FULL=1, not both")
	}
	sanityBin := os.Getenv("NFS_SANITY_BIN")
	configDir := os.Getenv("NFS_SANITY_CONFIG_DIR")
	if sanityBin == "" || configDir == "" {
		t.Fatal("NFS_SANITY_BIN and NFS_SANITY_CONFIG_DIR are required")
	}
	dir, err := os.MkdirTemp(os.Getenv("TMPDIR"), "nfs-sanity-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "csi.sock")
	d, err := NewDriver(InputParameters{
		Endpoint: "unix://" + socket, Role: "all", NodeId: "local-sanity",
		Ip: os.Getenv("STORAGE_IPS"), PoolName: os.Getenv("STORAGE_POOL_NAME"),
		ApiKey: os.Getenv("STORAGE_API_KEY"), DataIP: os.Getenv("STORAGE_DATA_IP"),
	})
	if err != nil {
		t.Fatalf("initialize live test driver: %v", err)
	}
	d.coordination = newFakeCoordinator()
	d.nodeLookup = fakeNodeLookup{exists: true, nodeID: d.NodeId}
	if err := d.prepareCoordination(context.Background()); err != nil {
		t.Fatalf("test coordination preflight: %v", err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("test CSI socket: %v", err)
	}
	server := grpc.NewServer()
	d.Server = server
	csi.RegisterIdentityServer(server, d)
	csi.RegisterControllerServer(server, d)
	csi.RegisterNodeServer(server, d)
	d.Ready = true
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.Serve(listener) }()
	deadline := time.Now().Add(15 * time.Second)
	for {
		conn, dialErr := net.DialTimeout("unix", socket, 100*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		select {
		case runErr := <-serverErr:
			t.Fatalf("test driver startup failed: %v", runErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("test driver socket did not become ready")
		}
		time.Sleep(100 * time.Millisecond)
	}
	defer func() { server.Stop(); <-serverErr }()
	timeout := 10 * time.Minute
	if fullSuite {
		timeout = 60 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	args := []string{
		"--ginkgo.no-color", "--ginkgo.v", "--ginkgo.fail-on-empty",
		"--ginkgo.skip=GroupController", "--csi.endpoint=unix://" + socket,
		"--csi.controllerendpoint=unix://" + socket, "--csi.testvolumesize=1073741824",
		"--csi.testvolumeexpandsize=2147483648", "--csi.testvolumeaccesstype=mount",
		"--csi.mountdir=" + filepath.Join(dir, "mnt"),
		"--csi.secrets=" + filepath.Join(configDir, "secrets.yaml"),
		"--csi.testvolumeparameters=" + filepath.Join(configDir, "params.yaml"),
		"--csi.testvolumemutableparameters=" + filepath.Join(configDir, "mutableparams.yaml"),
	}
	if !fullSuite {
		args = append(args, "--ginkgo.focus-file="+focus)
	}
	// Opt-in shared-array inventory checks; unset => dedicated mode.
	if os.Getenv("SANITY_SHARED_ARRAY") != "" {
		args = append(args, "--shared-array")
	}
	cmd := exec.CommandContext(ctx, sanityBin, args...)
	cmd.Dir = dir
	var output bytes.Buffer
	combinedOutput := io.MultiWriter(os.Stdout, &output)
	cmd.Stdout = combinedOutput
	cmd.Stderr = combinedOutput
	if err := cmd.Run(); err != nil {
		t.Fatalf("live sanity spec %s failed: %v", focus, err)
	}
	result := output.String()
	if fullSuite && (!strings.Contains(result, "Will run ") || !strings.Contains(result, "SUCCESS! -- ") || !strings.Contains(result, " | 0 Failed | ")) {
		t.Fatal("full live sanity suite did not complete with zero failures")
	}
	if !fullSuite && (!strings.Contains(result, "Will run 1 of ") || !strings.Contains(result, "1 Passed | 0 Failed")) {
		t.Fatalf("live sanity focus %s must select exactly one passing spec and have zero failures", focus)
	}
}

// TestLocalSanityCleanupTestVolume removes one explicitly named csi-sanity
// share after an interrupted live run. The caller must provide its exact
// backend ID and expected unique name prefix.
func TestLocalSanityCleanupTestVolume(t *testing.T) {
	if os.Getenv("NFS_LIVE_MOCK_CLEANUP") != "1" {
		t.Skip("explicit live test-volume cleanup opt-in required")
	}
	volumeID := os.Getenv("NFS_CLEANUP_VOLUME_ID")
	namePrefix := os.Getenv("NFS_CLEANUP_VOLUME_NAME_PREFIX")
	if volumeID == "" || namePrefix == "" {
		t.Fatal("NFS_CLEANUP_VOLUME_ID and NFS_CLEANUP_VOLUME_NAME_PREFIX are required")
	}
	d, err := NewDriver(InputParameters{
		Endpoint: "unix://" + filepath.Join(os.Getenv("TMPDIR"), "nfs-cleanup-unused.sock"),
		Role:     "all", NodeId: "local-sanity-cleanup",
		Ip: os.Getenv("STORAGE_IPS"), PoolName: os.Getenv("STORAGE_POOL_NAME"),
		ApiKey: os.Getenv("STORAGE_API_KEY"), DataIP: os.Getenv("STORAGE_DATA_IP"),
	})
	if err != nil {
		t.Fatalf("initialize cleanup driver: %v", err)
	}
	d.coordination = newFakeCoordinator()
	share, err := d.client.Shares().Get(context.Background(), volumeID)
	if err != nil {
		t.Fatalf("resolve explicitly selected test share: %v", err)
	}
	if !strings.HasPrefix(share.Name, namePrefix) {
		t.Fatalf("refusing to delete share %q: name does not match expected test prefix %q", share.Name, namePrefix)
	}
	if _, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: volumeID}); err != nil {
		t.Fatalf("delete explicitly selected test share: %v", err)
	}
	t.Logf("deleted test-owned share %s (%s)", volumeID, share.Name)
}

func (f *fakeCoordinator) acquire(ctx context.Context, label string) (*fakeOperationLock, error) {
	select {
	case f.attempts <- label:
	default:
	}
	select {
	case f.gate <- struct{}{}:
		f.acquireCount.Add(1)
		lock := &fakeOperationLock{gate: f.gate, lost: make(chan struct{})}
		lock.owned.Store(true)
		return lock, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeCoordinator) Acquire(ctx context.Context, key string, _ coord.AcquireOptions) (coord.Lock, error) {
	f.recordMu.Lock()
	f.acquireKeys = append(f.acquireKeys, key)
	f.recordMu.Unlock()
	return f.acquire(ctx, key)
}

func (f *fakeCoordinator) AcquireMany(ctx context.Context, keys []string, _ coord.AcquireOptions) (coord.LockSet, error) {
	copyKeys := append([]string(nil), keys...)
	f.recordMu.Lock()
	f.multiKeys = append(f.multiKeys, copyKeys)
	f.recordMu.Unlock()
	return f.acquire(ctx, strings.Join(copyKeys, ","))
}

func (f *fakeCoordinator) Preflight(context.Context) error { return f.preflightErr }

func (f *fakeCoordinator) recordedAcquireKeys() []string {
	f.recordMu.Lock()
	defer f.recordMu.Unlock()
	return append([]string(nil), f.acquireKeys...)
}

func (f *fakeCoordinator) recordedMultiKeys() [][]string {
	f.recordMu.Lock()
	defer f.recordMu.Unlock()
	result := make([][]string, len(f.multiKeys))
	for i := range f.multiKeys {
		result[i] = append([]string(nil), f.multiKeys[i]...)
	}
	return result
}

type fakeOperationLock struct {
	gate  chan struct{}
	lost  chan struct{}
	once  sync.Once
	owned atomic.Bool
}

func (f *fakeOperationLock) AssertOwned(context.Context) error {
	if !f.owned.Load() {
		return errors.New("lock ownership lost")
	}
	return nil
}

func (f *fakeOperationLock) OwnershipLost() <-chan struct{} { return f.lost }

func (f *fakeOperationLock) Release(context.Context) error {
	f.once.Do(func() {
		f.owned.Store(false)
		<-f.gate
	})
	return nil
}

func TestPrepareCoordinationSkipsNodeAndFailsClosedController(t *testing.T) {
	node := &Driver{Role: "node"}
	if err := node.prepareCoordination(context.Background()); err != nil {
		t.Fatalf("node prepareCoordination() = %v, want nil without Lease dependency", err)
	}

	coord := newFakeCoordinator()
	coord.preflightErr = errors.New("forbidden")
	controller := &Driver{Role: "controller", coordination: coord}
	if err := controller.prepareCoordination(context.Background()); err == nil {
		t.Fatal("controller prepareCoordination() succeeded despite preflight failure")
	}
}

func TestControllerMutationFailsClosedWithoutCoordinator(t *testing.T) {
	driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		"GET /api/v2/share/vol-1":    {{body: `{"id":"vol-1","name":"share"}`}},
		"DELETE /api/v2/share/vol-1": {{body: `{"success":true}`}},
	})
	driver.coordination = nil

	_, err := driver.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-1"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("DeleteVolume() code = %s, want %s: %v", status.Code(err), codes.Unavailable, err)
	}
	if len(backend.requests) != 0 {
		t.Fatalf("backend called %d times without coordinator, want 0", len(backend.requests))
	}
}

func waitForCoordinationAttempt(t *testing.T, coord *fakeCoordinator, want string) {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case got := <-coord.attempts:
			if got == want {
				return
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for coordination attempt %q", want)
		}
	}
}

func coordinatedCreateVolumeRequest(name string) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Name:               name,
		CapacityRange:      &csi.CapacityRange{RequiredBytes: utils.UnitGiB},
		VolumeCapabilities: []*csi.VolumeCapability{NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)},
	}
}

func TestConcurrentSameNameCreateVolumeCreatesExactlyOnce(t *testing.T) {
	driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/status/services": {
			{body: `{"nfs":"running"}`}, {body: `{"nfs":"running"}`},
		},
		http.MethodGet + " /api/v2/share/list": {
			{body: `[]`},
			{body: `[{"id":"vol-race","name":"pvcrace","capacity":{"soft_quota":1073741824}}]`},
		},
		http.MethodGet + " /api/v2/status/capacity": {{body: `[{"name":"pool","logical":{"available":"2147483648000","reserved":"0"}}]`}},
		http.MethodPost + " /api/v2/share/":         {{body: `{"success":true}`}},
		http.MethodGet + " /api/v2/share":           {{body: `[{"id":"vol-race","name":"pvcrace"}]`}},
	})

	start := make(chan struct{})
	type result struct {
		response *csi.CreateVolumeResponse
		err      error
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			response, err := driver.CreateVolume(context.Background(), coordinatedCreateVolumeRequest("race"))
			results <- result{response: response, err: err}
		}()
	}
	close(start)

	for i := 0; i < 2; i++ {
		got := <-results
		if got.err != nil {
			t.Fatalf("CreateVolume() concurrent call failed: %v", got.err)
		}
		if got.response.GetVolume().GetVolumeId() != "vol-race" {
			t.Fatalf("CreateVolume() volume ID = %q, want vol-race", got.response.GetVolume().GetVolumeId())
		}
	}
	if calls := len(backend.RequestsFor(http.MethodPost, "/api/v2/share/")); calls != 1 {
		t.Fatalf("share create calls = %d, want exactly 1", calls)
	}

	coord := driver.coordination.(*fakeCoordinator)
	keys := coord.recordedAcquireKeys()
	if len(keys) != 2 {
		t.Fatalf("CreateVolume() single-lock acquisitions = %#v, want 2", keys)
	}
	for _, key := range keys {
		if key != "create-volume:pvcrace" {
			t.Fatalf("CreateVolume() lock key = %q, want create-volume:pvcrace", key)
		}
	}
	if multi := coord.recordedMultiKeys(); len(multi) != 0 {
		t.Fatalf("fresh CreateVolume unexpectedly used AcquireMany: %#v", multi)
	}
}

func TestConcurrentSameNameCreateSnapshotCreatesExactlyOnce(t *testing.T) {
	driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/snapshot/list": {
			{body: `[]`},
			{body: `[{"id":"snap-race","name":"snapshot","volume_id":"vol-1","created":"1700000000","reserved":"1073741824"}]`},
		},
		http.MethodPost + " /api/v2/snapshot": {{body: `{"id":"snap-race","name":"snapshot","volume_id":"vol-1","created":"1700000000","reserved":"1073741824"}`}},
	})

	start := make(chan struct{})
	type result struct {
		response *csi.CreateSnapshotResponse
		err      error
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			response, err := driver.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
				Name: "snapshot", SourceVolumeId: "vol-1",
			})
			results <- result{response: response, err: err}
		}()
	}
	close(start)

	for i := 0; i < 2; i++ {
		got := <-results
		if got.err != nil {
			t.Fatalf("CreateSnapshot() concurrent call failed: %v", got.err)
		}
		if got.response.GetSnapshot().GetSnapshotId() != "snap-race" {
			t.Fatalf("CreateSnapshot() snapshot ID = %q, want snap-race", got.response.GetSnapshot().GetSnapshotId())
		}
	}
	if calls := len(backend.RequestsFor(http.MethodPost, "/api/v2/snapshot")); calls != 1 {
		t.Fatalf("snapshot create calls = %d, want exactly 1", calls)
	}

	coord := driver.coordination.(*fakeCoordinator)
	for _, keys := range coord.recordedMultiKeys() {
		if len(keys) != 2 || keys[0] != "snapshot-name:snapshot" || keys[1] != "volume:vol-1" {
			t.Fatalf("CreateSnapshot() lock keys = %#v", keys)
		}
	}
}

func TestSnapshotCloneSerializesAgainstDeleteSnapshot(t *testing.T) {
	driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/status/services":        {{body: `{"nfs":"running"}`}},
		http.MethodGet + " /api/v2/share/list":             {{body: `[]`}},
		http.MethodGet + " /api/v2/snapshot/list":          {{body: `[{"id":"snap-1","name":"source","volume_id":"source-vol","created":"1700000000","reserved":"1073741824"}]`}},
		http.MethodGet + " /api/v2/status/capacity":        {{body: `[{"name":"pool","logical":{"available":"2147483648000","reserved":"0"}}]`}},
		http.MethodPost + " /api/v2/snapshot/clone/snap-1": {{body: `{"volume_id":"vol-clone"}`}},
		http.MethodGet + " /api/v2/share/vol-clone":        {{body: `{"id":"vol-clone","name":"pvcclone","capacity":{"soft_quota":1073741824}}`}},
		http.MethodDelete + " /api/v2/snapshot/snap-1":     {{body: `{"success":true}`}},
	})

	cloneEntered := make(chan struct{})
	releaseClone := make(chan struct{})
	backend.beforeResponse = func(request controllerBackendRequest) {
		if request.method == http.MethodPost && request.path == "/api/v2/snapshot/clone/snap-1" {
			close(cloneEntered)
			<-releaseClone
		}
	}

	cloneReq := coordinatedCreateVolumeRequest("clone")
	cloneReq.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
		Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "snap-1"},
	}}
	cloneErr := make(chan error, 1)
	go func() {
		_, err := driver.CreateVolume(context.Background(), cloneReq)
		cloneErr <- err
	}()
	select {
	case <-cloneEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("clone did not reach backend")
	}

	coord := driver.coordination.(*fakeCoordinator)
	waitForCoordinationAttempt(t, coord, "create-volume:pvcclone,snapshot:snap-1")
	deleteErr := make(chan error, 1)
	go func() {
		_, err := driver.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: "snap-1"})
		deleteErr <- err
	}()
	waitForCoordinationAttempt(t, coord, "snapshot:snap-1")
	if calls := len(backend.RequestsFor(http.MethodDelete, "/api/v2/snapshot/snap-1")); calls != 0 {
		t.Fatalf("DeleteSnapshot reached backend while clone held snapshot lock: %d calls", calls)
	}

	close(releaseClone)
	if err := <-cloneErr; err != nil {
		t.Fatalf("clone failed: %v", err)
	}
	if err := <-deleteErr; err != nil {
		t.Fatalf("DeleteSnapshot failed: %v", err)
	}
	if calls := len(backend.RequestsFor(http.MethodDelete, "/api/v2/snapshot/snap-1")); calls != 1 {
		t.Fatalf("DeleteSnapshot calls after clone = %d, want 1", calls)
	}
}

func TestPublishSerializesAllNodeUnpublishWithLockInsideReread(t *testing.T) {
	const shareRoute = "/api/v2/share/vol-1"
	driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " " + shareRoute: {
			{body: `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":false,"read_only":false}}}`},
			{body: `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":true,"read_only":false}}}`},
		},
		http.MethodPatch + " " + shareRoute: {{body: `{"success":true}`}, {body: `{"success":true}`}},
	})

	publishPatchEntered := make(chan struct{})
	releasePublish := make(chan struct{})
	backend.beforeResponse = func(request controllerBackendRequest) {
		if request.method == http.MethodPatch && request.path == shareRoute && strings.Contains(string(request.body), `"nfs_export":true`) {
			close(publishPatchEntered)
			<-releasePublish
		}
	}

	publishErr := make(chan error, 1)
	go func() {
		_, err := driver.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
			VolumeId: "vol-1", NodeId: "node-1",
			VolumeCapability: NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
		})
		publishErr <- err
	}()
	select {
	case <-publishPatchEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("publish did not reach export PATCH")
	}

	coord := driver.coordination.(*fakeCoordinator)
	waitForCoordinationAttempt(t, coord, "volume:vol-1") // publish acquisition
	unpublishErr := make(chan error, 1)
	go func() {
		_, err := driver.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{VolumeId: "vol-1"})
		unpublishErr <- err
	}()
	waitForCoordinationAttempt(t, coord, "volume:vol-1") // blocked all-node unpublish
	if calls := len(backend.RequestsFor(http.MethodGet, shareRoute)); calls != 1 {
		t.Fatalf("all-node unpublish re-read escaped the volume lock: GET calls = %d, want 1 while publish blocked", calls)
	}

	close(releasePublish)
	if err := <-publishErr; err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if err := <-unpublishErr; err != nil {
		t.Fatalf("all-node unpublish failed: %v", err)
	}
	patches := backend.RequestsFor(http.MethodPatch, shareRoute)
	if len(patches) != 2 || !strings.Contains(string(patches[0].body), `"nfs_export":true`) || string(patches[1].body) != `{"nfs_export":false}` {
		t.Fatalf("serialized publish/unpublish PATCHes = %#v", patches)
	}
}

func TestConcurrentReadOnlyThenWritablePublishUsesSerializedGlobalPolicy(t *testing.T) {
	const shareRoute = "/api/v2/share/vol-1"
	driver, backend := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " " + shareRoute: {
			{body: `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":false,"read_only":false}}}`},
			{body: `{"id":"vol-1","name":"share","pool_name":"pool","exports":{"nfs":{"enabled":true,"read_only":true}}}`},
		},
		http.MethodPatch + " " + shareRoute: {{body: `{"success":true}`}},
	})

	readOnlyPatchEntered := make(chan struct{})
	releaseReadOnly := make(chan struct{})
	backend.beforeResponse = func(request controllerBackendRequest) {
		if request.method == http.MethodPatch && strings.Contains(string(request.body), `"nfs_read_only":true`) {
			close(readOnlyPatchEntered)
			<-releaseReadOnly
		}
	}

	roErr := make(chan error, 1)
	go func() {
		_, err := driver.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
			VolumeId: "vol-1", NodeId: "node-1", Readonly: true,
			VolumeCapability: NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
		})
		roErr <- err
	}()
	select {
	case <-readOnlyPatchEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("read-only publish did not reach PATCH")
	}

	coord := driver.coordination.(*fakeCoordinator)
	waitForCoordinationAttempt(t, coord, "volume:vol-1")
	rwErr := make(chan error, 1)
	go func() {
		_, err := driver.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
			VolumeId: "vol-1", NodeId: "node-1", Readonly: false,
			VolumeCapability: NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
		})
		rwErr <- err
	}()
	waitForCoordinationAttempt(t, coord, "volume:vol-1")
	if calls := len(backend.RequestsFor(http.MethodGet, shareRoute)); calls != 1 {
		t.Fatalf("writable publish read stale export state while RO publish held lock: %d GETs", calls)
	}
	close(releaseReadOnly)
	if err := <-roErr; err != nil {
		t.Fatalf("read-only publish failed: %v", err)
	}
	if err := <-rwErr; status.Code(err) != codes.AlreadyExists {
		t.Fatalf("serialized writable publish code = %s, want %s: %v", status.Code(err), codes.AlreadyExists, err)
	}
	if calls := len(backend.RequestsFor(http.MethodPatch, shareRoute)); calls != 1 {
		t.Fatalf("global RO/RW policy emitted %d PATCHes, want 1", calls)
	}
}

func TestDeleteExpandModifyUseSharedVolumeLockKey(t *testing.T) {
	tests := []struct {
		name   string
		routes map[string][]controllerBackendResponse
		call   func(*Driver) error
	}{
		{
			name: "delete",
			routes: map[string][]controllerBackendResponse{
				http.MethodGet + " /api/v2/share/vol-1":    {{body: `{"id":"vol-1","name":"share","pool_name":"pool"}`}},
				http.MethodDelete + " /api/v2/share/vol-1": {{body: `{"success":true}`}},
			},
			call: func(d *Driver) error {
				_, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-1"})
				return err
			},
		},
		{
			name: "expand",
			routes: map[string][]controllerBackendResponse{
				http.MethodGet + " /api/v2/share/vol-1": {{body: `{"id":"vol-1","capacity":{"soft_quota":1073741824}}`}},
			},
			call: func(d *Driver) error {
				_, err := d.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
					VolumeId: "vol-1", CapacityRange: &csi.CapacityRange{RequiredBytes: utils.UnitGiB},
				})
				return err
			},
		},
		{
			name: "modify",
			routes: map[string][]controllerBackendResponse{
				http.MethodGet + " /api/v2/share/vol-1":   {{body: `{"id":"vol-1","name":"share","pool_name":"pool"}`}},
				http.MethodPatch + " /api/v2/share/vol-1": {{body: `{"success":true}`}},
			},
			call: func(d *Driver) error {
				_, err := d.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
					VolumeId: "vol-1", MutableParameters: map[string]string{"compression": "true"},
				})
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver, _ := InstallControllerBackend(t, test.routes)
			if err := test.call(driver); err != nil {
				t.Fatal(err)
			}
			keys := driver.coordination.(*fakeCoordinator).recordedAcquireKeys()
			if len(keys) != 1 || keys[0] != "volume:vol-1" {
				t.Fatalf("%s lock keys = %#v, want [volume:vol-1]", test.name, keys)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Controller share create/name-lookup/canonical-5011 owner-modify tests
// (merged from reconcile_test.go)
// ---------------------------------------------------------------------------

// reconcileMockServer serves the controller-initialization and refresh
// endpoints and delegates everything else to the test handler.
func reconcileMockServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
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
	server := httptest.NewServer(wrap)
	t.Cleanup(server.Close)
	return server
}

type reconcileTransportFunc func(*http.Request) (*http.Response, error)

func (f reconcileTransportFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// A dropped create response may still have created the name-keyed share; the
// helper must resolve it by name instead of failing or creating a duplicate.
func TestCreateShareReconciledAfterTransportFailure(t *testing.T) {
	posts := 0
	server := reconcileMockServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/api/v2/share/list"):
			w.Write([]byte(`[{"id":"sh-1","name":"vol","pool_name":"pool1"}]`))
		default:
			http.NotFound(w, r)
		}
	})
	fail := true
	transport := reconcileTransportFunc(func(req *http.Request) (*http.Response, error) {
		if fail && req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/api/v2/share/") {
			fail = false
			posts++
			return nil, fmt.Errorf("read tcp: EOF")
		}
		req.URL.Scheme = "http"
		req.URL.Host = server.Listener.Addr().String()
		return http.DefaultTransport.RoundTrip(req)
	})
	d, err := NewDriverWithHTTPClient(InputParameters{
		Ip:       "10.0.0.1",
		PoolName: "pool1",
		ApiKey:   "test-key",
		DataIP:   "10.0.0.2",
		Endpoint: "http://127.0.0.1",
		NodeId:   "node-1",
		Role:     "controller",
	}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatalf("NewDriverWithHTTPClient: %v", err)
	}
	d.coordination = newFakeCoordinator()

	share, err := d.reconciler().CreateShareReconciled(context.Background(), ngxstorage.ShareCreateRequest{Name: "vol", SoftQuotaBytes: 1})
	if err != nil {
		t.Fatalf("createShareReconciled: %v", err)
	}
	if share == nil || share.ID != "sh-1" {
		t.Fatalf("reconciled share = %+v, want id sh-1", share)
	}
	if posts != 1 {
		t.Fatalf("failed create POST count = %d, want 1 (no blind duplicate create)", posts)
	}
}

// serveTwoController builds one controller of a two-controller cluster whose
// master role follows the shared phase value.
func serveTwoController(t *testing.T, phase *atomic.Int32, self int32, patch http.HandlerFunc) *httptest.Server {
	t.Helper()
	wrap := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isMaster := phase.Load() == self
		switch {
		case strings.Contains(r.URL.Path, "/status/cluster"):
			if isMaster {
				w.Write([]byte(`{"connected":1,"status":"Master"}`))
			} else {
				w.Write([]byte(`{"connected":1,"status":"Ready"}`))
			}
		case strings.HasSuffix(r.URL.Path, "/api/v2/pool"):
			if isMaster {
				w.Write([]byte(`[{"id":"pool1","name":"pool1"}]`))
			} else {
				w.Write([]byte(`[]`))
			}
		case r.Method == http.MethodPatch && strings.Contains(r.URL.Path, "/api/v2/share/"):
			patch(w, r)
		default:
			http.NotFound(w, r)
		}
	})
	server := httptest.NewServer(wrap)
	t.Cleanup(server.Close)
	return server
}

func newTwoControllerDriver(t *testing.T, serverA, serverB *httptest.Server, failFirstPatch *atomic.Bool) *Driver {
	t.Helper()
	transport := reconcileTransportFunc(func(req *http.Request) (*http.Response, error) {
		if failFirstPatch != nil && failFirstPatch.Load() &&
			req.Method == http.MethodPatch && strings.Contains(req.URL.Path, "/api/v2/share/") &&
			req.URL.Host == "10.0.0.1" {
			failFirstPatch.Store(false)
			return nil, fmt.Errorf("write tcp: EOF")
		}
		target := serverB
		if req.URL.Host == "10.0.0.1" {
			target = serverA
		}
		req.URL.Scheme = "http"
		req.URL.Host = target.Listener.Addr().String()
		return http.DefaultTransport.RoundTrip(req)
	})
	d, err := NewDriverWithHTTPClient(InputParameters{
		Ip:       "10.0.0.1,10.0.0.2",
		PoolName: "pool1",
		ApiKey:   "test-key",
		DataIP:   "10.0.0.9",
		Endpoint: "http://127.0.0.1",
		NodeId:   "node-1",
		Role:     "controller",
	}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatalf("NewDriverWithHTTPClient: %v", err)
	}
	d.coordination = newFakeCoordinator()
	return d
}

// The live-observed case: a share PATCH reaches the stale controller, which
// answers canonical NGX 5011; the operation re-resolves the owner and applies
// the mutation exactly once on the new controller.
func TestModifyShareOnOwnerRetriesOnceOnCanonical5011(t *testing.T) {
	var phase atomic.Int32 // 0 = controller A is master
	var aPatches, bPatches atomic.Int32
	serverA := serveTwoController(t, &phase, 0, func(w http.ResponseWriter, _ *http.Request) {
		aPatches.Add(1)
		w.Write([]byte(`{"code":5011,"error":"no share here"}`))
	})
	serverB := serveTwoController(t, &phase, 1, func(w http.ResponseWriter, _ *http.Request) {
		bPatches.Add(1)
		w.Write([]byte(`{"success":true}`))
	})
	d := newTwoControllerDriver(t, serverA, serverB, nil)
	if err := d.client.RefreshController(context.Background()); err != nil {
		t.Fatalf("RefreshController: %v", err)
	}
	if got := d.client.CurrentController(); got != "10.0.0.1" {
		t.Fatalf("initial controller = %s, want 10.0.0.1", got)
	}
	phase.Store(1)

	if err := d.reconciler().ModifyShareOnOwner(context.Background(), "sh-1", map[string]interface{}{"nfs_read_only": true}); err != nil {
		t.Fatalf("modifyShareOnOwner: %v", err)
	}
	if got := aPatches.Load(); got != 1 {
		t.Fatalf("controller A patches = %d, want 1", got)
	}
	if got := bPatches.Load(); got != 1 {
		t.Fatalf("controller B patches = %d, want 1 (exactly one retry)", got)
	}
}

// If the refresh keeps the same controller there is no second mutation.
func TestModifyShareOnOwnerNoRetryWithoutControllerChange(t *testing.T) {
	var phase atomic.Int32
	var aPatches, bPatches atomic.Int32
	serverA := serveTwoController(t, &phase, 0, func(w http.ResponseWriter, _ *http.Request) {
		aPatches.Add(1)
		w.Write([]byte(`{"code":5011,"error":"no share here"}`))
	})
	serverB := serveTwoController(t, &phase, 1, func(w http.ResponseWriter, _ *http.Request) {
		bPatches.Add(1)
		w.Write([]byte(`{"success":true}`))
	})
	d := newTwoControllerDriver(t, serverA, serverB, nil)
	if err := d.client.RefreshController(context.Background()); err != nil {
		t.Fatalf("RefreshController: %v", err)
	}

	err := d.reconciler().ModifyShareOnOwner(context.Background(), "sh-1", map[string]interface{}{"nfs_read_only": true})
	if err == nil {
		t.Fatal("modifyShareOnOwner succeeded, want the original 5011")
	}
	if !ngxstorage.IsCanonicalNotFoundError(err) {
		t.Fatalf("err = %v, want canonical 5011", err)
	}
	if got := aPatches.Load(); got != 1 {
		t.Fatalf("controller A patches = %d, want 1", got)
	}
	if got := bPatches.Load(); got != 0 {
		t.Fatalf("controller B patches = %d, want 0", got)
	}
}

// A code-less HTTP 404 is a routing/object error: it must never be replayed on
// the other controller, even though a refresh would change the selection.
func TestModifyShareOnOwnerCodeless404NeverReplays(t *testing.T) {
	var phase atomic.Int32
	var aPatches, bPatches atomic.Int32
	serverA := serveTwoController(t, &phase, 0, func(w http.ResponseWriter, _ *http.Request) {
		aPatches.Add(1)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"no route"}`))
	})
	serverB := serveTwoController(t, &phase, 1, func(w http.ResponseWriter, _ *http.Request) {
		bPatches.Add(1)
		w.Write([]byte(`{"success":true}`))
	})
	d := newTwoControllerDriver(t, serverA, serverB, nil)
	if err := d.client.RefreshController(context.Background()); err != nil {
		t.Fatalf("RefreshController: %v", err)
	}
	phase.Store(1)

	err := d.reconciler().ModifyShareOnOwner(context.Background(), "sh-1", map[string]interface{}{"nfs_read_only": true})
	if err == nil {
		t.Fatal("modifyShareOnOwner succeeded, want the code-less 404")
	}
	if ngxstorage.IsCanonicalNotFoundError(err) {
		t.Fatalf("err = %v, a code-less 404 is not a canonical 5011", err)
	}
	if got := aPatches.Load(); got != 1 {
		t.Fatalf("controller A patches = %d, want 1", got)
	}
	if got := bPatches.Load(); got != 0 {
		t.Fatalf("controller B patches = %d, want 0", got)
	}
}

// An ambiguous transport failure on a write follows the driver's existing
// reconciliation path; this semantic-5011 helper must not fire at all.
func TestModifyShareOnOwnerTransportFailureNoSemanticReplay(t *testing.T) {
	var phase atomic.Int32
	var aPatches, bPatches atomic.Int32
	serverA := serveTwoController(t, &phase, 0, func(w http.ResponseWriter, _ *http.Request) {
		aPatches.Add(1)
		w.Write([]byte(`{"code":5011,"error":"no share here"}`))
	})
	serverB := serveTwoController(t, &phase, 1, func(w http.ResponseWriter, _ *http.Request) {
		bPatches.Add(1)
		w.Write([]byte(`{"success":true}`))
	})
	failFirstPatch := new(atomic.Bool)
	failFirstPatch.Store(true)
	d := newTwoControllerDriver(t, serverA, serverB, failFirstPatch)
	if err := d.client.RefreshController(context.Background()); err != nil {
		t.Fatalf("RefreshController: %v", err)
	}

	if err := d.reconciler().ModifyShareOnOwner(context.Background(), "sh-1", map[string]interface{}{"nfs_read_only": true}); err == nil {
		t.Fatal("modifyShareOnOwner succeeded, want the transport error")
	}
	if got := aPatches.Load(); got != 0 {
		t.Fatalf("controller A object requests = %d, want 0", got)
	}
	if got := bPatches.Load(); got != 0 {
		t.Fatalf("controller B patches = %d, want 0 (no semantic replay on transport failure)", got)
	}
}

// TestDeleteVolumeContradictoryStatusNotAbsence binds the contradictory
// status/code guard to the real DeleteVolume call path: a delete ack followed
// by an absence readback returning HTTP 403/500 carrying the NotFound code
// (5011) must NOT be treated as authoritative absence — DeleteVolume fails
// closed. The general CO error mapping may still classify code 5011 as
// NotFound, but the post-delete readback requires an actual HTTP 404.
func TestDeleteVolumeContradictoryStatusNotAbsence(t *testing.T) {
	const detailRoute = "GET /api/v2/share/vol-1"
	const deleteRoute = "DELETE /api/v2/share/vol-1"
	present := controllerBackendResponse{body: `{"id":"vol-1","name":"share","pool_name":"pool"}`}
	okDelete := controllerBackendResponse{body: `{"success":true}`}
	for _, tc := range []struct {
		name   string
		status int
	}{
		{"403 carrying NotFound code", http.StatusForbidden},
		{"500 carrying NotFound code", http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			contradictory := controllerBackendResponse{status: tc.status, body: `{"code":5011,"error":"not found"}`}
			routes := map[string][]controllerBackendResponse{
				detailRoute: append([]controllerBackendResponse{present}, repeatResponse(contradictory, 30)...),
				deleteRoute: {okDelete},
			}
			driver, backend := InstallControllerBackend(t, routes)
			driver.deleteVerifyTimeout = 150 * time.Millisecond
			driver.deleteVerifyInterval = 20 * time.Millisecond
			response, err := driver.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-1"})
			if err == nil || response != nil {
				t.Fatalf("DeleteVolume succeeded on contradictory %d+NotFound readback: resp=%v err=%v", tc.status, response, err)
			}
			if code := status.Code(err); code != codes.DeadlineExceeded && code != codes.Internal {
				t.Fatalf("code = %v, want DeadlineExceeded/Internal (err=%v)", code, err)
			}
			if deletes := len(backend.RequestsFor(http.MethodDelete, "/api/v2/share/vol-1")); deletes != 1 {
				t.Fatalf("share DELETE calls = %d, want exactly 1", deletes)
			}
			gets := backend.RequestsFor(http.MethodGet, "/api/v2/share/vol-1")
			if len(gets) < 2 {
				t.Fatalf("expected an absence readback GET after the delete ack, got %d GETs", len(gets))
			}
		})
	}
}
