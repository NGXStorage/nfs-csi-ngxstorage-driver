package driver

import (
	"context"
	utils "github.com/ngxstorage/nfs-csi-ngxstorage-driver/pkg/utils"
	"net/http"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestResolveDriverName(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		want   string
	}{
		{name: "empty prefix preserves canonical name", prefix: "", want: DriverName},
		{name: "whitespace prefix preserves canonical name", prefix: "  ", want: DriverName},
		{name: "201 prefix prepends label", prefix: "201", want: "201." + DriverName},
		{name: "205 prefix prepends label", prefix: "205", want: "205." + DriverName},
		{name: "prefix is lower-cased", prefix: "NFS40", want: "nfs40." + DriverName},
		{name: "trailing dot is trimmed", prefix: "201.", want: "201." + DriverName},
		{name: "dash prefix is preserved", prefix: "nfs-1g", want: "nfs-1g." + DriverName},
		{name: "helm-rendered quotes are stripped", prefix: `"201"`, want: "201." + DriverName},
		{name: "quoted empty becomes canonical", prefix: `""`, want: DriverName},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ResolveDriverName(test.prefix); got != test.want {
				t.Fatalf("ResolveDriverName(%q) = %q, want %q", test.prefix, got, test.want)
			}
		})
	}
}

func TestNewDriverUsesConfiguredDriverPrefix(t *testing.T) {
	backend := &controllerBackend{responses: map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/status/cluster": {{
			body: `{"status":"Master","connected":1}`,
		}},
		http.MethodGet + " /api/v2/pool": {{
			body: `[{"id":"pool-id","name":"pool","owner":"192.0.2.10"}]`,
		}},
	}}
	driver, err := NewDriverWithHTTPClient(InputParameters{
		Endpoint:     "http://127.0.0.1",
		ApiKey:       "key",
		Ip:           "192.0.2.10",
		PoolName:     "pool",
		DataIP:       "192.0.2.20",
		NodeId:       "node-1",
		DriverPrefix: "201",
	}, &http.Client{Transport: backend})
	if err != nil {
		t.Fatal(err)
	}
	if driver.Name != "201."+DriverName {
		t.Fatalf("driver name = %q, want %q", driver.Name, "201."+DriverName)
	}
}

func TestNewDriverPreservesCanonicalNameWithoutPrefix(t *testing.T) {
	backend := &controllerBackend{responses: map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/status/cluster": {{
			body: `{"status":"Master","connected":1}`,
		}},
		http.MethodGet + " /api/v2/pool": {{
			body: `[{"id":"pool-id","name":"pool","owner":"192.0.2.10"}]`,
		}},
	}}
	driver, err := NewDriverWithHTTPClient(InputParameters{
		Endpoint: "http://127.0.0.1",
		ApiKey:   "key",
		Ip:       "192.0.2.10",
		PoolName: "pool",
		DataIP:   "192.0.2.20",
		NodeId:   "node-1",
	}, &http.Client{Transport: backend})
	if err != nil {
		t.Fatal(err)
	}
	if driver.Name != DriverName {
		t.Fatalf("driver name = %q, want canonical %q", driver.Name, DriverName)
	}
}

func TestInputParametersStringRedactsAPIKey(t *testing.T) {
	parameters := &InputParameters{
		Endpoint: "http://127.0.0.1",
		ApiKey:   "super-secret",
		Ip:       "192.0.2.10",
		PoolName: "pool",
		DataIP:   "192.0.2.20",
		NodeId:   "node-1",
		Log:      "warn",
	}
	value := parameters.String()
	if strings.Contains(value, parameters.ApiKey) || !strings.Contains(value, "[REDACTED]") {
		t.Fatalf("InputParameters.String() = %q", value)
	}
}

func TestNewDriverRejectsMissingRequiredConfiguration(t *testing.T) {
	valid := InputParameters{
		Endpoint: "http://127.0.0.1",
		ApiKey:   "key",
		Ip:       "192.0.2.10",
		PoolName: "pool",
		DataIP:   "192.0.2.20",
		NodeId:   "node-1",
	}
	tests := []struct {
		name   string
		mutate func(*InputParameters)
	}{
		{name: "IP", mutate: func(value *InputParameters) { value.Ip = "" }},
		{name: "pool", mutate: func(value *InputParameters) { value.PoolName = "" }},
		{name: "API key", mutate: func(value *InputParameters) { value.ApiKey = "" }},
		{name: "endpoint", mutate: func(value *InputParameters) { value.Endpoint = "" }},
		{name: "node ID", mutate: func(value *InputParameters) { value.NodeId = "defaultNodeIDValue" }},
		{name: "node ID empty", mutate: func(value *InputParameters) { value.NodeId = "" }},
		{name: "data IP sentinel", mutate: func(value *InputParameters) { value.DataIP = "defaultDataIpValue" }},
		{name: "data IP empty", mutate: func(value *InputParameters) { value.DataIP = "" }},
		{name: "role", mutate: func(value *InputParameters) { value.Role = "bogus" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parameters := valid
			test.mutate(&parameters)
			_, err := NewDriver(parameters)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("NewDriver() code = %s, want %s: %v", status.Code(err), codes.InvalidArgument, err)
			}
		})
	}
}

func TestNewDriverInitializesPlacementCapabilitiesAndMountPermissions(t *testing.T) {
	backend := &controllerBackend{responses: map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/status/cluster": {{
			body: `{"status":"Master","connected":1}`,
		}},
		http.MethodGet + " /api/v2/pool": {{
			body: `[{"id":"pool-id","name":"pool","owner":"192.0.2.10"}]`,
		}},
	}}
	driver, err := NewDriverWithHTTPClient(InputParameters{
		Endpoint: "http://127.0.0.1",
		ApiKey:   "key",
		Ip:       "192.0.2.10",
		PoolName: "pool",
		DataIP:   "192.0.2.20",
		NodeId:   "node-1",
	}, &http.Client{Transport: backend})
	if err != nil {
		t.Fatal(err)
	}
	if driver.mountPermissions != 0o777 {
		t.Fatalf("mount permissions = %#o, want 0777", driver.mountPermissions)
	}
	if driver.Role != "node" {
		t.Fatalf("default role = %q, want node", driver.Role)
	}
	if driver.Ready {
		t.Fatal("driver became ready before Run registered the CSI services")
	}
	if len(driver.nsCap) != len(supportedNodeCapabilities) ||
		len(driver.csCap) != len(supportedControllerCapabilities) ||
		len(driver.AccessModes) != len(supportedAccessModes) {
		t.Fatalf("initialized capabilities = node %d controller %d access %d",
			len(driver.nsCap), len(driver.csCap), len(driver.AccessModes))
	}
}

func TestDriverDoesNotImplementGroupController(t *testing.T) {
	if _, implemented := any(&Driver{}).(csi.GroupControllerServer); implemented {
		t.Fatal("Driver unexpectedly implements the unsupported GroupController service")
	}
}

func TestSupportedCapabilitySetsExcludeUnsupportedRPCs(t *testing.T) {
	for _, capability := range supportedNodeCapabilities {
		switch capability {
		case csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
			csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
			csi.NodeServiceCapability_RPC_VOLUME_MOUNT_GROUP:
			t.Fatalf("unsupported node capability is configured: %s", capability)
		}
	}
	for _, capability := range supportedControllerCapabilities {
		switch capability {
		case csi.ControllerServiceCapability_RPC_CLONE_VOLUME:
			t.Fatalf("unsupported controller capability is configured: %s", capability)
		}
	}
}

func TestNewDriverAcceptsExplicitRoles(t *testing.T) {
	backend := &controllerBackend{responses: map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/status/cluster": {
			{body: `{"status":"Master","connected":1}`},
			{body: `{"status":"Master","connected":1}`},
			{body: `{"status":"Master","connected":1}`},
			{body: `{"status":"Master","connected":1}`},
		},
		http.MethodGet + " /api/v2/pool": {
			{body: `[{"id":"pool-id","name":"pool","owner":"192.0.2.10"}]`},
			{body: `[{"id":"pool-id","name":"pool","owner":"192.0.2.10"}]`},
			{body: `[{"id":"pool-id","name":"pool","owner":"192.0.2.10"}]`},
			{body: `[{"id":"pool-id","name":"pool","owner":"192.0.2.10"}]`},
		},
	}}
	for _, test := range []struct {
		role   string
		want   string
		nodeID string
	}{
		{role: "", want: "node", nodeID: "node-1"},
		{role: "node", want: "node", nodeID: "node-1"},
		{role: "controller", want: "controller", nodeID: ""},
		{role: "all", want: "all", nodeID: "node-1"},
	} {
		t.Run("role="+test.role, func(t *testing.T) {
			driver, err := NewDriverWithHTTPClient(InputParameters{
				Endpoint: "http://127.0.0.1",
				ApiKey:   "key",
				Ip:       "192.0.2.10",
				PoolName: "pool",
				DataIP:   "192.0.2.20",
				NodeId:   test.nodeID,
				Role:     test.role,
			}, &http.Client{Transport: backend})
			if err != nil {
				t.Fatal(err)
			}
			if driver.Role != test.want {
				t.Fatalf("driver role = %q, want %q", driver.Role, test.want)
			}
		})
	}
}

func TestNewDriverNodeRequiresNodeID(t *testing.T) {
	backend := &controllerBackend{responses: map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/status/cluster": {{body: `{"status":"Master","connected":1}`}},
		http.MethodGet + " /api/v2/pool":           {{body: `[{"id":"pool-id","name":"pool","owner":"192.0.2.10"}]`}},
	}}
	_, err := NewDriverWithHTTPClient(InputParameters{
		Endpoint: "http://127.0.0.1",
		ApiKey:   "key",
		Ip:       "192.0.2.10",
		PoolName: "pool",
		DataIP:   "192.0.2.20",
		Role:     "node",
	}, &http.Client{Transport: backend})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("NewDriver() code = %s, want %s: %v", status.Code(err), codes.InvalidArgument, err)
	}
}

func TestRunRejectsNonUnixEndpoint(t *testing.T) {
	driver := &Driver{Endpoint: "tcp://127.0.0.1:10000"}
	if err := driver.Run(); err == nil {
		t.Fatal("Run() accepted a non-unix endpoint")
	}
}

func TestRegisterServicesRegistersNodeAndController(t *testing.T) {
	server := grpc.NewServer()
	defer server.Stop()

	driver := &Driver{Role: "all"}
	csi.RegisterNodeServer(server, driver)
	csi.RegisterControllerServer(server, driver)
	csi.RegisterIdentityServer(server, driver)

	info := server.GetServiceInfo()
	for _, service := range []string{"csi.v1.Identity", "csi.v1.Controller", "csi.v1.Node"} {
		if _, registered := info[service]; !registered {
			t.Fatalf("%s service not registered: %#v", service, info)
		}
	}
}

// ---------------------------------------------------------------------------
// Shared retry sleeper test hook (merged from reconcile_test.go)
// ---------------------------------------------------------------------------

// installFakeSleeper replaces the between-attempt backoff and counts calls.
func installFakeSleeper(t *testing.T, fn func(context.Context, int) error) *int {
	t.Helper()
	calls := new(int)
	original := utils.ReconcileSleeper
	utils.ReconcileSleeper = func(ctx context.Context, attempt int) error {
		*calls++
		if fn != nil {
			return fn(ctx, attempt)
		}
		return nil
	}
	t.Cleanup(func() { utils.ReconcileSleeper = original })
	return calls
}
