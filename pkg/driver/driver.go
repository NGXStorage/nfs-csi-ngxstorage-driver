package driver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	coord "github.com/NGXStorage/csi-ngxstorage-utils/coord"
	kubeutils "github.com/NGXStorage/csi-ngxstorage-utils/kubernetes"
	nodestate "github.com/NGXStorage/csi-ngxstorage-utils/node"
	logutils "github.com/ngxstorage/go-logutils"
	helper "github.com/ngxstorage/nfs-csi-ngxstorage-driver/pkg/helper"
	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	zap "go.uber.org/zap"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
)

// DriverName is the canonical CSI driver name; DriverVersion is the version
// advertised by the identity service.
const (
	DriverName    = "nfs.csi.ngxstorage.com"
	DriverVersion = "26.01"
)

// coordinationReleaseTimeout bounds the Lease release after a controller RPC
// critical section. Release runs on a fresh background context with this
// timeout so a cancelled request cannot leak a held Lease.
const coordinationReleaseTimeout = 5 * time.Second

// DeleteVolume read-after-delete verification bounds. A successful backend
// delete response is NOT proof of absence (the array can acknowledge a delete
// it cannot perform, e.g. a share whose snapshot still has clones). The
// driver verifies authoritative NotFound before claiming CSI success; the
// budget is capped by the request context. 403/timeout/5xx are never absence.

var (
	supportedNodeCapabilities = []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
		csi.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
		csi.NodeServiceCapability_RPC_GET_VOLUME_HEALTH,
		csi.NodeServiceCapability_RPC_GET_STORAGE_HEALTH,
	}
	supportedControllerCapabilities = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
		csi.ControllerServiceCapability_RPC_GET_CAPACITY,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
		csi.ControllerServiceCapability_RPC_GET_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_READONLY,
		csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
		csi.ControllerServiceCapability_RPC_MODIFY_VOLUME,
		csi.ControllerServiceCapability_RPC_GET_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_GET_VOLUME_HEALTH,
		csi.ControllerServiceCapability_RPC_LIST_VOLUME_HEALTH,
	}
	supportedAccessModes = []csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER,
	}
)

// Driver represents a Container Storage Interface (CSI) driver implementation.
type Driver struct {
	logger *zap.Logger
	NodeId string // nodeid is the identifier of the node where the driver is running.
	Role   string // role is the CSI role: node or controller.

	Endpoint string // endpoint is the CSI endpoint.

	Server *grpc.Server // server is the gRPC server used by the driver.

	// client is the unified NGX API v2 SDK client. NFS-specific interpretation
	// and state live in pkg/helper, not in the SDK or CSI RPC implementation.
	client *ngxstorage.Client

	// dataIP is returned in publish context; dataPath is initialized from the
	// canonical export base path owned by pkg/helper.
	dataIP   string
	dataPath string

	volumeHashes helper.VolumeHashIndex

	// deleteVerifyTimeout/deleteVerifyInterval bound the DeleteVolume
	// read-after-delete verification; zero values use the package defaults.
	// Overridable in tests for deterministic budgets.
	deleteVerifyTimeout  time.Duration
	deleteVerifyInterval time.Duration

	Name    string // Name is the name of the CSI driver.
	Version string // Version is the version of the CSI driver.
	Ready   bool   // ready indicates whether the driver is ready to serve requests.

	mountPermissions   uint64                                                // mountPermissions specifies the mount permissions.
	temporaryMountRoot string                                                // temporaryMountRoot is the host-shared directory for static-PV setup mounts.
	mounter            mount.Interface                                       // mounter is the interface for performing mounts.
	mountFn            func(context.Context, string, string, []string) error // mountFn permits deterministic mount retry tests.
	pathStat           func(string) (os.FileInfo, error)                     // pathStat permits deterministic stale-mount handling tests.
	AccessModes        []csi.VolumeCapability_AccessMode_Mode                // available access modes for the driver
	csCap              []*csi.ControllerServiceCapability                    // csCap represents the list of Controller Service Capabilities supported by the driver.
	nsCap              []*csi.NodeServiceCapability                          // nsCap represents the list of Node Service Capabilities supported by the driver.
	nodeState          nodestate.State                                       // nodeState owns node-local publish bookkeeping and per-volume locks.
	// coordination serializes controller mutations across replicas. NFS node
	// operations remain node-local and never require Kubernetes Lease access.
	coordination coord.Coordinator
	// nodeLookup resolves Kubernetes node IDs for controller publish/unpublish
	// validation. Node-only processes never build it.
	nodeLookup kubeutils.NodeLookup

	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer
}

// InputParameters represents configuration parameters required to initialize a CSI driver.
type InputParameters struct {
	Logger   *zap.Logger
	Endpoint string // Endpoint is the endpoint used by the CSI driver.
	ApiKey   string // ApiKey is the authentication key required for accessing APIs.
	Ip       string // Ip is the comma-separated NGX storage controller addresses.
	PoolName string // PoolName is the name of the storage pool used by the driver.
	DataIP   string // DataIP is the NFS data address returned in publish context.
	NodeId   string // NodeID is the identifier of the node where the driver is running.
	Role     string // Role is the CSI role: node or controller.
	Log      string // Console log level for the driver.
	// DriverPrefix optionally prepends a user label to the canonical CSI driver
	// name. A non-empty value changes the advertised driver name to
	// <prefix>.nfs.csi.ngxstorage.com so multiple driver instances can serve
	// distinct NGX backends on one cluster. An empty value preserves
	// nfs.csi.ngxstorage.com.
	DriverPrefix string
}

// mountService assembles the NFS mount orchestration from the current seams.
func (d *Driver) mountService() *helper.MountService {
	return &helper.MountService{MountFn: d.mountFn, Mounter: d.mounter, Permissions: os.FileMode(d.mountPermissions), Log: d.logger}
}

// publishService assembles the node publish preparation steps.
func (d *Driver) publishService() *helper.PublishService {
	return &helper.PublishService{NodeState: &d.nodeState, Mounter: d.mounter, Mount: d.mountService(), DataIP: d.dataIP, DataPath: d.dataPath, MountPermissions: d.mountPermissions, TemporaryMountRoot: d.temporaryMountRoot, Log: d.logger}
}

// reconciler assembles the backend share reconcile service.
func (d *Driver) reconciler() *helper.Reconciler {
	return &helper.Reconciler{Client: d.client, DeleteVerifyTimeout: d.deleteVerifyTimeout, DeleteVerifyInterval: d.deleteVerifyInterval, Log: d.logger}
}

func (d *Driver) log() *zap.Logger {
	if d != nil && d.logger != nil {
		return d.logger
	}
	return zap.NewNop()
}

// ResolveDriverName returns the effective CSI driver name for the configured
// prefix. An empty prefix preserves the canonical nfs.csi.ngxstorage.com name;
// a non-empty prefix becomes the leading DNS label
// (<prefix>.nfs.csi.ngxstorage.com). The prefix is lower-cased, trimmed, and
// unquoted so configuration from Helm (which may render quote characters)
// cannot accidentally produce an invalid driver identity.
func ResolveDriverName(prefix string) string {
	prefix = strings.TrimSpace(strings.ToLower(prefix))
	prefix = strings.Trim(prefix, "\"")
	prefix = strings.TrimSuffix(prefix, ".")
	if prefix == "" {
		return DriverName
	}
	return prefix + "." + DriverName
}

// String returns a redacted diagnostic representation of driver parameters.
func (p *InputParameters) String() string {
	return fmt.Sprintf("[ %s %s %s %s [REDACTED] %s %s %s %s ]",
		p.Endpoint, p.Ip, p.PoolName, p.DataIP, p.NodeId, p.Role, p.Log, p.DriverPrefix)
}

/*
 * NewDriver validates local configuration, creates the NGX client, and resolves
 * a serviceable controller/pool placement before any CSI socket is served.
 * Initialization is bounded to 60 seconds and never logs the API key.
 *
 * @param parameters: Requires controller address, canonical pool name, API key,
 *                    unix endpoint, and a node ID for the node role. The
 *                    controller role does not require node identity. DataIP
 *                    is returned later in NFS publish context.
 *
 * @return *Driver: Initialized but not-yet-serving controller or node service
 *         with canonical capabilities and 0777 local mount permissions. NFS
 *         CSI mounts are consumed by arbitrary OpenShift UIDs, so the target
 *         path must be traversable without requiring a privileged SCC.
 * @return error: InvalidArgument for local configuration, NotFound for an
 *         absent configured pool, Unavailable for unusable controller mode, or
 *         Internal for NGX client construction failure.
 */
func NewDriver(parameters InputParameters) (*Driver, error) {
	return newDriver(parameters, nil)
}

// NewDriverWithHTTPClient provides isolated transport injection for tests.
func NewDriverWithHTTPClient(parameters InputParameters, client *http.Client) (*Driver, error) {
	return newDriver(parameters, client)
}

func newDriver(parameters InputParameters, injectedClient *http.Client) (*Driver, error) {
	logger := parameters.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	logger.Info("NewDriver(): init")
	logger.Debug("NewDriver(): parameters",
		zap.String("parameters", parameters.String()),
	)

	// Validating input parameters to ensure that required fields are provided
	if (parameters.Ip == "") || (parameters.Ip == "defaultIpValue") {
		logger.Error("NewDriver(): IP is empty, must be provided")
		return nil, status.Error(codes.InvalidArgument, "IP is empty, must be provided")
	}

	if (parameters.PoolName == "") || (parameters.PoolName == "defaultPoolNameValue") {
		logger.Error("NewDriver(): PoolName is empty, must be provided")
		return nil, status.Error(codes.InvalidArgument, "PoolName is empty, must be provided")
	}

	if (parameters.ApiKey == "") || (parameters.ApiKey == "defaultApiKeyValue") {
		logger.Error("NewDriver(): ApiKey is empty, must be provided")
		return nil, status.Error(codes.InvalidArgument, "ApiKey is empty, must be provided")
	}

	if parameters.Endpoint == "" || (parameters.Endpoint == "defaultEndpointValue") {
		logger.Error("NewDriver(): Endpoint is empty, must be provided")
		return nil, status.Error(codes.InvalidArgument, "Endpoint is empty, must be provided")
	}

	// The data IP flows into every publish context as the NFS server; an
	// unset flag would silently point every mount at the default sentinel.
	if parameters.DataIP == "" || (parameters.DataIP == "defaultDataIpValue") {
		logger.Error("NewDriver(): DataIP is empty, must be provided")
		return nil, status.Error(codes.InvalidArgument, "DataIP is empty, must be provided")
	}

	if parameters.Role == "" {
		parameters.Role = "node"
	}
	if parameters.Role != "node" && parameters.Role != "controller" && parameters.Role != "all" {
		logger.Error("NewDriver(): Role must be node, controller, or all",
			zap.String("role", parameters.Role),
		)
		return nil, status.Error(codes.InvalidArgument, "Role must be node, controller, or all")
	}
	if (parameters.Role == "node" || parameters.Role == "all") && (parameters.NodeId == "" || parameters.NodeId == "defaultNodeIDValue") {
		logger.Error("NewDriver(): NodeID is empty, must be provided")
		return nil, status.Error(codes.InvalidArgument, "NodeID is empty, must be provided")
	}

	// Construct the unified NGX SDK client. The API key is never logged. The
	// NFS data IP and export path are driver-local (not part of the SDK).
	cfg := ngxstorage.Config{
		Controllers: strings.Split(parameters.Ip, ","),
		APIKey:      parameters.ApiKey,
		PoolName:    parameters.PoolName,
		// NGX appliances use self-signed certificates; skip verification by
		// default.
		InsecureSkipVerify: true,
		Logger:             logutils.NewSDKLogger(logger),
	}
	if injectedClient != nil {
		cfg.HTTPClient = injectedClient
	}
	client, err := ngxstorage.NewClient(cfg)
	if err != nil {
		logger.Error("NewDriver(): storage client creation failed",
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "create storage client: %v", err)
	}
	logger.Info("NewDriver(): storage client created",
		zap.String("pool_name", parameters.PoolName),
		zap.String("controller_ips", parameters.Ip),
	)

	// Resolve the active controller and configured pool once during startup.
	// The SDK repeats this after five minutes and on transport failures.
	initializationContext, cancelInitialization := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelInitialization()
	if err := client.RefreshController(initializationContext); err != nil {
		logger.Error("NewDriver(): controller and pool initialization failed",
			zap.Error(err),
		)
		code := codes.Unavailable
		if errors.Is(err, ngxstorage.ErrPoolNotFound) {
			code = codes.NotFound
		}
		return nil, status.Errorf(code, "driver initialization: %v", err)
	}
	logger.Info("NewDriver(): controller and pool initialized")

	// Build local CSI state only after backend placement is known.
	d := &Driver{
		logger: logger,
		NodeId: parameters.NodeId,
		Role:   parameters.Role,

		Endpoint: parameters.Endpoint,
		client:   client,

		dataIP:   parameters.DataIP,
		dataPath: helper.DataPath,

		// Resolve the effective driver identity from the optional prefix so a
		// second driver instance can register a distinct CSIDriver on the same
		// cluster. An empty prefix preserves the canonical name.
		Name:    ResolveDriverName(parameters.DriverPrefix),
		Version: DriverVersion,
		Ready:   false,

		mountPermissions:   0o777,
		temporaryMountRoot: helper.StaticTemporaryMountRoot,
	}

	// Mount mechanics are node-local and do not carry backend state.
	d.mounter = mount.New("")

	// Materialize only the centralized capability sets advertised by the
	// identity, controller, and node services.
	d.nsCap = make([]*csi.NodeServiceCapability, 0, len(supportedNodeCapabilities))
	for _, capability := range supportedNodeCapabilities {
		logger.Info("NewDriver(): node capability configured",
			zap.String("capability", capability.String()),
		)
		d.nsCap = append(d.nsCap, &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{Type: capability},
			},
		})
	}
	d.csCap = make([]*csi.ControllerServiceCapability, 0, len(supportedControllerCapabilities))
	for _, capability := range supportedControllerCapabilities {
		logger.Info("NewDriver(): controller capability configured",
			zap.String("capability", capability.String()),
		)
		d.csCap = append(d.csCap, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{Type: capability},
			},
		})
	}
	d.AccessModes = append([]csi.VolumeCapability_AccessMode_Mode(nil), supportedAccessModes...)
	for _, mode := range d.AccessModes {
		logger.Info("NewDriver(): access mode configured",
			zap.String("mode", mode.String()),
		)
	}
	logger.Debug("NewDriver(): capabilities configured",
		zap.Reflect("node_capabilities", d.nsCap),
		zap.Reflect("controller_capabilities", d.csCap),
		zap.Reflect("access_modes", d.AccessModes),
	)
	logger.Info("NewDriver(): driver initialized",
		zap.String("name", d.Name),
		zap.String("version", d.Version),
		zap.String("endpoint", d.Endpoint),
		zap.Uint64("mount_permissions", d.mountPermissions),
		zap.String("node_id", d.NodeId),
	)
	return d, nil
}

/*
 * Run creates the configured unix socket, registers the identity service and
 * exactly one node or controller service, then blocks in gRPC Serve.
 * Existing socket removal is limited to the resolved socket path.
 *
 * @return error: Invalid endpoint/scheme, directory or socket preparation
 *         failure, listener failure, or the terminal gRPC Serve error.
 */
func (d *Driver) Run() error {
	logger := d.log()
	logger.Info("Run(): init")
	logger.Debug("Run(): configuration",
		zap.String("endpoint", d.Endpoint),
		zap.String("node_id", d.NodeId),
		zap.String("driver_name", d.Name),
		zap.String("driver_version", d.Version),
	)
	if err := d.prepareCoordination(context.Background()); err != nil {
		logger.Error("Run(): controller coordination preflight failed", zap.Error(err))
		return fmt.Errorf("coordination preflight: %w", err)
	}

	// CSI deployment manifests use a unix endpoint shared with sidecars.
	parsedEndpoint, err := url.Parse(d.Endpoint)
	if err != nil {
		logger.Error("Run(): endpoint parse failed",
			zap.String("endpoint", d.Endpoint),
			zap.Error(err),
		)
		return fmt.Errorf("invalid endpoint: %w", err)
	}
	if parsedEndpoint.Scheme != "unix" {
		logger.Error("Run(): endpoint scheme is unsupported",
			zap.String("scheme", parsedEndpoint.Scheme),
		)
		return fmt.Errorf("invalid endpoint scheme: %s", parsedEndpoint.Scheme)
	}
	socketPath := path.Join(parsedEndpoint.Host, filepath.FromSlash(parsedEndpoint.Path))
	if parsedEndpoint.Host == "" {
		socketPath = filepath.FromSlash(parsedEndpoint.Path)
	}
	socketDirectory := filepath.Dir(socketPath)
	if err := os.MkdirAll(socketDirectory, 0o750); err != nil {
		logger.Error("Run(): CSI socket directory creation failed",
			zap.String("directory", socketDirectory),
			zap.Error(err),
		)
		return fmt.Errorf("create CSI socket directory: %w", err)
	}
	logger.Info("Run(): CSI socket directory is ready",
		zap.String("directory", socketDirectory),
	)

	// Remove only the exact stale socket before opening the listener.
	err = os.Remove(socketPath)
	if err != nil && !os.IsNotExist(err) {
		logger.Error("Run(): existing CSI socket removal failed",
			zap.String("socket_path", socketPath),
			zap.Error(err),
		)
		return fmt.Errorf("remove existing CSI socket: %w", err)
	}
	logger.Info("Run(): CSI socket path is ready",
		zap.String("socket_path", socketPath),
	)

	// Listen before publishing process readiness.
	listener, err := net.Listen(parsedEndpoint.Scheme, socketPath)
	if err != nil {
		logger.Error("Run(): CSI listener creation failed",
			zap.String("socket_path", socketPath),
			zap.Error(err),
		)
		return fmt.Errorf("create CSI listener: %w", err)
	}
	logger.Info("Run(): CSI listener created",
		zap.String("socket_path", socketPath),
	)

	d.Server = grpc.NewServer()
	csi.RegisterIdentityServer(d.Server, d)
	switch d.Role {
	case "controller":
		csi.RegisterControllerServer(d.Server, d)
	case "node":
		csi.RegisterNodeServer(d.Server, d)
	case "all":
		// Test-only mode for csi-sanity and local integration harnesses.
		csi.RegisterNodeServer(d.Server, d)
		csi.RegisterControllerServer(d.Server, d)
	}
	logger.Info("Run(): CSI services registered",
		zap.String("node_id", d.NodeId),
	)

	// Probe becomes ready only after the listener and services exist.
	d.Ready = true
	logger.Info("Run(): CSI service is ready",
		zap.Bool("ready", d.Ready),
		zap.String("socket_path", socketPath),
	)

	// Serve blocks until a terminal listener or server error.
	if err := d.Server.Serve(listener); err != nil {
		logger.Error("Run(): gRPC serve stopped with error",
			zap.String("socket_path", socketPath),
			zap.Error(err),
		)
		return err
	}
	logger.Info("Run(): gRPC serve stopped",
		zap.String("socket_path", socketPath),
	)
	return nil
}

func (d *Driver) prepareCoordination(ctx context.Context) error {
	// NFS has no distributed node mutation. Node-only startup must therefore be
	// independent of Lease environment, RBAC, projected tokens, and API health.
	if d.Role != "controller" && d.Role != "all" {
		return nil
	}
	if d.coordination == nil {
		coordinator, err := coord.NewControllerCoordinatorFromEnvironment(d.Role)
		if err != nil {
			return err
		}
		d.coordination = coordinator
	}
	if d.nodeLookup == nil {
		lookup, err := kubeutils.NewNodeLookup()
		if err != nil {
			return err
		}
		d.nodeLookup = lookup
	}
	return coord.PreflightCoordinator(ctx, d.coordination)
}

// ---------------------------------------------------------------------------
// Shared retry policy for backend mutations and mounts.
//
// The NGX management API can drop a connection while a mutation is in flight
// (observed live as `EOF` on POST/PATCH during management-service restarts).
// The SDK only replays idempotent HTTP methods automatically, so the driver
// reconciles the mutations it can resolve from backend state. Controller
// ownership recovery for semantic not-found answers also depends on this
// policy: the SDK request layer never replays a write. NFS mount retries reuse
// the same bounded schedule.
// ---------------------------------------------------------------------------

// VolumeHashIDByName preserves the driver-level diagnostic API while the
// protocol-specific index is owned by pkg/helper.
func (d *Driver) VolumeHashIDByName(name string) (string, bool) {
	return d.volumeHashes.IDByName(name)
}
