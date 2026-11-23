package driver

import (
	"context"
	"errors"
	"fmt"
	nfs "github.com/ngxstorage/nfs-csi-ngxstorage-driver/pkg/nfs"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
)

type cancelNodeContextOnMountInspection struct {
	mount.Interface
	cancel context.CancelFunc
}

func (m *cancelNodeContextOnMountInspection) IsLikelyNotMountPoint(target string) (bool, error) {
	notMountPoint, err := m.Interface.IsLikelyNotMountPoint(target)
	m.cancel()
	return notMountPoint, err
}

// slowMountNodeMounter delays the final mount call so a concurrent request can
// observe that the first publish is still in flight.
type slowMountNodeMounter struct {
	mount.Interface
	mountStarted chan struct{}
	once         sync.Once
	delay        time.Duration
}

func (m *slowMountNodeMounter) Mount(source, target, fstype string, options []string) error {
	m.once.Do(func() { close(m.mountStarted) })
	time.Sleep(m.delay)
	return m.Interface.Mount(source, target, fstype, options)
}

func DynamicNodePublishRequest(targetPath string) *csi.NodePublishVolumeRequest {
	return &csi.NodePublishVolumeRequest{
		VolumeId:         "vol-1",
		TargetPath:       targetPath,
		VolumeCapability: NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
		VolumeContext: map[string]string{
			"share_id":   "vol-1",
			"share_name": "share",
			"pool_name":  "pool",
		},
		PublishContext: map[string]string{
			"server": "192.0.2.20",
			"path":   "/exports/pool/share",
		},
	}
}

func StaticNodePublishRequest(targetPath string) *csi.NodePublishVolumeRequest {
	return &csi.NodePublishVolumeRequest{
		VolumeId:         DriverName + ".existing",
		TargetPath:       targetPath,
		VolumeCapability: NFSMountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
		VolumeContext:    map[string]string{},
		PublishContext: map[string]string{
			"server": "192.0.2.20",
			"path":   "/exports/pool/existing",
		},
	}
}

func TestNodePublishVolumeValidatesRequiredFieldsInline(t *testing.T) {
	driver := &Driver{AccessModes: nfsTestModes}
	valid := DynamicNodePublishRequest("/target")

	for _, test := range []struct {
		name    string
		request *csi.NodePublishVolumeRequest
	}{
		{name: "nil request"},
		{name: "missing volume ID", request: &csi.NodePublishVolumeRequest{}},
		{name: "missing target path", request: &csi.NodePublishVolumeRequest{VolumeId: valid.GetVolumeId()}},
		{
			name: "missing volume context",
			request: &csi.NodePublishVolumeRequest{
				VolumeId:   valid.GetVolumeId(),
				TargetPath: valid.GetTargetPath(),
			},
		},
		{
			name: "missing publish context",
			request: &csi.NodePublishVolumeRequest{
				VolumeId:      valid.GetVolumeId(),
				TargetPath:    valid.GetTargetPath(),
				VolumeContext: valid.GetVolumeContext(),
			},
		},
		{
			name: "missing capability",
			request: &csi.NodePublishVolumeRequest{
				VolumeId:       valid.GetVolumeId(),
				TargetPath:     valid.GetTargetPath(),
				VolumeContext:  valid.GetVolumeContext(),
				PublishContext: valid.GetPublishContext(),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := driver.NodePublishVolume(context.Background(), test.request)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("NodePublishVolume() code = %s, want %s: %v", status.Code(err), codes.InvalidArgument, err)
			}
		})
	}
}

func TestNodePublishVolumeUsesCanonicalPublishContext(t *testing.T) {
	driver, backend := InstallControllerBackend(t, nil)
	fakeMounter := mount.NewFakeMounter(nil)
	driver.mounter = fakeMounter
	driver.mountPermissions = 0o750
	targetPath := filepath.Join(t.TempDir(), "mount")

	_, err := driver.NodePublishVolume(context.Background(), DynamicNodePublishRequest(targetPath))
	if err != nil {
		t.Fatal(err)
	}
	actions := fakeMounter.GetLog()
	if len(actions) != 1 || actions[0].Action != mount.FakeActionMount {
		t.Fatalf("mount actions = %#v", actions)
	}
	if actions[0].Source != "192.0.2.20:/exports/pool/share" || actions[0].Target != targetPath {
		t.Fatalf("mount action = %#v", actions[0])
	}
	if len(backend.RequestsFor("GET", "/api/v2/status/services")) != 0 {
		t.Fatal("NodePublishVolume contacted NGX instead of using publish context")
	}
}

func TestNodePublishVolumeAddsReadOnlyMountOption(t *testing.T) {
	driver, _ := InstallControllerBackend(t, nil)
	fakeMounter := mount.NewFakeMounter(nil)
	driver.mounter = fakeMounter
	targetPath := filepath.Join(t.TempDir(), "mount")
	request := DynamicNodePublishRequest(targetPath)
	request.Readonly = true

	if _, err := driver.NodePublishVolume(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	mountPoints, err := fakeMounter.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(mountPoints) != 1 {
		t.Fatalf("mount points = %#v", mountPoints)
	}
	for _, option := range mountPoints[0].Opts {
		if option == "ro" {
			return
		}
	}
	t.Fatalf("readonly option missing from %#v", mountPoints[0].Opts)
}

func TestNodePublishVolumePreparesStaticRecursiveDirectory(t *testing.T) {
	driver, backend := InstallControllerBackend(t, nil)
	fakeMounter := mount.NewFakeMounter(nil)
	// FakeMounter does not hide mounted contents. Remove the simulated remote
	// child during unmount so CleanupMountPoint sees the empty local mountpoint
	// that a real NFS unmount leaves behind.
	fakeMounter.UnmountFunc = func(target string) error {
		return os.RemoveAll(filepath.Join(target, "pv-static"))
	}
	driver.mounter = fakeMounter
	driver.mountPermissions = 0o750
	driver.temporaryMountRoot = t.TempDir()
	targetPath := filepath.Join(t.TempDir(), "pv-static", "mount")

	_, err := driver.NodePublishVolume(context.Background(), StaticNodePublishRequest(targetPath))
	if err != nil {
		t.Fatal(err)
	}

	actions := fakeMounter.GetLog()
	if len(actions) != 5 {
		t.Fatalf("mount actions = %#v", actions)
	}
	if actions[0].Action != mount.FakeActionMount ||
		actions[0].Source != "192.0.2.20:/exports/pool/existing" {
		t.Fatalf("preparation mount action = %#v", actions[0])
	}
	if filepath.Dir(actions[0].Target) != driver.temporaryMountRoot {
		t.Fatalf("preparation mount root = %q, want %q", filepath.Dir(actions[0].Target), driver.temporaryMountRoot)
	}
	if actions[1].Action != mount.FakeActionUnmount ||
		actions[1].Target != actions[0].Target {
		t.Fatalf("preparation unmount action = %#v", actions[1])
	}
	// The final publish mounts the base export and bind-mounts the PV child
	// directory because NFS clients cannot mount a share subpath directly.
	if actions[2].Action != mount.FakeActionMount ||
		actions[2].Source != "192.0.2.20:/exports/pool/existing" {
		t.Fatalf("publish mount action = %#v", actions[2])
	}
	if actions[3].Action != mount.FakeActionMount ||
		actions[3].Source != filepath.Join(actions[2].Target, "pv-static") ||
		actions[3].Target != targetPath {
		t.Fatalf("bind mount action = %#v", actions[3])
	}
	if actions[4].Action != mount.FakeActionUnmount ||
		actions[4].Target != actions[2].Target {
		t.Fatalf("publish unmount action = %#v", actions[4])
	}
	if len(backend.RequestsFor("GET", "/api/v2/share/list")) != 0 {
		t.Fatal("NodePublishVolume performed a static-share backend lookup")
	}
}

func TestNodePublishVolumeStaticAllowsNilVolumeContext(t *testing.T) {
	driver, _ := InstallControllerBackend(t, nil)
	fakeMounter := mount.NewFakeMounter(nil)
	fakeMounter.UnmountFunc = func(target string) error {
		return os.RemoveAll(filepath.Join(target, "pv-static"))
	}
	driver.mounter = fakeMounter
	driver.mountPermissions = 0o750
	driver.temporaryMountRoot = t.TempDir()
	request := StaticNodePublishRequest(filepath.Join(t.TempDir(), "pv-static", "mount"))
	request.VolumeContext = nil

	if _, err := driver.NodePublishVolume(context.Background(), request); err != nil {
		t.Fatalf("NodePublishVolume() error = %v", err)
	}
}

func TestNodePublishVolumeStaticRecursiveMountIsIdempotent(t *testing.T) {
	driver, _ := InstallControllerBackend(t, nil)
	targetPath := filepath.Join(t.TempDir(), "pv-static", "mount")
	if err := os.MkdirAll(targetPath, 0o750); err != nil {
		t.Fatal(err)
	}
	source := "192.0.2.20:/exports/pool/existing/pv-static"
	fakeMounter := mount.NewFakeMounter([]mount.MountPoint{{
		Device: source,
		Path:   targetPath,
		Type:   "nfs",
		Opts:   []string{"rw", "vers=3", "nolock"},
	}})
	fakeMounter.UnmountFunc = func(target string) error {
		return os.RemoveAll(filepath.Join(target, "pv-static"))
	}
	driver.mounter = fakeMounter
	driver.temporaryMountRoot = t.TempDir()

	_, err := driver.NodePublishVolume(context.Background(), StaticNodePublishRequest(targetPath))
	if err != nil {
		t.Fatal(err)
	}
	actions := fakeMounter.GetLog()
	// A pre-bound target is unbound and republished so a stale bind never
	// blocks a retry: some unmount of targetPath must precede a mount of it.
	unboundAt, reboundAt := -1, -1
	for index, action := range actions {
		if action.Action == mount.FakeActionUnmount && action.Target == targetPath && unboundAt == -1 {
			unboundAt = index
		}
		if action.Action == mount.FakeActionMount && action.Target == targetPath {
			reboundAt = index
		}
	}
	if unboundAt < 0 || reboundAt < unboundAt {
		t.Fatalf("idempotent static publish did not unbind then rebind the target: %#v", actions)
	}
}

func TestNodePublishVolumeRejectsInconsistentContext(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*csi.NodePublishVolumeRequest)
	}{
		{
			name: "missing publish path",
			mutate: func(req *csi.NodePublishVolumeRequest) {
				delete(req.PublishContext, "path")
			},
		},
		{
			name: "volume ID mismatch",
			mutate: func(req *csi.NodePublishVolumeRequest) {
				req.VolumeContext["share_id"] = "other-volume"
			},
		},
		{
			name: "share path mismatch",
			mutate: func(req *csi.NodePublishVolumeRequest) {
				req.PublishContext["path"] = "/exports/pool/other-share"
			},
		},
		{
			name: "server mismatch",
			mutate: func(req *csi.NodePublishVolumeRequest) {
				req.PublishContext["server"] = "192.0.2.99"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver, _ := InstallControllerBackend(t, nil)
			fakeMounter := mount.NewFakeMounter(nil)
			driver.mounter = fakeMounter
			req := DynamicNodePublishRequest(filepath.Join(t.TempDir(), "mount"))
			test.mutate(req)

			_, err := driver.NodePublishVolume(context.Background(), req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("NodePublishVolume() code = %s, want %s: %v", status.Code(err), codes.InvalidArgument, err)
			}
			if actions := fakeMounter.GetLog(); len(actions) != 0 {
				t.Fatalf("invalid context caused mount actions: %#v", actions)
			}
		})
	}
}

func TestNodePublishVolumeMountCompatibility(t *testing.T) {
	source := "192.0.2.20:/exports/pool/share"

	for _, test := range []struct {
		name     string
		device   string
		fsType   string
		options  []string
		mutate   func(*csi.NodePublishVolumeRequest)
		wantCode codes.Code
	}{
		{
			name:    "compatible repeat",
			device:  source,
			fsType:  "nfs",
			options: []string{"rw", "vers=3", "nolock", "hard", "relatime"},
		},
		{
			name:     "source mismatch",
			device:   "192.0.2.21:/exports/pool/other",
			fsType:   "nfs",
			options:  []string{"rw", "vers=3", "nolock"},
			wantCode: codes.AlreadyExists,
		},
		{
			name:     "requested readonly conflicts with writable mount",
			device:   source,
			fsType:   "nfs",
			options:  []string{"rw", "vers=3", "nolock"},
			mutate:   func(req *csi.NodePublishVolumeRequest) { req.Readonly = true },
			wantCode: codes.AlreadyExists,
		},
		{
			name:     "requested writable conflicts with readonly mount",
			device:   source,
			fsType:   "nfs",
			options:  []string{"ro", "vers=3", "nolock"},
			wantCode: codes.AlreadyExists,
		},
		{
			name:    "requested option matches",
			device:  source,
			fsType:  "nfs",
			options: []string{"rw", "vers=3", "nolock", "timeo=600"},
			mutate: func(req *csi.NodePublishVolumeRequest) {
				req.VolumeCapability.GetMount().MountFlags = []string{"timeo=600"}
			},
		},
		{
			name:    "requested option differs",
			device:  source,
			fsType:  "nfs",
			options: []string{"rw", "vers=3", "nolock", "timeo=300"},
			mutate: func(req *csi.NodePublishVolumeRequest) {
				req.VolumeCapability.GetMount().MountFlags = []string{"timeo=600"}
			},
			wantCode: codes.AlreadyExists,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			targetPath := filepath.Join(t.TempDir(), "mount")
			if err := os.MkdirAll(targetPath, 0o750); err != nil {
				t.Fatal(err)
			}
			driver, _ := InstallControllerBackend(t, nil)
			fakeMounter := mount.NewFakeMounter([]mount.MountPoint{{
				Device: test.device,
				Path:   targetPath,
				Type:   test.fsType,
				Opts:   test.options,
			}})
			driver.mounter = fakeMounter
			request := DynamicNodePublishRequest(targetPath)
			if test.mutate != nil {
				test.mutate(request)
			}

			_, err := driver.NodePublishVolume(context.Background(), request)
			if status.Code(err) != test.wantCode {
				t.Fatalf("NodePublishVolume() code = %s, want %s: %v", status.Code(err), test.wantCode, err)
			}
			if actions := fakeMounter.GetLog(); len(actions) != 0 {
				t.Fatalf("existing target caused mount actions: %#v", actions)
			}
		})
	}
}

func TestNodePublishVolumeAllowsDifferentTargets(t *testing.T) {
	driver, _ := InstallControllerBackend(t, nil)
	fakeMounter := mount.NewFakeMounter(nil)
	driver.mounter = fakeMounter
	firstTarget := filepath.Join(t.TempDir(), "first")
	secondTarget := filepath.Join(t.TempDir(), "second")

	for _, target := range []string{firstTarget, secondTarget} {
		if _, err := driver.NodePublishVolume(context.Background(), DynamicNodePublishRequest(target)); err != nil {
			t.Fatalf("NodePublishVolume(%s): %v", target, err)
		}
	}
	actions := fakeMounter.GetLog()
	if len(actions) != 2 ||
		actions[0].Action != mount.FakeActionMount ||
		actions[1].Action != mount.FakeActionMount ||
		actions[0].Target == actions[1].Target {
		t.Fatalf("different-target mount actions = %#v", actions)
	}
}

func TestNodePublishVolumeRejectsSecondTargetForSingleNodeSingleWriter(t *testing.T) {
	driver, _ := InstallControllerBackend(t, nil)
	fakeMounter := mount.NewFakeMounter(nil)
	driver.mounter = fakeMounter
	firstTarget := filepath.Join(t.TempDir(), "first")
	secondTarget := filepath.Join(t.TempDir(), "second")

	request := func(target string) *csi.NodePublishVolumeRequest {
		return &csi.NodePublishVolumeRequest{
			VolumeId:         "vol-1",
			TargetPath:       target,
			VolumeCapability: NFSMountCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER),
			VolumeContext: map[string]string{
				"share_id":   "vol-1",
				"share_name": "share",
				"pool_name":  "pool",
			},
			PublishContext: map[string]string{
				"server": "192.0.2.20",
				"path":   "/exports/pool/share",
			},
		}
	}

	if _, err := driver.NodePublishVolume(context.Background(), request(firstTarget)); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if _, err := driver.NodePublishVolume(context.Background(), request(firstTarget)); err != nil {
		t.Fatalf("same-target republish must be idempotent: %v", err)
	}
	_, err := driver.NodePublishVolume(context.Background(), request(secondTarget))
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second target code = %s, want %s: %v", status.Code(err), codes.FailedPrecondition, err)
	}

	if _, err := driver.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "vol-1",
		TargetPath: firstTarget,
	}); err != nil {
		t.Fatalf("unpublish: %v", err)
	}
	if _, err := driver.NodePublishVolume(context.Background(), request(secondTarget)); err != nil {
		t.Fatalf("publish after unpublish: %v", err)
	}
}

func TestNodePublishVolumeRunsDifferentVolumesInParallel(t *testing.T) {
	driver, _ := InstallControllerBackend(t, nil)
	slowMounter := &slowMountNodeMounter{
		Interface:    mount.NewFakeMounter(nil),
		mountStarted: make(chan struct{}),
		delay:        300 * time.Millisecond,
	}
	driver.mounter = slowMounter
	driver.mountPermissions = 0o750

	firstTarget := filepath.Join(t.TempDir(), "first")
	secondTarget := filepath.Join(t.TempDir(), "second")
	first := DynamicNodePublishRequest(firstTarget)
	second := DynamicNodePublishRequest(secondTarget)
	second.VolumeId = "vol-2"
	second.VolumeContext["share_id"] = "vol-2"

	firstResult := make(chan error, 1)
	go func() {
		_, err := driver.NodePublishVolume(context.Background(), first)
		firstResult <- err
	}()

	select {
	case <-slowMounter.mountStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first publish never reached the mount step")
	}

	secondResult := make(chan error, 1)
	go func() {
		_, err := driver.NodePublishVolume(context.Background(), second)
		secondResult <- err
	}()

	if err := <-firstResult; err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if err := <-secondResult; err != nil {
		t.Fatalf("second publish must proceed while another volume is mounting: %v", err)
	}
}

func TestNodePublishVolumeSerializesConcurrentSameVolumePublish(t *testing.T) {
	driver, _ := InstallControllerBackend(t, nil)
	slowMounter := &slowMountNodeMounter{
		Interface:    mount.NewFakeMounter(nil),
		mountStarted: make(chan struct{}),
		delay:        300 * time.Millisecond,
	}
	driver.mounter = slowMounter
	driver.mountPermissions = 0o750

	firstTarget := filepath.Join(t.TempDir(), "first")
	secondTarget := filepath.Join(t.TempDir(), "second")
	request := func(target string) *csi.NodePublishVolumeRequest {
		return &csi.NodePublishVolumeRequest{
			VolumeId:         "vol-1",
			TargetPath:       target,
			VolumeCapability: NFSMountCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER),
			VolumeContext: map[string]string{
				"share_id":   "vol-1",
				"share_name": "share",
				"pool_name":  "pool",
			},
			PublishContext: map[string]string{
				"server": "192.0.2.20",
				"path":   "/exports/pool/share",
			},
		}
	}

	firstResult := make(chan error, 1)
	go func() {
		_, err := driver.NodePublishVolume(context.Background(), request(firstTarget))
		firstResult <- err
	}()

	select {
	case <-slowMounter.mountStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first publish never reached the mount step")
	}

	secondResult := make(chan error, 1)
	go func() {
		_, err := driver.NodePublishVolume(context.Background(), request(secondTarget))
		secondResult <- err
	}()

	if err := <-firstResult; err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if err := <-secondResult; status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("concurrent same-volume publish code = %s, want %s: %v", status.Code(err), codes.FailedPrecondition, err)
	}
}

func TestNodeUnpublishVolumeWaitsForInFlightPublish(t *testing.T) {
	driver, _ := InstallControllerBackend(t, nil)
	fakeMounter := mount.NewFakeMounter(nil)
	slowMounter := &slowMountNodeMounter{
		Interface:    fakeMounter,
		mountStarted: make(chan struct{}),
		delay:        300 * time.Millisecond,
	}
	driver.mounter = slowMounter
	driver.mountPermissions = 0o750
	targetPath := filepath.Join(t.TempDir(), "mount")

	publishResult := make(chan error, 1)
	go func() {
		_, err := driver.NodePublishVolume(context.Background(), DynamicNodePublishRequest(targetPath))
		publishResult <- err
	}()

	select {
	case <-slowMounter.mountStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("publish never reached the mount step")
	}

	unpublishResult := make(chan error, 1)
	go func() {
		_, err := driver.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
			VolumeId:   "vol-1",
			TargetPath: targetPath,
		})
		unpublishResult <- err
	}()

	if err := <-publishResult; err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := <-unpublishResult; err != nil {
		t.Fatalf("unpublish must wait for the in-flight publish of the same volume: %v", err)
	}
	actions := fakeMounter.GetLog()
	if len(actions) != 2 ||
		actions[0].Action != mount.FakeActionMount ||
		actions[1].Action != mount.FakeActionUnmount {
		t.Fatalf("serialized publish/unpublish actions = %#v", actions)
	}
}

func TestNodePublishVolumeRecoversStaleTargetOnce(t *testing.T) {
	driver, _ := InstallControllerBackend(t, nil)
	targetPath := filepath.Join(t.TempDir(), "mount")
	if err := os.MkdirAll(targetPath, 0o750); err != nil {
		t.Fatal(err)
	}
	source := "192.0.2.20:/exports/pool/share"
	fakeMounter := mount.NewFakeMounter([]mount.MountPoint{{
		Device: source,
		Path:   targetPath,
		Type:   "nfs",
		Opts:   []string{"rw", "vers=3", "nolock"},
	}})
	fakeMounter.MountCheckErrors = map[string]error{
		targetPath: &os.PathError{Op: "stat", Path: targetPath, Err: syscall.ESTALE},
	}
	driver.mounter = fakeMounter

	if _, err := driver.NodePublishVolume(context.Background(), DynamicNodePublishRequest(targetPath)); err != nil {
		t.Fatal(err)
	}
	actions := fakeMounter.GetLog()
	if len(actions) != 2 ||
		actions[0].Action != mount.FakeActionUnmount ||
		actions[1].Action != mount.FakeActionMount {
		t.Fatalf("stale-target recovery actions = %#v", actions)
	}
}

func TestNodePublishVolumeStopsBeforeMountAfterContextCancellation(t *testing.T) {
	driver, _ := InstallControllerBackend(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	fakeMounter := mount.NewFakeMounter(nil)
	driver.mounter = &cancelNodeContextOnMountInspection{
		Interface: fakeMounter,
		cancel:    cancel,
	}
	targetPath := filepath.Join(t.TempDir(), "mount")

	_, err := driver.NodePublishVolume(ctx, DynamicNodePublishRequest(targetPath))
	if status.Code(err) != codes.Canceled {
		t.Fatalf("NodePublishVolume() code = %s, want %s: %v", status.Code(err), codes.Canceled, err)
	}
	if actions := fakeMounter.GetLog(); len(actions) != 0 {
		t.Fatalf("canceled publish caused mount actions: %#v", actions)
	}
}

func TestNodeUnpublishVolumeUsesMountTableAfterRestart(t *testing.T) {
	driver, backend := InstallControllerBackend(t, nil)
	targetPath := filepath.Join(t.TempDir(), "mount")
	if err := os.MkdirAll(targetPath, 0o750); err != nil {
		t.Fatal(err)
	}
	fakeMounter := mount.NewFakeMounter([]mount.MountPoint{{
		Device: "192.0.2.20:/exports/pool/share",
		Path:   targetPath,
		Type:   "nfs",
	}})
	driver.mounter = fakeMounter

	_, err := driver.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "vol-1",
		TargetPath: targetPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	actions := fakeMounter.GetLog()
	if len(actions) != 1 || actions[0].Action != mount.FakeActionUnmount {
		t.Fatalf("unmount actions = %#v", actions)
	}
	if len(backend.RequestsFor("GET", "/api/v2/status/services")) != 0 {
		t.Fatal("NodeUnpublishVolume contacted NGX")
	}
}

func TestNodeUnpublishVolumeRecoversStaleTarget(t *testing.T) {
	driver, _ := InstallControllerBackend(t, nil)
	targetPath := filepath.Join(t.TempDir(), "mount")
	if err := os.MkdirAll(targetPath, 0o750); err != nil {
		t.Fatal(err)
	}
	driver.pathStat = func(string) (os.FileInfo, error) {
		return nil, &os.PathError{Op: "stat", Path: targetPath, Err: syscall.ESTALE}
	}
	fakeMounter := mount.NewFakeMounter(nil)
	driver.mounter = fakeMounter

	if _, err := driver.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "vol-1",
		TargetPath: targetPath,
	}); err != nil {
		t.Fatalf("NodeUnpublishVolume() error = %v", err)
	}
	actions := fakeMounter.GetLog()
	if len(actions) != 1 || actions[0].Action != mount.FakeActionUnmount || actions[0].Target != targetPath {
		t.Fatalf("stale-target actions = %#v", actions)
	}
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		t.Fatalf("stale target still exists: %v", err)
	}
}

func TestNodeUnpublishVolumeStopsBeforeUnmountAfterContextCancellation(t *testing.T) {
	driver, _ := InstallControllerBackend(t, nil)
	targetPath := filepath.Join(t.TempDir(), "mount")
	if err := os.MkdirAll(targetPath, 0o750); err != nil {
		t.Fatal(err)
	}
	fakeMounter := mount.NewFakeMounter([]mount.MountPoint{{
		Device: "192.0.2.20:/exports/pool/share",
		Path:   targetPath,
		Type:   "nfs",
	}})
	ctx, cancel := context.WithCancel(context.Background())
	driver.mounter = &cancelNodeContextOnMountInspection{
		Interface: fakeMounter,
		cancel:    cancel,
	}

	_, err := driver.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "vol-1",
		TargetPath: targetPath,
	})
	if status.Code(err) != codes.Canceled {
		t.Fatalf("NodeUnpublishVolume() code = %s, want %s: %v", status.Code(err), codes.Canceled, err)
	}
	if actions := fakeMounter.GetLog(); len(actions) != 0 {
		t.Fatalf("canceled unpublish caused mount actions: %#v", actions)
	}
}

func TestNodeUnpublishVolumeIsIdempotentWithoutCache(t *testing.T) {
	for _, test := range []struct {
		name         string
		createTarget bool
	}{
		{name: "missing target"},
		{name: "existing unmounted target", createTarget: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver, _ := InstallControllerBackend(t, nil)
			fakeMounter := mount.NewFakeMounter(nil)
			driver.mounter = fakeMounter
			targetPath := filepath.Join(t.TempDir(), "mount")
			if test.createTarget {
				if err := os.MkdirAll(targetPath, 0o750); err != nil {
					t.Fatal(err)
				}
			}

			_, err := driver.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
				VolumeId:   "vol-1",
				TargetPath: targetPath,
			})
			if err != nil {
				t.Fatal(err)
			}
			if actions := fakeMounter.GetLog(); len(actions) != 0 {
				t.Fatalf("idempotent unpublish actions = %#v", actions)
			}
		})
	}
}

func TestNodeStageAndUnstageAreUnimplemented(t *testing.T) {
	driver := &Driver{}

	if _, err := driver.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: "/stage",
	}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("NodeStageVolume() code = %s, want %s", status.Code(err), codes.Unimplemented)
	}

	if _, err := driver.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: "/stage",
	}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("NodeUnstageVolume() code = %s, want %s", status.Code(err), codes.Unimplemented)
	}
}

func TestNodeGetVolumeStatsRequiresMountedPath(t *testing.T) {
	volumePath := t.TempDir()
	driver := &Driver{mounter: mount.NewFakeMounter([]mount.MountPoint{{
		Device: "192.0.2.20:/exports/pool/share",
		Path:   volumePath,
		Type:   "nfs",
	}})}

	response, err := driver.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
		VolumeId:   "vol-1",
		VolumePath: volumePath,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.GetUsage()) != 2 ||
		response.GetUsage()[0].GetUnit() != csi.VolumeUsage_BYTES ||
		response.GetUsage()[1].GetUnit() != csi.VolumeUsage_INODES {
		t.Fatalf("usage = %#v", response.GetUsage())
	}
	for _, usage := range response.GetUsage() {
		if usage.GetAvailable() < 0 || usage.GetUsed() < 0 || usage.GetTotal() < 0 {
			t.Fatalf("negative usage = %#v", usage)
		}
	}

	driver.mounter = mount.NewFakeMounter(nil)
	_, err = driver.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
		VolumeId:   "vol-1",
		VolumePath: volumePath,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("NodeGetVolumeStats() code = %s, want %s: %v", status.Code(err), codes.NotFound, err)
	}
}

func TestNodeGetVolumeStatsReturnsNotFoundForMissingVolumePath(t *testing.T) {
	volumePath := "some/path"
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "missing parent", err: &os.PathError{Op: "stat", Path: volumePath, Err: syscall.ENOENT}},
		{name: "parent is not a directory", err: &os.PathError{Op: "stat", Path: volumePath, Err: syscall.ENOTDIR}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fakeMounter := mount.NewFakeMounter(nil)
			fakeMounter.MountCheckErrors = map[string]error{volumePath: test.err}
			driver := &Driver{mounter: fakeMounter}

			_, err := driver.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
				VolumeId:   "vol-1",
				VolumePath: volumePath,
			})
			if status.Code(err) != codes.NotFound {
				t.Fatalf("NodeGetVolumeStats() code = %s, want %s: %v", status.Code(err), codes.NotFound, err)
			}
		})
	}
}

func TestNodeGetVolumeStatsKeepsInternalForOtherMountInspectionErrors(t *testing.T) {
	volumePath := "some/path"
	fakeMounter := mount.NewFakeMounter(nil)
	fakeMounter.MountCheckErrors = map[string]error{
		volumePath: &os.PathError{Op: "stat", Path: volumePath, Err: syscall.EACCES},
	}
	driver := &Driver{mounter: fakeMounter}

	_, err := driver.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
		VolumeId:   "vol-1",
		VolumePath: volumePath,
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("NodeGetVolumeStats() code = %s, want %s: %v", status.Code(err), codes.Internal, err)
	}
}

func TestNodeExpandVolumeIsUnimplemented(t *testing.T) {
	driver := &Driver{}
	_, err := driver.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:   "vol-1",
		VolumePath: "/var/lib/kubelet/pods/example",
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("NodeExpandVolume() code = %s, want %s: %v", status.Code(err), codes.Unimplemented, err)
	}
}

func TestNodeGetInfoOmitsInventedTopology(t *testing.T) {
	driver := &Driver{NodeId: "node-1"}
	response, err := driver.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetNodeId() != "node-1" {
		t.Fatalf("node ID = %q", response.GetNodeId())
	}
	if response.GetAccessibleTopology() != nil {
		t.Fatalf("unexpected topology = %#v", response.GetAccessibleTopology())
	}
}

func TestNodeGetCapabilitiesDoesNotAdvertiseStagingOrExpansion(t *testing.T) {
	driver := &Driver{}
	for _, capability := range supportedNodeCapabilities {
		driver.nsCap = append(driver.nsCap, &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{Type: capability},
			},
		})
	}
	response, err := driver.NodeGetCapabilities(context.Background(), &csi.NodeGetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.GetCapabilities()) != len(supportedNodeCapabilities) {
		t.Fatalf("node capabilities = %#v", response.GetCapabilities())
	}
	for _, capability := range response.GetCapabilities() {
		switch capability.GetRpc().GetType() {
		case csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME:
			t.Fatal("STAGE_UNSTAGE_VOLUME was advertised")
		case csi.NodeServiceCapability_RPC_EXPAND_VOLUME:
			t.Fatal("EXPAND_VOLUME was advertised")
		}
	}
}

func TestNodeGetVolumeHealthInvalid(t *testing.T) {
	driver, _ := InstallControllerBackend(t, nil)
	_, err := driver.NodeGetVolumeHealth(context.Background(), &csi.NodeGetVolumeHealthRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %s, want %s", status.Code(err), codes.InvalidArgument)
	}
}

func TestNodeGetStorageHealth(t *testing.T) {
	driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/status/cluster":  {{body: `{"status":"Ready","connected":1}`}},
		http.MethodGet + " /api/v2/status/services": {{body: `{"nfs":"running"}`}},
		http.MethodGet + " /api/v2/status/capacity": {{body: `[{"name":"pool","logical":{"available":"2147483648000","reserved":"0"},"physical":{"total":"2147483648000"}}]`}},
	})
	rsp, err := driver.NodeGetStorageHealth(context.Background(), &csi.NodeGetStorageHealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if rsp == nil {
		t.Fatal("response is nil")
	}
	if len(rsp.GetBackendHealth()) == 0 {
		t.Fatal("expected at least one backend health entry")
	}
}

func TestNodeGetVolumeHealthHealthyBackend(t *testing.T) {
	driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/status/cluster":  {{body: `{"status":"Ready","connected":1}`}},
		http.MethodGet + " /api/v2/status/services": {{body: `{"nfs":"running"}`}},
		http.MethodGet + " /api/v2/status/capacity": {{body: `[{"name":"pool","logical":{"available":"2147483648000","reserved":"0"},"physical":{"total":"2147483648000"}}]`}},
	})
	rsp, err := driver.NodeGetVolumeHealth(context.Background(), &csi.NodeGetVolumeHealthRequest{VolumeId: "vol-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rsp.GetVolumeHealth().GetHealthStatuses()) != 0 {
		t.Fatalf("expected zero health statuses, got %d", len(rsp.GetVolumeHealth().GetHealthStatuses()))
	}
}

func TestNodeGetStorageHealthClusterWorkMode(t *testing.T) {
	// In cluster work mode the appliance reports "Cluster" on both
	// controllers; the SDK treats that as a serviceable mode, so health
	// must not report the backend unreachable.
	driver, _ := InstallControllerBackend(t, map[string][]controllerBackendResponse{
		http.MethodGet + " /api/v2/status/cluster":  {{body: `{"status":"Cluster","connected":1}`}},
		http.MethodGet + " /api/v2/status/services": {{body: `{"nfs":"running"}`}},
		http.MethodGet + " /api/v2/status/capacity": {{body: `[{"name":"pool","logical":{"available":"2147483648000","reserved":"0"},"physical":{"total":"2147483648000"}}]`}},
	})
	rsp, err := driver.NodeGetStorageHealth(context.Background(), &csi.NodeGetStorageHealthRequest{})
	if err != nil {
		t.Fatalf("cluster work mode must be serviceable: %v", err)
	}
	entry := rsp.GetBackendHealth()[0]
	// Healthy = UNKNOWN error type with an empty reason (the CSI enum has
	// no explicit healthy value); StorageUnreachable would carry the
	// cluster-status reason string.
	if entry.GetStatus() != csi.StorageHealthErrorType_UNKNOWN_STORAGE_HEALTH_ERROR_TYPE || entry.GetReason() != "" {
		t.Fatalf("backend health = %s/%q, want UNKNOWN with empty reason", entry.GetStatus(), entry.GetReason())
	}
}

func TestNodeUnpublishVolumeRemovesTargetPath(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatal(err)
	}
	mounter := mount.NewFakeMounter(nil)
	d := &Driver{mounter: mounter}
	if _, err := d.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId: "nfs.csi.ngxstorage.com.0004", TargetPath: target,
	}); err != nil {
		t.Fatalf("NodeUnpublishVolume: %v", err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("target path %q was not removed (stat err=%v)", target, err)
	}
}

// ---------------------------------------------------------------------------
// Mount retry, transient classification, and static-PV preparation tests
// (merged from reconcile_test.go)
// ---------------------------------------------------------------------------

func TestTransientNFSMountError(t *testing.T) {
	for _, test := range []struct {
		message string
		want    bool
	}{
		{"mount.nfs: Connection timed out", true},
		{"mount.nfs: No route to host", true},
		{"mount.nfs: Connection refused", true},
		{"mount.nfs: access denied by server while mounting", false},
		{"target path has incompatible mount state", false},
	} {
		if got := nfs.TransientNFSMountError(fmt.Errorf("%s", test.message)); got != test.want {
			t.Fatalf("nfs.TransientNFSMountError(%q) = %v, want %v", test.message, got, test.want)
		}
	}
	if nfs.TransientNFSMountError(nil) {
		t.Fatal("nfs.TransientNFSMountError(nil) = true")
	}
}

func TestIsReadOnlyExportMountError(t *testing.T) {
	for _, test := range []struct {
		message string
		want    bool
	}{
		{"mount.nfs: /export is read-only file system", true},
		{"mount.nfs: access denied by server while mounting", true},
		{"mount.nfs: Permission denied", true},
		{"mount.nfs: Connection timed out", false},
		{"mount.nfs: Protocol error", false},
	} {
		if got := nfs.IsReadOnlyExportMountError(fmt.Errorf("%s", test.message)); got != test.want {
			t.Fatalf("nfs.IsReadOnlyExportMountError(%q) = %v, want %v", test.message, got, test.want)
		}
	}
	if nfs.IsReadOnlyExportMountError(nil) {
		t.Fatal("nfs.IsReadOnlyExportMountError(nil) = true")
	}
}

// installFakeMount records attempts and option sets, and runs behavior for
// each attempt so tests can fail deterministically without real mounts.
func installFakeMount(d *Driver, behavior func(attempt int, options []string) error) (attempts *int, optionsSeen *[][]string) {
	attempts = new(int)
	optionsSeen = new([][]string)
	d.mountFn = func(_ context.Context, _ string, _ string, options []string) error {
		*attempts++
		*optionsSeen = append(*optionsSeen, append([]string(nil), options...))
		return behavior(*attempts, options)
	}
	return attempts, optionsSeen
}

func TestMountWithRetryTransientThenSuccess(t *testing.T) {
	sleeps := installFakeSleeper(t, nil)
	d := &Driver{mountPermissions: 0o755}
	attempts, _ := installFakeMount(d, func(attempt int, _ []string) error {
		if attempt < 3 {
			return fmt.Errorf("mount.nfs: Connection timed out")
		}
		return nil
	})

	if err := d.mountService().MountWithRetry(context.Background(), "srv:/x", "/t", []string{"rw"}); err != nil {
		t.Fatalf("mountWithRetry: %v", err)
	}
	if *attempts != 3 {
		t.Fatalf("attempts = %d, want 3", *attempts)
	}
	if *sleeps != 2 {
		t.Fatalf("sleeps = %d, want 2", *sleeps)
	}
}

// A permanent failure must not be retried and must not sleep.
func TestMountWithRetryPermanentSingleAttempt(t *testing.T) {
	sleeps := installFakeSleeper(t, nil)
	d := &Driver{mountPermissions: 0o755}
	attempts, _ := installFakeMount(d, func(int, []string) error {
		return fmt.Errorf("mount.nfs: Protocol error")
	})

	if err := d.mountService().MountWithRetry(context.Background(), "srv:/x", "/t", []string{"rw"}); err == nil {
		t.Fatal("mountWithRetry succeeded on a permanent error")
	}
	if *attempts != 1 {
		t.Fatalf("attempts = %d, want 1", *attempts)
	}
	if *sleeps != 0 {
		t.Fatalf("sleeps = %d, want 0", *sleeps)
	}
}

// The final attempt returns its error immediately: exactly two sleeps for
// three attempts.
func TestMountWithRetryNoSleepAfterFinalAttempt(t *testing.T) {
	sleeps := installFakeSleeper(t, nil)
	d := &Driver{mountPermissions: 0o755}
	attempts, _ := installFakeMount(d, func(int, []string) error {
		return fmt.Errorf("mount.nfs: Connection timed out")
	})

	if err := d.mountService().MountWithRetry(context.Background(), "srv:/x", "/t", []string{"rw"}); err == nil {
		t.Fatal("mountWithRetry succeeded, want transient failure")
	}
	if *attempts != 3 {
		t.Fatalf("attempts = %d, want 3", *attempts)
	}
	if *sleeps != 2 {
		t.Fatalf("sleeps = %d, want 2 (no sleep after the final attempt)", *sleeps)
	}
}

func TestMountWithRetryContextCancellationDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sleeps := installFakeSleeper(t, func(ctx context.Context, _ int) error {
		cancel()
		return ctx.Err()
	})
	d := &Driver{mountPermissions: 0o755}
	attempts, _ := installFakeMount(d, func(int, []string) error {
		return fmt.Errorf("mount.nfs: Connection timed out")
	})

	err := d.mountService().MountWithRetry(ctx, "srv:/x", "/t", []string{"rw"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if *attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (cancelled before a second attempt)", *attempts)
	}
	if *sleeps != 1 {
		t.Fatalf("sleeps = %d, want 1", *sleeps)
	}
}

// A positively identified read-only/write-denied failure takes the read-only
// fallback once; a connectivity failure never does.
func TestMountStaticBaseExportForPreparationReadOnlyFallback(t *testing.T) {
	sleeps := installFakeSleeper(t, nil)
	d := &Driver{mountPermissions: 0o755}
	attempts, _ := installFakeMount(d, func(_ int, options []string) error {
		for _, option := range options {
			if option == "ro" {
				return nil
			}
		}
		return fmt.Errorf("mount.nfs: /export is read-only file system")
	})

	readOnly, err := d.mountService().MountStaticBaseExport(
		context.Background(), "srv:/x", "/t", []string{"rw"}, []string{"rw", "ro"},
	)
	if err != nil {
		t.Fatalf("mountStaticBaseExportForPreparation: %v", err)
	}
	if !readOnly {
		t.Fatal("readOnly = false, want true after the read-only fallback")
	}
	if *attempts != 2 {
		t.Fatalf("attempts = %d, want 2", *attempts)
	}
	if *sleeps != 0 {
		t.Fatalf("sleeps = %d, want 0", *sleeps)
	}
}

func TestMountStaticBaseExportForPreparationConnectivityNoFallback(t *testing.T) {
	sleeps := installFakeSleeper(t, nil)
	d := &Driver{mountPermissions: 0o755}
	attempts, optionsSeen := installFakeMount(d, func(int, []string) error {
		return fmt.Errorf("mount.nfs: Connection timed out")
	})

	readOnly, err := d.mountService().MountStaticBaseExport(
		context.Background(), "srv:/x", "/t", []string{"rw"}, []string{"rw", "ro"},
	)
	if err == nil {
		t.Fatal("mountStaticBaseExportForPreparation succeeded, want transient failure")
	}
	if readOnly {
		t.Fatal("readOnly = true, a connectivity failure must not fall back")
	}
	if *attempts != 3 {
		t.Fatalf("attempts = %d, want 3 (bounded retries, no fallback)", *attempts)
	}
	if *sleeps != 2 {
		t.Fatalf("sleeps = %d, want 2", *sleeps)
	}
	for _, options := range *optionsSeen {
		for _, option := range options {
			if option == "ro" {
				t.Fatalf("read-only fallback attempted with options %v", options)
			}
		}
	}
}
