package nfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	mount "k8s.io/mount-utils"
)

type cancelOnMountInspection struct {
	mount.Interface
	cancel context.CancelFunc
}

func (m *cancelOnMountInspection) IsLikelyNotMountPoint(target string) (bool, error) {
	notMountPoint, err := m.Interface.IsLikelyNotMountPoint(target)
	m.cancel()
	return notMountPoint, err
}

func TestMountHelpers(t *testing.T) {
	defaults, err := MountOptionsFromFlags(nil, false)
	if err != nil || len(defaults) != 3 ||
		defaults[0] != "nfsvers=3" || defaults[1] != "rw" || defaults[2] != "nolock" {
		t.Fatalf("unexpected default mount options: %#v, %v", defaults, err)
	}
	flags, err := MountOptionsFromFlags([]string{"hard", "nfsvers=4.1"}, true)
	if err != nil || len(flags) != 3 || flags[2] != "ro" {
		t.Fatalf("unexpected explicit NFSv4 options: %#v, %v", flags, err)
	}
	if _, err := MountOptionsFromFlags([]string{"nfsvers=9"}, false); err == nil {
		t.Fatal("unsupported NFS version was accepted")
	}
	if _, err := MountOptionsFromFlags([]string{"vers=3", "nfsvers=4.1"}, false); err == nil {
		t.Fatal("conflicting NFS versions were accepted")
	}
	if _, err := MountOptionsFromFlags([]string{"ro", "rw"}, false); err == nil {
		t.Fatal("conflicting access options were accepted")
	}
	if _, err := MountOptionsFromFlags([]string{"rw"}, true); err == nil {
		t.Fatal("readonly publish with rw flag was accepted")
	}
	aliases, err := MountOptionsFromFlags([]string{"vers=3,nfsvers=3"}, false)
	if err != nil || len(aliases) != 3 {
		t.Fatalf("NFS version aliases were not deduplicated: %#v, %v", aliases, err)
	}
}

func TestInspectNFSMountCompatibility(t *testing.T) {
	source := "nfs.example.test:/exports/pool/share"

	for _, test := range []struct {
		name       string
		mountPoint *mount.MountPoint
		checkError error
		options    []string
		wantState  NFSMountState
	}{
		{
			name: "compatible canonical source and effective options",
			mountPoint: &mount.MountPoint{
				Device: "NFS.EXAMPLE.TEST:/exports/pool/../pool/share",
				Type:   "nfs",
				Opts:   []string{"rw", "vers=3", "nolock", "hard", "relatime"},
			},
			options:   []string{"rw", "nfsvers=3", "nolock", "hard"},
			wantState: NFSMountCompatible,
		},
		{
			name: "source mismatch",
			mountPoint: &mount.MountPoint{
				Device: "nfs.example.test:/exports/pool/other",
				Type:   "nfs",
				Opts:   []string{"rw", "vers=3", "nolock"},
			},
			options:   []string{"rw", "nfsvers=3", "nolock"},
			wantState: NFSMountConflict,
		},
		{
			name: "readonly mismatch",
			mountPoint: &mount.MountPoint{
				Device: source,
				Type:   "nfs",
				Opts:   []string{"rw", "vers=3", "nolock"},
			},
			options:   []string{"ro", "nfsvers=3", "nolock"},
			wantState: NFSMountConflict,
		},
		{
			name: "mount option mismatch",
			mountPoint: &mount.MountPoint{
				Device: source,
				Type:   "nfs",
				Opts:   []string{"rw", "vers=3", "nolock", "timeo=300"},
			},
			options:   []string{"rw", "nfsvers=3", "nolock", "timeo=600"},
			wantState: NFSMountConflict,
		},
		{
			name: "non NFS filesystem",
			mountPoint: &mount.MountPoint{
				Device: source,
				Type:   "ext4",
				Opts:   []string{"rw"},
			},
			options:   []string{"rw", "nfsvers=3", "nolock"},
			wantState: NFSMountConflict,
		},
		{
			name:      "absent target",
			options:   []string{"rw", "nfsvers=3", "nolock"},
			wantState: NFSMountAbsent,
		},
		{
			name: "stale target",
			mountPoint: &mount.MountPoint{
				Device: source,
				Type:   "nfs",
				Opts:   []string{"rw", "vers=3", "nolock"},
			},
			checkError: &os.PathError{Op: "stat", Path: "target", Err: syscall.ESTALE},
			options:    []string{"rw", "nfsvers=3", "nolock"},
			wantState:  NFSMountStale,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			target := filepath.Join(t.TempDir(), "mount")
			var mountPoints []mount.MountPoint
			if test.mountPoint != nil {
				if err := os.MkdirAll(target, 0o750); err != nil {
					t.Fatal(err)
				}
				mountPoint := *test.mountPoint
				mountPoint.Path = target
				mountPoints = append(mountPoints, mountPoint)
			}
			fakeMounter := mount.NewFakeMounter(mountPoints)
			if test.checkError != nil {
				fakeMounter.MountCheckErrors = map[string]error{target: test.checkError}
			}

			state, _, err := InspectNFSMount(fakeMounter, target, source, test.options)
			if err != nil {
				t.Fatal(err)
			}
			if state != test.wantState {
				t.Fatalf("InspectNFSMount() state = %d, want %d", state, test.wantState)
			}
		})
	}
}

func TestMountAndUnmountLifecycle(t *testing.T) {
	fakeMounter := mount.NewFakeMounter(nil)
	target := filepath.Join(t.TempDir(), "mount")
	source := "192.0.2.20:/exports/pool/share"

	options := []string{"nfsvers=3", "rw", "nolock"}
	if err := Mount(context.Background(), fakeMounter, source, target, options, 0o750); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(target); err != nil || info.Mode().Perm() != 0o750 {
		t.Fatalf("mount target mode = %v, %v", info, err)
	}
	actions := fakeMounter.GetLog()
	if len(actions) != 1 || actions[0].Action != mount.FakeActionMount ||
		actions[0].Source != source || actions[0].Target != target {
		t.Fatalf("mount actions = %#v", actions)
	}

	// A repeated mount on the same target is handled idempotently by the mount
	// utility and must not call the mounter again.
	if err := Mount(context.Background(), fakeMounter, source, target, options, 0o750); err != nil {
		t.Fatal(err)
	}
	if actions := fakeMounter.GetLog(); len(actions) != 1 {
		t.Fatalf("idempotent mount actions = %#v", actions)
	}

	if err := Unmount(fakeMounter, target); err != nil {
		t.Fatal(err)
	}
	actions = fakeMounter.GetLog()
	if len(actions) != 2 || actions[1].Action != mount.FakeActionUnmount {
		t.Fatalf("unmount actions = %#v", actions)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("mount target still exists after cleanup: %v", err)
	}
}

func TestMountStopsAfterContextCancellation(t *testing.T) {
	t.Run("before target creation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		target := filepath.Join(t.TempDir(), "mount")
		fakeMounter := mount.NewFakeMounter(nil)

		err := Mount(ctx, fakeMounter, "192.0.2.20:/exports/pool/share", target, []string{"rw", "nfsvers=3", "nolock"}, 0o750)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Mount() error = %v, want context canceled", err)
		}
		if _, statErr := os.Stat(target); !os.IsNotExist(statErr) {
			t.Fatalf("target exists after pre-canceled mount: %v", statErr)
		}
	})

	t.Run("after mount inspection", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		fakeMounter := mount.NewFakeMounter(nil)
		mounter := &cancelOnMountInspection{Interface: fakeMounter, cancel: cancel}
		target := filepath.Join(t.TempDir(), "mount")

		err := Mount(ctx, mounter, "192.0.2.20:/exports/pool/share", target, []string{"rw", "nfsvers=3", "nolock"}, 0o750)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Mount() error = %v, want context canceled", err)
		}
		if actions := fakeMounter.GetLog(); len(actions) != 0 {
			t.Fatalf("canceled mount caused mount actions: %#v", actions)
		}
	})
}

func TestMountRecoversStaleTargetOnce(t *testing.T) {
	target := filepath.Join(t.TempDir(), "mount")
	source := "192.0.2.20:/exports/pool/share"
	fakeMounter := mount.NewFakeMounter([]mount.MountPoint{{
		Device: source,
		Path:   target,
		Type:   "nfs",
		Opts:   []string{"rw", "vers=3", "nolock"},
	}})
	fakeMounter.MountCheckErrors = map[string]error{
		target: &os.PathError{Op: "stat", Path: target, Err: syscall.ESTALE},
	}

	if err := Mount(context.Background(), fakeMounter, source, target, []string{"rw", "nfsvers=3", "nolock"}, 0o750); err != nil {
		t.Fatal(err)
	}
	actions := fakeMounter.GetLog()
	if len(actions) != 2 ||
		actions[0].Action != mount.FakeActionUnmount ||
		actions[1].Action != mount.FakeActionMount {
		t.Fatalf("stale recovery actions = %#v", actions)
	}
}
