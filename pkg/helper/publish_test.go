package helper

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.uber.org/zap"
	"k8s.io/mount-utils"
)

// kernelUnmountMounter models unmounting a NETWORK filesystem: the mounted
// content lives on the remote export, so after unmount the local mount point
// is empty (the fake's locally-created child directories stand in for remote
// content and vanish on unmount). CleanupMountPoint then removes the empty
// mount point, exactly as in production.
type kernelUnmountMounter struct {
	*mount.FakeMounter
}

func (m kernelUnmountMounter) Unmount(target string) error {
	if err := os.RemoveAll(target); err != nil {
		return err
	}
	if err := os.MkdirAll(target, 0o755); err != nil {
		return err
	}
	return m.FakeMounter.Unmount(target)
}

// TestPrepareStaticSubdirLeavesNoTemporaryResidue pins the success-path
// cleanup invariant: after a static recursive PV preparation no
// ngx-nfs-recursive-* temporary base-export directory remains under the
// plugin root. The success path unmounts via nfs.Unmount ->
// mount.CleanupMountPoint, which removes the (empty, post-unmount) mount
// point directory; this regression guards against a future change leaving
// empty temp directories behind on every static publish.
func TestPrepareStaticSubdirLeavesNoTemporaryResidue(t *testing.T) {
	root := t.TempDir()
	fakeMounter := kernelUnmountMounter{FakeMounter: mount.NewFakeMounter(nil)}
	svc := &PublishService{
		Mounter:            fakeMounter,
		Mount:              &MountService{Mounter: fakeMounter, Permissions: 0o755, Log: zap.NewNop()},
		TemporaryMountRoot: root,
		MountPermissions:   0o755,
		Log:                zap.NewNop(),
	}
	target := filepath.Join(root, "target")
	if err := svc.PrepareStaticSubdir(context.Background(), "vol-1", target, "srv", "/export", "pv-child", []string{"nfsvers=4.1"}); err != nil {
		t.Fatalf("PrepareStaticSubdir: %v", err)
	}
	tempRoot := StaticMountRoot(root, StaticTemporaryMountRoot)
	entries, err := os.ReadDir(tempRoot)
	if err != nil {
		t.Fatalf("read temporary root: %v", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "ngx-nfs-recursive-") {
			t.Fatalf("temporary base-export directory residue after successful preparation: %s", e.Name())
		}
	}
}
