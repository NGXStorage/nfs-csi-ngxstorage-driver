package helper

import (
	"testing"

	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"
)

func TestVolumeHashIndex(t *testing.T) {
	var index VolumeHashIndex
	share := ngxstorage.Share{ID: "share-1", Name: "pvc-hash"}
	index.Sync([]ngxstorage.Share{share, {}})
	if id, ok := index.IDByName(share.Name); !ok || id != share.ID {
		t.Fatalf("IDByName = %q, %v", id, ok)
	}
	index.Untrack(&share)
	if _, ok := index.IDByName(share.Name); ok {
		t.Fatal("untracked share remains")
	}
	index.Track(&share)
	if id, ok := index.IDByName(share.Name); !ok || id != share.ID {
		t.Fatal("tracked share missing")
	}
}

func TestSharePathHelpers(t *testing.T) {
	if ShareName("nfs.csi.ngxstorage.com.share") != "share" {
		t.Fatal("unexpected share name")
	}
	if ExtractPVName("/var/lib/kubelet/pods/p/volumes/csi/pvc/mount") != "pvc" {
		t.Fatal("unexpected PV name")
	}
}
