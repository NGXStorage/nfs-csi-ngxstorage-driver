package helper

import (
	"path/filepath"
	"strings"
	"sync"

	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"
)

// ShareName extracts the canonical share name from a CSI volume ID.
func ShareName(volumeID string) string {
	return strings.TrimPrefix(volumeID, "nfs.csi.ngxstorage.com.")
}

// ExtractPVName extracts the PV directory immediately above the mount suffix.
func ExtractPVName(targetPath string) string {
	clean := filepath.Clean(targetPath)
	if clean == "." || clean == string(filepath.Separator) {
		return ""
	}
	return filepath.Base(filepath.Dir(clean))
}

// VolumeHashIndex caches canonical share IDs by hashed CSI name. Backend lists
// remain authoritative; this index only accelerates idempotency resolution.
type VolumeHashIndex struct {
	mu     sync.RWMutex
	byID   map[string]*ngxstorage.Share
	byName map[string]string
}

func (i *VolumeHashIndex) Sync(shares []ngxstorage.Share) {
	byID := make(map[string]*ngxstorage.Share, len(shares))
	byName := make(map[string]string, len(shares))
	for offset := range shares {
		share := shares[offset]
		if share.ID == "" || share.Name == "" {
			continue
		}
		byID[share.ID] = &share
		byName[share.Name] = share.ID
	}
	i.mu.Lock()
	i.byID, i.byName = byID, byName
	i.mu.Unlock()
}

func (i *VolumeHashIndex) Track(share *ngxstorage.Share) {
	if share == nil || share.ID == "" || share.Name == "" {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.byID == nil {
		i.byID = make(map[string]*ngxstorage.Share)
	}
	if i.byName == nil {
		i.byName = make(map[string]string)
	}
	i.byID[share.ID], i.byName[share.Name] = share, share.ID
}

func (i *VolumeHashIndex) Untrack(share *ngxstorage.Share) {
	if share == nil || share.ID == "" {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if record := i.byID[share.ID]; record != nil {
		delete(i.byName, record.Name)
	}
	delete(i.byID, share.ID)
	if share.Name != "" {
		delete(i.byName, share.Name)
	}
}

func (i *VolumeHashIndex) IDByName(name string) (string, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	id, ok := i.byName[name]
	return id, ok
}
