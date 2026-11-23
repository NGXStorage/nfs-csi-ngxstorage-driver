// Package nfs owns NFS-specific mount semantics used by the node service.
package nfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	mount "k8s.io/mount-utils"
)

// NFSMountState describes the effective state of one node target after
// inspecting the mount table and mount-point health.
type NFSMountState uint8

const (
	NFSMountAbsent NFSMountState = iota
	NFSMountCompatible
	NFSMountConflict
	NFSMountStale
)

// ErrIncompatibleNFSMount marks a race in which a target became occupied by
// incompatible NFS state between the caller's inspection and mount attempt.
var ErrIncompatibleNFSMount = errors.New("incompatible NFS mount")

// MountOptionsFromFlags normalizes CSI mount flags and adds the driver's
// default NFS version, readonly flag, and protocol-specific nolock option.
// It only constructs local mount arguments; it never changes NGX settings.
func MountOptionsFromFlags(flags []string, readOnly bool) ([]string, error) {
	options := make([]string, 0, len(flags)+3)
	seen := make(map[string]bool)
	version := ""
	for _, raw := range flags {
		for _, item := range strings.Split(raw, ",") {
			option := strings.TrimSpace(item)
			if option == "" || seen[option] {
				continue
			}
			if strings.HasPrefix(option, "nfsvers=") || strings.HasPrefix(option, "vers=") {
				currentVersion := strings.TrimPrefix(strings.TrimPrefix(option, "nfsvers="), "vers=")
				if version != "" && version != currentVersion {
					return nil, fmt.Errorf("conflicting NFS versions %s and %s", version, currentVersion)
				}
				version = currentVersion
				option = "nfsvers=" + currentVersion
			}
			if seen[option] {
				continue
			}
			seen[option] = true
			options = append(options, option)
		}
	}
	if seen["ro"] && seen["rw"] {
		return nil, errors.New("mount flags contain both ro and rw")
	}
	if readOnly && seen["rw"] {
		return nil, errors.New("readonly publish conflicts with rw mount flag")
	}
	if version == "" {
		version = "3"
		if !seen["nfsvers=3"] {
			options = append(options, "nfsvers=3")
			seen["nfsvers=3"] = true
		}
	}
	if readOnly && !seen["ro"] {
		options = append(options, "ro")
		seen["ro"] = true
	}
	if !readOnly && !seen["rw"] {
		options = append(options, "rw")
		seen["rw"] = true
	}
	switch {
	case strings.HasPrefix(version, "2"), strings.HasPrefix(version, "3"):
		if !seen["nolock"] {
			options = append(options, "nolock")
		}
	case strings.HasPrefix(version, "4"):
		// Explicit NFSv4 is allowed as a local mount option only.
	default:
		return nil, fmt.Errorf("nfsvers %s is not supported", version)
	}
	return options, nil
}

// InspectNFSMount compares one target with the requested canonical NFS source,
// read-only state, and observable mount options. Kernel-added options are
// ignored, but every requested effective option must be visible or the target
// fails safe as a conflict.
func InspectNFSMount(mounter mount.Interface, target, source string, requestedOptions []string) (NFSMountState, string, error) {
	notMountPoint, err := mounter.IsLikelyNotMountPoint(target)
	if err != nil {
		switch {
		case os.IsNotExist(err):
			return NFSMountAbsent, "", nil
		case mount.IsCorruptedMnt(err):
			return NFSMountStale, err.Error(), nil
		default:
			return NFSMountAbsent, "", fmt.Errorf("inspect mount target %s: %w", target, err)
		}
	}
	if notMountPoint {
		return NFSMountAbsent, "", nil
	}

	mountPoints, err := mounter.List()
	if err != nil {
		return NFSMountAbsent, "", fmt.Errorf("list mount table: %w", err)
	}
	cleanTarget := filepath.Clean(target)
	var existing *mount.MountPoint
	for index := range mountPoints {
		if filepath.Clean(mountPoints[index].Path) == cleanTarget {
			existing = &mountPoints[index]
		}
	}
	if existing == nil {
		return NFSMountAbsent, "", fmt.Errorf("target %s is mounted but absent from mount table", target)
	}
	if existing.Type != "nfs" && existing.Type != "nfs4" {
		return NFSMountConflict, fmt.Sprintf("filesystem type is %s", existing.Type), nil
	}
	if canonicalNFSSource(existing.Device) != canonicalNFSSource(source) {
		return NFSMountConflict, fmt.Sprintf("source is %s", existing.Device), nil
	}

	requested := effectiveMountOptions(requestedOptions)
	observed := effectiveMountOptions(existing.Opts)
	_, requestedReadOnly := requested["ro"]
	_, observedReadOnly := observed["ro"]
	_, observedReadWrite := observed["rw"]
	if observedReadOnly == observedReadWrite {
		return NFSMountConflict, "mount table does not expose one effective ro/rw state", nil
	}
	if requestedReadOnly != observedReadOnly {
		return NFSMountConflict, "read-only state differs", nil
	}

	for key, expected := range requested {
		switch key {
		case "ro", "rw", "_netdev", "auto", "noauto", "nofail", "bg", "fg", "retry":
			continue
		case "nolock":
			if _, present := observed["nolock"]; present || observed["local_lock"] == "none" {
				continue
			}
		}
		if actual, present := observed[key]; !present || actual != expected {
			return NFSMountConflict, fmt.Sprintf("mount option %s differs or is not observable", key), nil
		}
	}
	return NFSMountCompatible, "", nil
}

// Mount creates the target and performs an idempotent local NFS mount. Context
// cancellation is checked before every filesystem or mount-table mutation.
func Mount(ctx context.Context, mounter mount.Interface, source, target string, options []string, mode os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(target, mode); err != nil {
		return fmt.Errorf("create mount target %s: %w", target, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	state, reason, err := InspectNFSMount(mounter, target, source, options)
	if err != nil {
		return err
	}
	switch state {
	case NFSMountCompatible:
		return nil
	case NFSMountConflict:
		return fmt.Errorf("%w: %s", ErrIncompatibleNFSMount, reason)
	case NFSMountStale:
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := mounter.Unmount(target); err != nil {
			return fmt.Errorf("unmount stale target %s: %w", target, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := mounter.Mount(source, target, "nfs", options); err != nil {
		return fmt.Errorf("mount %s on %s: %w", source, target, err)
	}
	return nil
}

func canonicalNFSSource(source string) string {
	source = strings.TrimSpace(source)
	separator := strings.Index(source, ":/")
	if separator < 0 {
		return source
	}
	server := strings.ToLower(strings.TrimSpace(source[:separator]))
	exportPath := path.Clean(source[separator+1:])
	return server + ":" + exportPath
}

func effectiveMountOptions(options []string) map[string]string {
	result := make(map[string]string)
	for _, raw := range options {
		for _, item := range strings.Split(raw, ",") {
			option := strings.TrimSpace(item)
			if option == "" {
				continue
			}
			key, value, found := strings.Cut(option, "=")
			if !found {
				value = ""
			}
			if key == "nfsvers" {
				key = "vers"
			}
			result[key] = value
		}
	}
	return result
}

// Unmount removes a local NFS mount and has no backend export side effect.
func Unmount(mounter mount.Interface, target string) error {
	if err := mount.CleanupMountPoint(target, mounter, true); err != nil {
		return fmt.Errorf("unmount %s: %w", target, err)
	}
	return nil
}

// TransientNFSMountError reports whether a mount failure looks like a
// data-plane blip (server restart, network drop) rather than a permanent
// configuration error.
func TransientNFSMountError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, needle := range []string{
		"connection timed out",
		"no route to host",
		"connection refused",
		"server not responding",
		"timed out",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

// IsReadOnlyExportMountError reports whether a mount failure positively
// indicates a read-only export or a write denial, the only cases where the
// static preparation path may retry the mount read-only.
func IsReadOnlyExportMountError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, needle := range []string{
		"read-only file system",
		"readonly file system",
		"erofs",
		"access denied by server",
		"permission denied",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}
