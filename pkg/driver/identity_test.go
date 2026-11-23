package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIdentityServiceReportsConfiguredDriver(t *testing.T) {
	driver := &Driver{Name: DriverName, Version: DriverVersion, Ready: true}

	info, err := driver.GetPluginInfo(context.Background(), &csi.GetPluginInfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if info.GetName() != DriverName || info.GetVendorVersion() != DriverVersion {
		t.Fatalf("plugin info = %#v", info)
	}

	capabilities, err := driver.GetPluginCapabilities(context.Background(), &csi.GetPluginCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(capabilities.GetCapabilities()) != 2 {
		t.Fatalf("plugin capabilities = %#v", capabilities.GetCapabilities())
	}
	seen := map[csi.PluginCapability_Service_Type]bool{}
	expansion := csi.PluginCapability_VolumeExpansion_UNKNOWN
	for _, capability := range capabilities.GetCapabilities() {
		if service := capability.GetService(); service != nil {
			seen[service.GetType()] = true
		}
		if volumeExpansion := capability.GetVolumeExpansion(); volumeExpansion != nil {
			expansion = volumeExpansion.GetType()
		}
	}
	if !seen[csi.PluginCapability_Service_CONTROLLER_SERVICE] ||
		seen[csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS] ||
		seen[csi.PluginCapability_Service_GROUP_CONTROLLER_SERVICE] {
		t.Fatalf("plugin capabilities = %#v", seen)
	}
	if expansion != csi.PluginCapability_VolumeExpansion_ONLINE {
		t.Fatalf("volume expansion capability = %s", expansion)
	}

	probe, err := driver.Probe(context.Background(), &csi.ProbeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !probe.GetReady().GetValue() {
		t.Fatal("Probe() did not return configured readiness")
	}
}

func TestIdentityServicePreservesCanceledContext(t *testing.T) {
	driver := &Driver{Name: DriverName, Version: DriverVersion, Ready: true}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for name, call := range map[string]func() error{
		"GetPluginInfo": func() error {
			_, err := driver.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
			return err
		},
		"GetPluginCapabilities": func() error {
			_, err := driver.GetPluginCapabilities(ctx, &csi.GetPluginCapabilitiesRequest{})
			return err
		},
		"Probe": func() error {
			_, err := driver.Probe(ctx, &csi.ProbeRequest{})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); status.Code(err) != codes.Canceled {
				t.Fatalf("%s() code = %s, want %s: %v", name, status.Code(err), codes.Canceled, err)
			}
		})
	}
}
