package helper

import (
	"context"
	"strconv"
	"strings"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"
	"go.uber.org/zap"
)

// HealthyStatuses returns the CSI representation of a resolved healthy share.
func HealthyStatuses(share *ngxstorage.Share) []*csi.VolumeHealth_VolumeHealthEntry {
	if share == nil {
		return nil
	}
	return []*csi.VolumeHealth_VolumeHealthEntry{}
}

type HealthCondition string

const (
	StorageOK          HealthCondition = "StorageOk"
	StorageDegraded    HealthCondition = "StorageDegraded"
	StorageUnreachable HealthCondition = "StorageUnreachable"
)

func CheckService(ctx context.Context, client *ngxstorage.Client, logger *zap.Logger) error {
	services, err := client.Status().Services(ctx)
	if err != nil {
		logger.Error("NFS service status lookup failed", zap.Error(err))
		return err
	}
	statusValue, _ := services["nfs"].(string)
	if !strings.EqualFold(statusValue, "running") {
		logger.Warn("NFS service is not running", zap.String("status", statusValue))
		return &HealthError{Reason: "nfs service is not running (status: " + statusValue + ")"}
	}
	logger.Debug("NFS service is running", zap.String("status", statusValue))
	return nil
}

func BackendHealth(ctx context.Context, client *ngxstorage.Client, logger *zap.Logger) (HealthCondition, error) {
	cluster, err := client.Status().Cluster(ctx)
	if err != nil {
		logger.Error("NFS backend cluster status lookup failed", zap.Error(err))
		return StorageUnreachable, err
	}
	if cluster.Connected == 0 {
		return StorageUnreachable, healthError("controller not connected")
	}
	if cluster.Status != "Master" && cluster.Status != "Ready" && cluster.Status != "Cluster" {
		return StorageUnreachable, healthError("cluster status " + cluster.Status)
	}
	if err := CheckService(ctx, client, logger); err != nil {
		return StorageDegraded, err
	}
	capacity, err := client.Status().Capacity(ctx)
	if err != nil {
		logger.Error("NFS backend capacity lookup failed", zap.Error(err))
		return StorageUnreachable, err
	}
	for _, record := range capacity {
		name, _ := record["name"].(string)
		if name != client.PoolName() {
			continue
		}
		logical, _ := record["logical"].(map[string]interface{})
		available, _ := toInt64(logical["available"])
		physical, _ := record["physical"].(map[string]interface{})
		total, _ := toInt64(physical["total"])
		if total > 0 && float64(available) < 0.05*float64(total) {
			logger.Warn("NFS backend pool is nearly full", zap.String("pool_name", name), zap.Int64("available_bytes", available), zap.Int64("total_bytes", total))
			return StorageDegraded, healthError("pool nearly full")
		}
		break
	}
	return StorageOK, nil
}

func toInt64(value interface{}) (int64, bool) {
	switch typed := value.(type) {
	case float64:
		return int64(typed), true
	case string:
		number, err := strconv.ParseInt(typed, 10, 64)
		return number, err == nil
	default:
		return 0, false
	}
}

type HealthError struct{ Reason string }

func (e *HealthError) Error() string  { return "BackendHealth: " + e.Reason }
func healthError(reason string) error { return &HealthError{Reason: reason} }
