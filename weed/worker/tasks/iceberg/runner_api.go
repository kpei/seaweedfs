package iceberg

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// MaintenanceHooks reports a RunMaintenance run's progress; any field may be nil.
type MaintenanceHooks struct {
	// OnResolved gets the settings and operations the run will use.
	OnResolved func(config Config, ops []string)
	OnOpStart  func(op string)
	OnOpDone   func(op, result string, metrics map[string]int64, err error)
	OnBinDone  func(binIdx, totalBins int)
}

// RunMaintenance runs config.Operations on one table outside the plugin
// scheduler, the way a maintenance job's Execute does: the table's maintenance
// configuration and Iceberg properties are layered over config, operations run
// in canonical order, a failed operation does not stop the later ones, and the
// outcome is recorded as the table's job status. It returns the last
// operation error.
func (h *Handler) RunMaintenance(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	config Config,
	hooks MaintenanceHooks,
) error {
	ops, err := parseOperations(config.Operations)
	if err != nil {
		return fmt.Errorf("invalid operations config: %w", err)
	}
	if err := validateWhereOperations(config.Where, ops); err != nil {
		return fmt.Errorf("invalid where config: %w", err)
	}

	maintenance, err := loadMaintenanceConfiguration(ctx, client, bucketName, tablePath)
	if err != nil {
		return fmt.Errorf("read maintenance configuration for %s/%s: %w", bucketName, tablePath, err)
	}
	ops = filterDisabledOperations(ops, maintenance)

	state, err := loadCurrentMetadata(ctx, client, bucketName, tablePath)
	if err != nil {
		return fmt.Errorf("read table properties for %s/%s: %w", bucketName, tablePath, err)
	}
	config = resolveTableConfig(config, state.Metadata.Properties(), maintenance)
	if hooks.OnResolved != nil {
		hooks.OnResolved(config, ops)
	}

	var lastErr error
	opErrors := make(map[string]error, len(ops))
	for _, op := range ops {
		if err := ctx.Err(); err != nil {
			return err
		}
		if hooks.OnOpStart != nil {
			hooks.OnOpStart(op)
		}

		var result string
		var metrics map[string]int64
		var opErr error
		switch op {
		case "compact":
			result, metrics, opErr = h.compactDataFiles(ctx, client, bucketName, tablePath, config, hooks.OnBinDone)
		case "rewrite_position_delete_files":
			result, metrics, opErr = h.rewritePositionDeleteFiles(ctx, client, bucketName, tablePath, config)
		case "expire_snapshots":
			result, metrics, opErr = h.expireSnapshots(ctx, client, bucketName, tablePath, config)
		case "remove_orphans":
			result, metrics, opErr = h.removeOrphans(ctx, client, bucketName, tablePath, config)
		case "rewrite_manifests":
			result, metrics, opErr = h.rewriteManifests(ctx, client, bucketName, tablePath, config)
		default:
			continue
		}

		opErrors[op] = opErr
		if opErr != nil {
			lastErr = opErr
		}
		if hooks.OnOpDone != nil {
			hooks.OnOpDone(op, result, metrics, opErr)
		}
	}

	recordJobStatus(ctx, client, bucketName, tablePath, buildJobStatus(opErrors, time.Now().UTC()))
	return lastErr
}

// DialFiler connects to a filer's gRPC address and checks it responds.
func (h *Handler) DialFiler(ctx context.Context, grpcAddress string) (filer_pb.SeaweedFilerClient, func() error, error) {
	conn, err := h.dialFiler(ctx, grpcAddress)
	if err != nil {
		return nil, nil, err
	}
	return filer_pb.NewSeaweedFilerClient(conn), conn.Close, nil
}
