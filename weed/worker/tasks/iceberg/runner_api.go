package iceberg

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// CompactTable compacts one table outside the plugin scheduler, resolving its
// settings the way a maintenance job does: the table's maintenance
// configuration and Iceberg properties are layered over config first. It
// passes the settings it will run with to onResolved before compacting.
func (h *Handler) CompactTable(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	config Config,
	onResolved func(Config),
	onProgress func(binIdx, totalBins int),
) (string, map[string]int64, error) {
	if err := validateWhereOperations(config.Where, []string{"compact"}); err != nil {
		return "", nil, err
	}

	maintenance, err := loadMaintenanceConfiguration(ctx, client, bucketName, tablePath)
	if err != nil {
		return "", nil, fmt.Errorf("read maintenance configuration for %s/%s: %w", bucketName, tablePath, err)
	}
	if len(filterDisabledOperations([]string{"compact"}, maintenance)) == 0 {
		return "compaction is disabled in this table's maintenance configuration", nil, nil
	}

	state, err := loadCurrentMetadata(ctx, client, bucketName, tablePath)
	if err != nil {
		return "", nil, fmt.Errorf("read table properties for %s/%s: %w", bucketName, tablePath, err)
	}
	config = resolveTableConfig(config, state.Metadata.Properties(), maintenance)
	if onResolved != nil {
		onResolved(config)
	}

	return h.compactDataFiles(ctx, client, bucketName, tablePath, config, onProgress)
}

// DialFiler connects to a filer's gRPC address and checks it responds.
func (h *Handler) DialFiler(ctx context.Context, grpcAddress string) (filer_pb.SeaweedFilerClient, func() error, error) {
	conn, err := h.dialFiler(ctx, grpcAddress)
	if err != nil {
		return nil, nil, err
	}
	return filer_pb.NewSeaweedFilerClient(conn), conn.Close, nil
}
