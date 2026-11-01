// iceberg-compact runs the iceberg_maintenance worker's data compaction on one
// table, outside the admin scheduler, with bins merged in parallel.
//
// Export the settings saved in the admin UI so the run matches them:
//
//	curl -s -b 'admin-session=<cookie>' http://<admin>/api/plugin/job-types/iceberg_maintenance/config > config.json
//
// Then check what it would do, and run it:
//
//	iceberg-compact -filer=<filer>:8888 -config=config.json -bucket=b -namespace=ns -table=t -where="day = 20390" -plan
//	iceberg-compact -filer=<filer>:8888 -config=config.json -bucket=b -namespace=ns -table=t -where="day = 20390" -concurrency=8
//
// Stop the scheduled worker from compacting the same table while this runs:
// any other commit to the table makes the compaction fail as a stale plan.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/iceberg"
	"google.golang.org/protobuf/encoding/protojson"
)

// setFlags collects repeated -set key=value worker config overrides.
type setFlags map[string]string

func (s setFlags) String() string { return fmt.Sprint(map[string]string(s)) }

func (s setFlags) Set(v string) error {
	key, value, ok := strings.Cut(v, "=")
	if !ok || strings.TrimSpace(key) == "" {
		return fmt.Errorf("expected key=value, got %q", v)
	}
	s[strings.TrimSpace(key)] = value
	return nil
}

func main() {
	filer := flag.String("filer", "localhost:8888", "filer address, host:port (gRPC is port+10000) or host:port.grpcPort")
	bucket := flag.String("bucket", "", "table bucket name")
	namespace := flag.String("namespace", "", "table namespace")
	tableName := flag.String("table", "", "table name")
	where := flag.String("where", "", "partitions to compact, e.g. \"day = 20390\" or \"day IN (20390, 20391) AND region = 'eu'\"; overrides the config's where")
	configFile := flag.String("config", "", "JSON from GET /api/plugin/job-types/iceberg_maintenance/config; its worker settings are used as the base")
	concurrency := flag.Int("concurrency", 1, "bins merged at once; each holds its input files and output in memory")
	plan := flag.Bool("plan", false, "only list the bins (partition, files, size) that would be compacted, then exit")
	timeout := flag.Duration("timeout", 0, "give up after this long, 0 for no limit")
	securitySection := flag.String("securitySection", "grpc.client", "security.toml section for gRPC client TLS")
	overrides := setFlags{}
	flag.Var(overrides, "set", "override one worker setting, key=value, repeatable (e.g. -set target_file_size_mb=512)")
	_ = flag.Set("logtostderr", "true")
	// Progress logging from compaction; -v=0 silences it.
	_ = flag.Set("v", "1")
	flag.Parse()

	if *bucket == "" || *namespace == "" || *tableName == "" {
		fmt.Fprintln(os.Stderr, "-bucket, -namespace and -table are required")
		flag.Usage()
		os.Exit(2)
	}

	values, err := loadWorkerConfigValues(*configFile)
	if err != nil {
		fatalf("load config: %v", err)
	}
	known := knownWorkerConfigKeys()
	for key, value := range overrides {
		if _, ok := known[key]; !ok {
			fatalf("unknown worker setting %q, expected one of: %s", key, strings.Join(sortedKeys(known), ", "))
		}
		values[key] = &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_StringValue{StringValue: value}}
	}
	if *where != "" {
		values["where"] = &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_StringValue{StringValue: *where}}
	}
	config := iceberg.ParseConfig(values)
	config.CompactConcurrency = int64(*concurrency)
	config.CompactPlanOnly = *plan

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}

	util.LoadSecurityConfiguration()
	handler := iceberg.NewHandler(security.LoadClientTLS(util.GetViper(), *securitySection))
	client, closeConn, err := handler.DialFiler(ctx, pb.ServerAddress(*filer).ToGrpcAddress())
	if err != nil {
		fatalf("connect to filer %s: %v", *filer, err)
	}
	defer closeConn()

	fmt.Fprintf(os.Stderr, "connected to filer %s\n", *filer)

	tablePath := path.Join(*namespace, *tableName)
	start := time.Now()
	result, metrics, err := handler.CompactTable(ctx, client, *bucket, tablePath, config, printConfig, func(binIdx, totalBins int) {
		fmt.Fprintf(os.Stderr, "[%s] bin %d/%d done\n", time.Since(start).Round(time.Second), binIdx+1, totalBins)
	})
	if err != nil {
		fatalf("compact %s/%s: %v", *bucket, tablePath, err)
	}
	fmt.Println(result)
	for _, key := range sortedKeys(metrics) {
		fmt.Printf("  %s=%d\n", key, metrics[key])
	}
	fmt.Printf("took %s\n", time.Since(start).Round(time.Second))
}

// loadWorkerConfigValues reads the worker settings out of the admin's saved
// job type config, or starts empty so the worker defaults apply.
func loadWorkerConfigValues(file string) (map[string]*plugin_pb.ConfigValue, error) {
	if file == "" {
		return map[string]*plugin_pb.ConfigValue{}, nil
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	persisted := &plugin_pb.PersistedJobTypeConfig{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, persisted); err != nil {
		return nil, fmt.Errorf("parse %s: %w", file, err)
	}
	if persisted.JobType != "" && persisted.JobType != "iceberg_maintenance" {
		return nil, fmt.Errorf("%s is the config for %q, not iceberg_maintenance", file, persisted.JobType)
	}
	if persisted.WorkerConfigValues == nil {
		return map[string]*plugin_pb.ConfigValue{}, nil
	}
	return persisted.WorkerConfigValues, nil
}

func knownWorkerConfigKeys() map[string]struct{} {
	known := map[string]struct{}{"min_manifests_to_rewrite": {}}
	for key := range iceberg.NewHandler(nil).Descriptor().GetWorkerDefaultValues() {
		known[key] = struct{}{}
	}
	return known
}

func printConfig(c iceberg.Config) {
	mb := func(b int64) string { return fmt.Sprintf("%dMB", b/(1024*1024)) }
	fmt.Fprintf(os.Stderr, "settings after table properties (planning now): target_file=%s min_input_files=%d apply_deletes=%v strategy=%q where=%q row_group_bytes=%d row_group_rows=%d concurrency=%d\n",
		mb(c.TargetFileSizeBytes), c.MinInputFiles, c.ApplyDeletes, c.RewriteStrategy, c.Where,
		c.RowGroupSizeBytes, c.RowGroupRowLimit, c.CompactConcurrency)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
