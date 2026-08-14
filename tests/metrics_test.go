package tests

import (
	"testing"

	"tests/helpers"

	"github.com/roadrunner-server/jobs/v6"
	"github.com/roadrunner-server/memory/v6"
	"github.com/roadrunner-server/metrics/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/require"
)

func TestJobsMetrics(t *testing.T) {
	helpers.Start(t, "configs/.rr-jobs-metrics.yaml", []any{
		&rpcPlugin.Plugin{},
		&server.Plugin{},
		&jobs.Plugin{},
		&metrics.Plugin{},
		&memory.Plugin{},
	}, helpers.WithRPCProbe(rpcAddr))

	client := helpers.NewJobsClient(t, rpcAddr)

	helpers.Declare(t, client, map[string]string{
		"driver":   "memory",
		"name":     "test-3",
		"prefetch": "10000",
	})
	helpers.Resume(t, client, "test-3")

	// nothing pushed yet: the counters exist and the single worker is exported
	exposition := helpers.RequireMetricsEventually(t, metricsURL,
		`rr_jobs_jobs_err 0`,
		`rr_jobs_jobs_ok 0`,
		`rr_jobs_jobs_requeue 0`,
		`rr_jobs_push_err 0`,
		`rr_jobs_push_ok 0`,
		`workers_memory_bytes`,
		`state="ready"}`,
		`{pid=`,
		`rr_jobs_total_workers 1`,
	)
	require.NotContains(t, exposition, `rr_jobs_requests_total`)
	require.NotContains(t, exposition, `rr_jobs_push_latency`)

	helpers.Push(t, client, "test-3", []byte("foo"))
	helpers.PushDelayed(t, client, "test-3", 5)
	helpers.Push(t, client, "test-3", []byte("foo"))

	exposition = helpers.RequireMetricsEventually(t, metricsURL,
		`rr_jobs_jobs_err 0`,
		`rr_jobs_jobs_ok 3`,
		`rr_jobs_jobs_requeue 0`,
		`rr_jobs_push_err 0`,
		`rr_jobs_push_ok 3`,
		`rr_jobs_requests_total{driver="memory",job="test-3",source="single"} 3`,
	)
	require.NotContains(t, exposition, `rr_jobs_requests_total{driver="memory",job="test-3",source="batch"}`)

	helpers.PushBatch(t, client, "test-3", 2, []byte("foo"))
	helpers.PushBatch(t, client, "test-3", 5, []byte("foo"))

	exposition = helpers.RequireMetricsEventually(t, metricsURL,
		`rr_jobs_jobs_err 0`,
		`rr_jobs_jobs_ok 10`,
		`rr_jobs_jobs_requeue 0`,
		`rr_jobs_push_err 0`,
		`rr_jobs_push_ok 10`,
		`rr_jobs_requests_total{driver="memory",job="test-3",source="single"} 3`,
		`rr_jobs_requests_total{driver="memory",job="test-3",source="batch"} 7`,
	)
	require.Contains(t, exposition, `rr_jobs_push_latency_bucket{driver="memory",job="test-3"`)

	helpers.DestroyPipelines(t, client, "test-3")
}
