package jobs

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/roadrunner-server/pool/v2/fsm"
	"github.com/roadrunner-server/pool/v2/state/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeInformer serves a fixed set of worker states to the exporter.
type fakeInformer struct{ states []*process.State }

func (f *fakeInformer) Workers() []*process.State { return f.states }

func TestStatsExporterDescribe(t *testing.T) {
	exporter := newStatsExporter(&fakeInformer{})

	descCh := make(chan *prometheus.Desc, 32)
	exporter.Describe(descCh)
	close(descCh)

	unique := make(map[*prometheus.Desc]struct{})
	for d := range descCh {
		unique[d] = struct{}{}
	}

	assert.Len(t, unique, 14)
}

// Without workers the exporter reports the aggregate gauges and the counters;
// the push histogram and the request counter have no series yet.
func TestStatsExporterCollectNoWorkers(t *testing.T) {
	exporter := newStatsExporter(&fakeInformer{})

	assert.Equal(t, 10, testutil.CollectAndCount(exporter))

	expected := `
# HELP rr_jobs_total_workers Total number of workers used by the plugin
# TYPE rr_jobs_total_workers gauge
rr_jobs_total_workers 0
# HELP rr_jobs_workers_memory_bytes Memory usage by workers.
# TYPE rr_jobs_workers_memory_bytes gauge
rr_jobs_workers_memory_bytes 0
# HELP rr_jobs_workers_ready Workers currently in ready state
# TYPE rr_jobs_workers_ready gauge
rr_jobs_workers_ready 0
# HELP rr_jobs_workers_working Workers currently in working state
# TYPE rr_jobs_workers_working gauge
rr_jobs_workers_working 0
# HELP rr_jobs_workers_invalid Workers currently in invalid,killing,destroyed,errored,inactive states
# TYPE rr_jobs_workers_invalid gauge
rr_jobs_workers_invalid 0
`

	require.NoError(t, testutil.CollectAndCompare(exporter, strings.NewReader(expected),
		"rr_jobs_total_workers", "rr_jobs_workers_memory_bytes",
		"rr_jobs_workers_ready", "rr_jobs_workers_working", "rr_jobs_workers_invalid"))
}

// Every worker contributes two per-worker metrics, and its state falls into one
// of the three aggregate buckets. Errored maps to the default (invalid) arm.
func TestStatsExporterCollectMixedStates(t *testing.T) {
	exporter := newStatsExporter(&fakeInformer{states: []*process.State{
		{Pid: 1, Status: fsm.StateReady, StatusStr: "ready", MemoryUsage: 100},
		{Pid: 2, Status: fsm.StateWorking, StatusStr: "working", MemoryUsage: 200},
		{Pid: 3, Status: fsm.StateErrored, StatusStr: "errored", MemoryUsage: 300},
	}})

	assert.Equal(t, 16, testutil.CollectAndCount(exporter))

	expected := `
# HELP rr_jobs_total_workers Total number of workers used by the plugin
# TYPE rr_jobs_total_workers gauge
rr_jobs_total_workers 3
# HELP rr_jobs_workers_memory_bytes Memory usage by workers.
# TYPE rr_jobs_workers_memory_bytes gauge
rr_jobs_workers_memory_bytes 600
# HELP rr_jobs_workers_ready Workers currently in ready state
# TYPE rr_jobs_workers_ready gauge
rr_jobs_workers_ready 1
# HELP rr_jobs_workers_working Workers currently in working state
# TYPE rr_jobs_workers_working gauge
rr_jobs_workers_working 1
# HELP rr_jobs_workers_invalid Workers currently in invalid,killing,destroyed,errored,inactive states
# TYPE rr_jobs_workers_invalid gauge
rr_jobs_workers_invalid 1
# HELP rr_jobs_worker_memory_bytes Worker current memory usage
# TYPE rr_jobs_worker_memory_bytes gauge
rr_jobs_worker_memory_bytes{pid="1"} 100
rr_jobs_worker_memory_bytes{pid="2"} 200
rr_jobs_worker_memory_bytes{pid="3"} 300
# HELP rr_jobs_worker_state Worker current state
# TYPE rr_jobs_worker_state gauge
rr_jobs_worker_state{pid="1",state="ready"} 0
rr_jobs_worker_state{pid="2",state="working"} 0
rr_jobs_worker_state{pid="3",state="errored"} 0
`

	require.NoError(t, testutil.CollectAndCompare(exporter, strings.NewReader(expected),
		"rr_jobs_total_workers", "rr_jobs_workers_memory_bytes",
		"rr_jobs_workers_ready", "rr_jobs_workers_working", "rr_jobs_workers_invalid",
		"rr_jobs_worker_memory_bytes", "rr_jobs_worker_state"))
}

// Each counter helper moves exactly one series.
func TestStatsExporterCounters(t *testing.T) {
	exporter := newStatsExporter(&fakeInformer{})

	exporter.CountJobOk()
	exporter.CountJobOk()
	exporter.CountJobErr()
	exporter.CountJobRequeue()
	exporter.CountPushOk()
	exporter.CountPushErr()
	exporter.CountPushErr()
	exporter.CountPushErr()

	expected := `
# HELP rr_jobs_jobs_ok Number of successfully processed jobs
# TYPE rr_jobs_jobs_ok counter
rr_jobs_jobs_ok 2
# HELP rr_jobs_jobs_err Number of jobs error while processing in the worker
# TYPE rr_jobs_jobs_err counter
rr_jobs_jobs_err 1
# HELP rr_jobs_jobs_requeue Number of re-queued jobs
# TYPE rr_jobs_jobs_requeue counter
rr_jobs_jobs_requeue 1
# HELP rr_jobs_push_ok Number of job push
# TYPE rr_jobs_push_ok counter
rr_jobs_push_ok 1
# HELP rr_jobs_push_err Number of jobs push which was failed
# TYPE rr_jobs_push_err counter
rr_jobs_push_err 3
`

	require.NoError(t, testutil.CollectAndCompare(exporter, strings.NewReader(expected),
		"rr_jobs_jobs_ok", "rr_jobs_jobs_err", "rr_jobs_jobs_requeue",
		"rr_jobs_push_ok", "rr_jobs_push_err"))
}

func TestPluginMetricsCollector(t *testing.T) {
	p := &Plugin{}
	p.metrics = newStatsExporter(p)

	collectors := p.MetricsCollector()

	require.Len(t, collectors, 1)
	assert.Same(t, p.metrics, collectors[0])
}
