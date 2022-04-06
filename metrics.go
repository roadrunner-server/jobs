package jobs

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/roadrunner-server/api/v2/plugins/informer"
	"github.com/roadrunner-server/sdk/v2/metrics"
)

func (p *Plugin) MetricsCollector() []prometheus.Collector {
	// p - implements Exporter interface (workers)
	return []prometheus.Collector{p.statsExporter}
}

const (
	namespace = "rr_jobs"
)

type statsExporter struct {
	jobsOk      *uint64
	pushOk      *uint64
	jobsErr     *uint64
	pushErr     *uint64
	pushOkDesc  *prometheus.Desc
	pushErrDesc *prometheus.Desc
	jobsErrDesc *prometheus.Desc
	jobsOkDesc  *prometheus.Desc

	defaultExporter *metrics.StatsExporter
}

func newStatsExporter(stats informer.Informer, jobsOk, pushOk, jobsErr, pushErr *uint64) *statsExporter {
	return &statsExporter{
		defaultExporter: &metrics.StatsExporter{
			TotalWorkersDesc: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "total_workers"), "Total number of workers used by the plugin", nil, nil),
			TotalMemoryDesc:  prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "workers_memory_bytes"), "Memory usage by workers.", nil, nil),
			StateDesc:        prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "worker_state"), "Worker current state", []string{"state", "pid"}, nil),
			WorkerMemoryDesc: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "worker_memory_bytes"), "Worker current memory usage", []string{"pid"}, nil),

			WorkersReady:   prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "workers_ready"), "Workers currently in ready state", nil, nil),
			WorkersWorking: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "workers_working"), "Workers currently in working state", nil, nil),
			WorkersInvalid: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "workers_invalid"), "Workers currently in invalid,killing,destroyed,errored,inactive states", nil, nil),

			Workers: stats,
		},

		jobsOk:  jobsOk,
		pushOk:  pushOk,
		jobsErr: jobsErr,
		pushErr: pushErr,

		pushOkDesc:  prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "push_ok"), "Number of job push", nil, nil),
		pushErrDesc: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "push_err"), "Number of jobs push which was failed", nil, nil),
		jobsErrDesc: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "jobs_err"), "Number of jobs error while processing in the worker", nil, nil),
		jobsOkDesc:  prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "jobs_ok"), "Number of successfully processed jobs", nil, nil),
	}
}

func (se *statsExporter) Describe(d chan<- *prometheus.Desc) {
	// send description
	se.defaultExporter.Describe(d)
	d <- se.pushErrDesc
	d <- se.pushOkDesc
	d <- se.jobsErrDesc
	d <- se.jobsOkDesc
}

func (se *statsExporter) Collect(ch chan<- prometheus.Metric) {
	// get the copy of the processes
	se.defaultExporter.Collect(ch)

	// send the values to the prometheus
	ch <- prometheus.MustNewConstMetric(se.jobsOkDesc, prometheus.GaugeValue, float64(atomic.LoadUint64(se.jobsOk)))
	ch <- prometheus.MustNewConstMetric(se.jobsErrDesc, prometheus.GaugeValue, float64(atomic.LoadUint64(se.jobsErr)))
	ch <- prometheus.MustNewConstMetric(se.pushOkDesc, prometheus.GaugeValue, float64(atomic.LoadUint64(se.pushOk)))
	ch <- prometheus.MustNewConstMetric(se.pushErrDesc, prometheus.GaugeValue, float64(atomic.LoadUint64(se.pushErr)))
}
