package jobs

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/roadrunner-server/sdk/v4/metrics"
)

const (
	namespace = "rr_jobs"
)

type statsExporter struct {
	jobsOk  *uint64
	pushOk  *uint64
	jobsErr *uint64
	pushErr *uint64

	pushOkDesc              *prometheus.Desc
	pushErrDesc             *prometheus.Desc
	jobsErrDesc             *prometheus.Desc
	jobsOkDesc              *prometheus.Desc
	pushJobLatencyHistogram *prometheus.HistogramVec
	pushJobRequestCounter   *prometheus.CounterVec

	defaultExporter *metrics.StatsExporter
}

func (p *Plugin) MetricsCollector() []prometheus.Collector {
	// p - implements Exporter interface (workers)
	return []prometheus.Collector{p.metrics}
}

func (se *statsExporter) CountJobOk() {
	atomic.AddUint64(se.jobsOk, 1)
}

func (se *statsExporter) CountJobErr() {
	atomic.AddUint64(se.jobsErr, 1)
}

func (se *statsExporter) CountPushOk() {
	atomic.AddUint64(se.pushOk, 1)
}

func (se *statsExporter) CountPushErr() {
	atomic.AddUint64(se.pushErr, 1)
}

func newStatsExporter(stats Informer) *statsExporter {
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

		jobsOk:  toPtr(uint64(0)),
		pushOk:  toPtr(uint64(0)),
		jobsErr: toPtr(uint64(0)),
		pushErr: toPtr(uint64(0)),

		pushOkDesc:  prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "push_ok"), "Number of job push", nil, nil),
		pushErrDesc: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "push_err"), "Number of jobs push which was failed", nil, nil),
		jobsErrDesc: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "jobs_err"), "Number of jobs error while processing in the worker", nil, nil),
		jobsOkDesc:  prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "jobs_ok"), "Number of successfully processed jobs", nil, nil),

		pushJobLatencyHistogram: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: prometheus.BuildFQName(namespace, "", "push_latency"),
			Help: "Histogram represents latency for pushed operation",
		}, []string{"job", "driver", "source"}),

		pushJobRequestCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "requests_total",
			Help:      "The total number of requests sent to the JOBS plugin",
		}, []string{"job", "driver", "source"}),
	}
}

func (se *statsExporter) Describe(d chan<- *prometheus.Desc) {
	// send description
	se.defaultExporter.Describe(d)
	d <- se.pushErrDesc
	d <- se.pushOkDesc
	d <- se.jobsErrDesc
	d <- se.jobsOkDesc

	se.pushJobLatencyHistogram.Describe(d)
	se.pushJobRequestCounter.Describe(d)
}

func (se *statsExporter) Collect(ch chan<- prometheus.Metric) {
	// get the copy of the processes
	se.defaultExporter.Collect(ch)

	// send the values to the prometheus
	ch <- prometheus.MustNewConstMetric(se.jobsOkDesc, prometheus.GaugeValue, float64(atomic.LoadUint64(se.jobsOk)))
	ch <- prometheus.MustNewConstMetric(se.jobsErrDesc, prometheus.GaugeValue, float64(atomic.LoadUint64(se.jobsErr)))
	ch <- prometheus.MustNewConstMetric(se.pushOkDesc, prometheus.GaugeValue, float64(atomic.LoadUint64(se.pushOk)))
	ch <- prometheus.MustNewConstMetric(se.pushErrDesc, prometheus.GaugeValue, float64(atomic.LoadUint64(se.pushErr)))

	se.pushJobLatencyHistogram.Collect(ch)
	se.pushJobRequestCounter.Collect(ch)
}

func toPtr[T any](v T) *T {
	return &v
}
