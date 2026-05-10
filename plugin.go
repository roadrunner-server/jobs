package jobs

import (
	"context"
	stderr "errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/roadrunner-server/api-go/v6/jobs/v2/jobsV2connect"
	jobsApi "github.com/roadrunner-server/api-plugins/v6/jobs"
	jprop "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/sync/semaphore"

	"github.com/roadrunner-server/endure/v2/dep"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/events"
	rh "github.com/roadrunner-server/jobs/v6/protocol"
	"github.com/roadrunner-server/pool/v2/payload"
	"github.com/roadrunner-server/pool/v2/state/process"
	pqImpl "github.com/roadrunner-server/priority_queue"
)

const (
	// RrMode env variable
	RrMode     string = "RR_MODE"
	RrModeJobs string = "jobs"

	PluginName string = "jobs"
	pipelines  string = "pipelines"

	// pipeline messages
	restartSrt string = "restart"
	stopStr    string = "stop"
)

type Plugin struct {
	mu sync.RWMutex

	// Jobs plugin configuration
	cfg         *Config `mapstructure:"jobs"`
	log         *slog.Logger
	workersPool Pool
	// workerPools holds multiple pools if configured
	// writes only in config phase, reads after that
	workersPools map[string]Pool
	server       Server
	eventBus     events.EventBus
	eventsCh     chan events.Event
	// bus id
	id           string
	experimental bool

	jobConstructors map[string]jobsApi.Constructor
	consumers       sync.Map // map[string]jobs.Consumer

	tracer *sdktrace.TracerProvider

	// priority queue implementation
	queue jobsApi.Queue
	minCh chan jobsApi.Job

	// parent config for broken options. keys are pipelines names, values - pointers to the associated pipeline
	pipelines sync.Map

	// initial set of the pipelines to consume
	consume map[string]struct{}

	// signal channel to stop the pollers
	stopCh chan struct{}

	// internal payloads pool
	pldPool       sync.Pool
	jobsProcessor *processor

	metrics     *statsExporter
	respHandler *rh.RespHandler

	// Pollers wait group
	pollersWg sync.WaitGroup
}

// Init configures the jobs plugin from the application config, sets up the priority queue,
// pipeline registry, and metrics collector. Returns errors.Disabled if the plugin section is not present.
func (p *Plugin) Init(cfg Configurer, log Logger, server Server) error {
	const op = errors.Op("jobs_plugin_init")
	if !cfg.Has(PluginName) {
		return errors.E(op, errors.Disabled)
	}

	err := cfg.UnmarshalKey(PluginName, &p.cfg)
	if err != nil {
		return errors.E(op, err)
	}

	err = p.cfg.InitDefaults()
	if err != nil {
		return errors.E(op, err)
	}

	p.server = server

	p.jobConstructors = make(map[string]jobsApi.Constructor)
	p.consume = make(map[string]struct{})
	p.stopCh = make(chan struct{}, 1)
	p.eventsCh = make(chan events.Event, 1)
	p.eventBus, p.id = events.NewEventBus()

	p.pldPool = sync.Pool{New: func() any {
		// with nil fields
		return &payload.Payload{}
	}}

	// initial set of pipelines
	for i := range p.cfg.Pipelines {
		p.pipelines.Store(i, p.cfg.Pipelines[i])
	}

	if len(p.cfg.Consume) > 0 {
		for i := range p.cfg.Consume {
			p.consume[p.cfg.Consume[i]] = struct{}{}
		}
	}

	// initialize pollers wg channel
	p.minCh = make(chan jobsApi.Job, 10)
	// initialize priority queue
	p.queue = pqImpl.NewBinHeap[jobsApi.Job](p.cfg.PipelineSize)
	p.log = log.NamedLogger(PluginName)
	p.jobsProcessor = newPipesProc(p.log, &p.consumers, p.consume, p.cfg.CfgOptions.Parallelism)
	p.experimental = cfg.Experimental()
	p.tracer = sdktrace.NewTracerProvider()

	// collector
	p.metrics = newStatsExporter(p)

	p.respHandler = rh.NewResponseHandler(p.log)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{}))

	return nil
}

// Serve initializes all configured pipelines via their driver constructors, creates worker pool(s),
// starts the queue pollers, and begins processing jobs. Returns an error channel for async errors.
func (p *Plugin) Serve() chan error {
	errCh := make(chan error, 1)
	const op = errors.Op("jobs_plugin_serve")

	// guard: if Serve() returns early, stop processor goroutines to prevent leaks
	started := false
	defer func() {
		if !started {
			p.jobsProcessor.stop()
		}
	}()

	if p.tracer == nil {
		// noop tracer
		p.tracer = sdktrace.NewTracerProvider()
	}

	err := p.eventBus.SubscribeP(p.id, fmt.Sprintf("*.%s", events.EventJOBSDriverCommand), p.eventsCh)
	if err != nil {
		errCh <- errors.E(op, err)
		return errCh
	}

	go p.readCommands(errCh)

	// register initial pipelines
	p.pipelines.Range(func(key, value any) bool {
		// pipeline associated with the name
		pipe := value.(jobsApi.Pipeline)
		pipeName := key.(string)
		// driver for the pipeline (ie amqp, ephemeral, etc)
		dr := pipe.Driver()

		if dr == "" {
			p.log.Error("can't find driver name for the pipeline, please, check that the 'driver' keyword for the pipelines specified correctly, JOBS plugin will try to run the next pipeline")
			return true
		}
		if _, ok := p.jobConstructors[dr]; !ok {
			p.log.Error("can't find driver constructor for the pipeline, please, check the global configuration for the specified driver",
				"driver", dr,
				"pipeline", pipeName)
			return true
		}

		// configuration key for the config
		configKey := fmt.Sprintf("%s.%s.%s.%s", PluginName, pipelines, pipeName, config)
		// save on how the pipeline was created
		pipe.With(createdWithConfig, configKey)

		p.jobsProcessor.add(&pjob{
			p.jobConstructors[dr],
			pipe,
			p.queue,
			configKey,
			p.cfg.Timeout,
			context.Background(),
		})

		return true
	})

	// block until all jobs are processed
	p.jobsProcessor.wait()
	// check for the errors
	if p.jobsProcessor.hasErrors() {
		errCh <- errors.E(op, stderr.Join(p.jobsProcessor.errors()...))
		return errCh
	}

	p.mu.Lock()
	if p.cfg.Pools != nil {
		p.workersPools = make(map[string]Pool, len(p.cfg.Pools))
		for poolName, poolCfg := range p.cfg.Pools {
			p.workersPools[poolName], err = p.server.NewPool(context.Background(), poolCfg, map[string]string{RrMode: RrModeJobs}, nil)
			if err != nil {
				p.mu.Unlock()
				errCh <- errors.E(op, err)
				return errCh
			}
		}
	} else {
		p.workersPool, err = p.server.NewPool(context.Background(), p.cfg.Pool, map[string]string{RrMode: RrModeJobs}, nil)
	}

	if err != nil {
		p.mu.Unlock()
		errCh <- errors.E(op, err)
		return errCh
	}

	// start listening
	started = true
	p.listener()
	p.mu.Unlock()
	return errCh
}

// Stop gracefully shuts down the plugin by signaling pollers to stop, waiting for in-flight jobs
// to complete, stopping all pipeline drivers, and destroying the worker pool(s).
func (p *Plugin) Stop(ctx context.Context) error {
	// Broadcast stop signal to all pollers
	close(p.stopCh)
	// drop subscriptions
	p.eventBus.Unsubscribe(p.id)
	close(p.eventsCh)

	defer func() {
		// workers' pool should be stopped
		p.mu.Lock()

		if p.workersPools != nil {
			for _, wp := range p.workersPools {
				if wp != nil {
					wp.Destroy(ctx)
				}
			}
		} else if p.workersPool != nil {
			p.workersPool.Destroy(ctx)
		}

		p.mu.Unlock()
	}()

	sema := semaphore.NewWeighted(int64(p.cfg.CfgOptions.Parallelism))
	// range over all consumers and call stop
	p.consumers.Range(func(key, value any) bool {
		// acquire semaphore, but if RR canceled the context, we should stop
		errA := sema.Acquire(ctx, 1)
		if errA != nil {
			return false
		}

		go func() {
			consumer := value.(jobsApi.Driver)
			ctxT, cancel := context.WithTimeout(ctx, p.cfg.TimeoutDuration())
			err := consumer.Stop(ctxT)
			if err != nil {
				p.log.Error("stop job driver", "driver", key, "error", err)
			}
			cancel()

			// release semaphore
			sema.Release(1)
		}()
		// process next
		return true
	})

	err := sema.Acquire(ctx, int64(p.cfg.CfgOptions.Parallelism))
	if err != nil {
		return err
	}

	// just to be sure
	if p.jobsProcessor != nil {
		p.jobsProcessor.stop()
	}

	p.waitPollersFinish(ctx)

	p.pipelines.Clear()
	p.consumers.Clear()

	return nil
}

// Collects declares the plugin's dependencies: job driver constructors and an optional tracer provider.
func (p *Plugin) Collects() []*dep.In {
	return []*dep.In{
		dep.Fits(func(pp any) {
			consumer := pp.(jobsApi.Constructor)
			p.jobConstructors[consumer.Name()] = consumer
		}, (*jobsApi.Constructor)(nil)),
		dep.Fits(func(pp any) {
			p.tracer = pp.(Tracer).Tracer()
		}, (*Tracer)(nil)),
	}
}

// Workers returns the state of all worker processes across all configured pools.
func (p *Plugin) Workers() []*process.State {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.workersPool == nil && p.workersPools == nil {
		p.log.Warn("pool and pools are nil, can't get workers")
		return nil
	}

	if len(p.workersPools) > 0 {
		var allStates []*process.State
		for _, wp := range p.workersPools {
			allStates = append(allStates, p.processState(wp.Workers())...)
		}

		return allStates
	}

	if p.workersPool != nil {
		wrk := p.workersPool.Workers()
		return p.processState(wrk)
	}

	return nil
}

// JobsState queries each registered pipeline driver for its current state (active, delayed, reserved job counts).
func (p *Plugin) JobsState(ctx context.Context) ([]*jobsApi.State, error) {
	const op = errors.Op("jobs_plugin_drivers_state")
	p.mu.RLock()
	defer p.mu.RUnlock()

	jst := make([]*jobsApi.State, 0, 2)
	var err error
	p.consumers.Range(func(_, value any) bool {
		consumer := value.(jobsApi.Driver)
		if consumer == nil {
			return true
		}
		newCtx, cancel := context.WithTimeout(ctx, p.cfg.TimeoutDuration())

		var state *jobsApi.State
		state, err = consumer.State(newCtx)
		if err != nil {
			jst = append(jst, &jobsApi.State{
				ErrorMessage: err.Error(),
			})
			cancel()
			return false
		}

		jst = append(jst, state)
		cancel()
		return true
	})

	if err != nil {
		return nil, errors.E(op, err)
	}
	return jst, nil
}

// Name returns the plugin identifier used for config lookup and dependency resolution.
func (p *Plugin) Name() string {
	return PluginName
}

// Reset restarts all worker pool(s) by destroying existing workers and spawning fresh ones.
func (p *Plugin) Reset() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	const op = errors.Op("jobs_plugin_reset")
	p.log.Info("reset signal was received")

	var errs []error
	switch {
	case len(p.workersPools) > 0:
		for poolName, wp := range p.workersPools {
			if err := wp.Reset(context.Background()); err != nil {
				errs = append(errs, errors.E(op, errors.Errorf("failed to reset pool %s: %s", poolName, err)))
			}
		}
	case p.workersPool != nil:
		if err := p.workersPool.Reset(context.Background()); err != nil {
			errs = append(errs, errors.E(op, err))
		}
	default:
		return errors.E(op, errors.Str("no worker pools configured"))
	}

	if len(errs) > 0 {
		return errors.E(op, stderr.Join(errs...))
	}

	p.log.Info("plugin was successfully reset")
	return nil
}

// Push sends a single job to the pipeline specified by the job's GroupID.
func (p *Plugin) Push(ctx context.Context, j jobsApi.Message) error {
	const op = errors.Op("jobs_plugin_push")

	start := time.Now().UTC()

	d, ppl, err := p.pipelineExists(j.GroupID())
	if err != nil {
		return errors.E(op, err)
	}

	p.metrics.pushJobRequestCounter.WithLabelValues(ppl.Name(), ppl.Driver(), "single").Inc()

	// if a job has no priority, inherit it from the pipeline
	if j.Priority() == 0 {
		j.UpdatePriority(ppl.Priority())
	}

	ctx, cancel := context.WithTimeout(ctx, p.cfg.TimeoutDuration())
	defer cancel()
	if val := ppl.Get(pool); val != nil {
		if valStr, ok := val.(string); ok && valStr != "" {
			j.Headers()[pool] = []string{valStr}
		}
	}

	err = d.Push(ctx, j)
	if err != nil {
		p.metrics.CountPushErr()
		p.log.Error("job push error", "ID", j.ID(), "pipeline", ppl.Name(), "driver", ppl.Driver(), "start", start, "elapsed", time.Since(start).Milliseconds(), "error", err)
		return errors.E(op, err)
	}

	p.metrics.CountPushOk()

	p.metrics.pushJobLatencyHistogram.WithLabelValues(ppl.Name(), ppl.Driver(), "single").Observe(time.Since(start).Seconds())

	p.log.Debug("job was pushed successfully", "ID", j.ID(), "pipeline", ppl.Name(), "driver", ppl.Driver(), "start", start, "elapsed", time.Since(start).Milliseconds())

	return nil
}

// PushBatch sends multiple jobs to their respective pipelines. Each job is pushed individually
// and the operation stops on the first error.
func (p *Plugin) PushBatch(ctx context.Context, j []jobsApi.Message) error {
	const op = errors.Op("jobs_plugin_push_batch")
	start := time.Now().UTC()

	for i := range j {
		operationStart := time.Now().UTC()

		d, ppl, err := p.pipelineExists(j[i].GroupID())
		if err != nil {
			return errors.E(op, err)
		}

		p.metrics.pushJobRequestCounter.WithLabelValues(ppl.Name(), ppl.Driver(), "batch").Inc()

		// if a job has no priority, inherit it from the pipeline
		if j[i].Priority() == 0 {
			j[i].UpdatePriority(ppl.Priority())
		}

		if val := ppl.Get(pool); val != nil {
			if valStr, ok := val.(string); ok && valStr != "" {
				j[i].Headers()[pool] = []string{valStr}
			}
		}

		ctxPush, cancel := context.WithTimeout(ctx, p.cfg.TimeoutDuration())
		err = d.Push(ctxPush, j[i])
		if err != nil {
			cancel()
			p.metrics.CountPushErr()
			p.log.Error("job push batch error", "ID", j[i].ID(), "pipeline", ppl.Name(), "driver", ppl.Driver(), "start", start, "elapsed", time.Since(start).Milliseconds(), "error", err)
			return errors.E(op, err)
		}

		p.metrics.CountPushOk()

		p.metrics.pushJobLatencyHistogram.WithLabelValues(ppl.Name(), ppl.Driver(), "batch").Observe(time.Since(operationStart).Seconds())

		cancel()
	}

	p.log.Debug("job batch was pushed successfully", "count", len(j), "start", start, "elapsed", time.Since(start).Milliseconds())

	return nil
}

// Pause suspends job consumption for the specified pipeline.
func (p *Plugin) Pause(ctx context.Context, pp string) error {
	d, ppl, err := p.pipelineExists(pp)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, p.cfg.TimeoutDuration())
	defer cancel()
	// redirect call to the underlying driver
	return d.Pause(ctx, ppl.Name())
}

// Resume restarts job consumption for a previously paused pipeline.
func (p *Plugin) Resume(ctx context.Context, pp string) error {
	d, ppl, err := p.pipelineExists(pp)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, p.cfg.TimeoutDuration())
	defer cancel()
	// redirect call to the underlying driver
	return d.Resume(ctx, ppl.Name())
}

// Declare dynamically registers a new pipeline at runtime, initializing its driver and starting consumption.
func (p *Plugin) Declare(ctx context.Context, pipeline jobsApi.Pipeline) error {
	const op = errors.Op("jobs_plugin_declare")
	// driver for the pipeline (ie amqp, ephemeral, etc)
	dr := pipeline.Driver()
	if dr == "" {
		return errors.E(op, errors.Errorf("no associated driver with the pipeline, pipeline name: %s", pipeline.Name()))
	}

	if _, ok := p.pipelines.Load(pipeline.Name()); ok {
		return errors.Errorf("pipeline already exists, name: %s, driver: %s", pipeline.Name(), pipeline.Driver())
	}

	// save priority as int64; fall back to defaultPriority on parse error.
	pr := pipeline.String(priority, strconv.FormatInt(defaultPriority, 10))
	prInt, err := strconv.ParseInt(pr, 10, 64)
	if err != nil {
		p.log.Error(priority, "error", err, "fallback", defaultPriority)
		prInt = defaultPriority
	}

	pipeline.With(priority, prInt)

	// jobConstructors contains constructors for the drivers
	// we need here to initialize these drivers for the pipelines
	if _, ok := p.jobConstructors[dr]; ok {
		// init the driver from a pipeline
		initializedDriver, err := p.jobConstructors[dr].DriverFromPipeline(ctx, pipeline, p.queue)
		if err != nil {
			return errors.E(op, err)
		}

		// if a pipeline initialized to be consumed, call Run on it,
		// but likely for the dynamic pipelines it should be started manually
		if _, ok := p.consume[pipeline.Name()]; ok {
			ctxDeclare, cancel := context.WithTimeout(ctx, p.cfg.TimeoutDuration())
			defer cancel()
			err = initializedDriver.Run(ctxDeclare, pipeline)
			if err != nil {
				return errors.E(op, err)
			}
		}

		// set how the pipeline was created
		pipeline.With(createdWithDeclare, trueStr)
		// add a driver to the set of the consumers (name - pipeline name, value - associated driver)
		p.consumers.Store(pipeline.Name(), initializedDriver)
		// save the pipeline
		p.pipelines.Store(pipeline.Name(), pipeline)
	}

	return nil
}

// Destroy stops the pipeline driver and removes it from the plugin's registry.
func (p *Plugin) Destroy(ctx context.Context, pp string) error {
	const op = errors.Op("jobs_plugin_destroy")
	pipe, ok := p.pipelines.Load(pp)
	if !ok {
		return errors.E(op, errors.Errorf("no such pipeline, requested: %s", pp))
	}

	if pipe == nil {
		return errors.Str("no pipe registered, value is nil")
	}
	// type conversion
	ppl := pipe.(jobsApi.Pipeline)

	// delete consumer
	d, ok := p.consumers.LoadAndDelete(ppl.Name())
	if !ok {
		return errors.E(op, errors.Errorf("consumer not registered for the requested driver: %s", ppl.Driver()))
	}

	// delete old pipeline
	p.pipelines.Delete(pp)

	ctx, cancel := context.WithTimeout(ctx, p.cfg.TimeoutDuration())
	err := d.(jobsApi.Driver).Stop(ctx)
	if err != nil {
		cancel()
		return errors.E(op, err)
	}

	cancel()
	return nil
}

// List returns the names of all registered pipelines.
func (p *Plugin) List() []string {
	out := make([]string, 0, 10)

	p.pipelines.Range(func(key, _ any) bool {
		// we can safely convert value here as we know that we store keys as strings
		out = append(out, key.(string))
		return true
	})

	return out
}

func (p *Plugin) RPC() (string, http.Handler) {
	return jobsV2connect.NewJobsServiceHandler(&rpc{p: p})
}
