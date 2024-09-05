package jobs

import (
	"context"
	stderr "errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	jobsApi "github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	jprop "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/semaphore"

	"github.com/roadrunner-server/endure/v2/dep"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/events"
	"github.com/roadrunner-server/goridge/v3/pkg/frame"
	rh "github.com/roadrunner-server/jobs/v5/protocol"
	"github.com/roadrunner-server/pool/payload"
	"github.com/roadrunner-server/pool/state/process"
	pqImpl "github.com/roadrunner-server/priority_queue"
	"go.uber.org/zap"
)

const (
	// RrMode env variable
	RrMode     string = "RR_MODE"
	RrModeJobs string = "jobs"

	PluginName string = "jobs"
	pipelines  string = "pipelines"

	// v2.7 and newer config key
	cfgKey string = "config"

	// v2023.1.0 OTEL
	spanName string = "jobs"
	// pipeline messages
	restartSrt string = "restart"
	stopStr    string = "stop"
)

type Plugin struct {
	mu sync.RWMutex

	// Jobs plugin configuration
	cfg         *Config `mapstructure:"jobs"`
	log         *zap.Logger
	workersPool Pool
	server      Server
	eventBus    events.EventBus
	eventsCh    chan events.Event
	// bus id
	id           string
	experimental bool

	jobConstructors map[string]jobsApi.Constructor
	consumers       sync.Map // map[string]jobs.Consumer

	tracer *sdktrace.TracerProvider

	// priority queue implementation
	queue jobsApi.Queue

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
}

func (p *Plugin) Init(cfg Configurer, log Logger, server Server) error {
	const op = errors.Op("jobs_plugin_init")
	if !cfg.Has(PluginName) {
		return errors.E(op, errors.Disabled)
	}

	err := cfg.UnmarshalKey(PluginName, &p.cfg)
	if err != nil {
		return errors.E(op, err)
	}

	p.cfg.InitDefaults()

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
		for i := 0; i < len(p.cfg.Consume); i++ {
			p.consume[p.cfg.Consume[i]] = struct{}{}
		}
	}

	// initialize priority queue
	p.queue = pqImpl.NewBinHeap[jobsApi.Job](p.cfg.PipelineSize)
	p.log = new(zap.Logger)
	p.log = log.NamedLogger(PluginName)
	p.jobsProcessor = newPipesProc(p.log, &p.consumers, &p.consume, p.cfg.CfgOptions.Parallelism)
	p.experimental = cfg.Experimental()

	// collector
	p.metrics = newStatsExporter(p)

	p.respHandler = rh.NewResponseHandler(p.log)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{}))

	return nil
}

func (p *Plugin) Serve() chan error {
	errCh := make(chan error, 1)
	const op = errors.Op("jobs_plugin_serve")

	if p.tracer == nil {
		// noop tracer
		p.tracer = sdktrace.NewTracerProvider()
	}

	err := p.eventBus.SubscribeP(p.id, fmt.Sprintf("*.%s", events.EventJOBSDriverCommand.String()), p.eventsCh)
	if err != nil {
		errCh <- errors.E(op, err)
		return errCh
	}

	go p.readCommands()

	// register initial pipelines
	p.pipelines.Range(func(key, value any) bool {
		// pipeline associated with the name
		pipe := value.(jobsApi.Pipeline)
		pipeName := key.(string)
		// driver for the pipeline (ie amqp, ephemeral, etc)
		dr := pipe.Driver()

		if dr == "" {
			p.log.Error("can't find driver name for the pipeline, please, pipelineExists that the 'driver' keyword for the pipelines specified correctly, JOBS plugin will try to run the next pipeline")
			return true
		}
		if _, ok := p.jobConstructors[dr]; !ok {
			p.log.Error("can't find driver constructor for the pipeline, please, pipelineExists the global configuration for the specified driver",
				zap.String("driver", dr),
				zap.String("pipeline", pipeName))
			return true
		}

		// configuration key for the config
		configKey := fmt.Sprintf("%s.%s.%s.%s", PluginName, pipelines, pipeName, cfgKey)
		// save on how the pipeline was created
		pipe.With(createdWithConfig, configKey)

		p.jobsProcessor.add(&pjob{
			p.jobConstructors[dr],
			pipe,
			p.queue,
			configKey,
			p.cfg.Timeout,
		})

		return true
	})

	// block until all jobs are processed
	p.jobsProcessor.wait()
	// pipelineExists for the errors
	if p.jobsProcessor.hasErrors() {
		// TODO(rustatian): pretty print errors
		errCh <- errors.E(op, stderr.Join(p.jobsProcessor.errors()...))
		return errCh
	}

	p.mu.Lock()

	p.workersPool, err = p.server.NewPool(context.Background(), p.cfg.Pool, map[string]string{RrMode: RrModeJobs}, nil)
	if err != nil {
		p.mu.Unlock()
		errCh <- errors.E(op, err)
		return errCh
	}

	// start listening
	p.listener()
	p.mu.Unlock()
	return errCh
}

func (p *Plugin) Stop(ctx context.Context) error {
	// Broadcast stop signal to all pollers
	close(p.stopCh)
	// drop subscriptions
	p.eventBus.Unsubscribe(p.id)
	close(p.eventsCh)

	defer func() {
		// workers' pool should be stopped
		p.mu.Lock()
		if p.workersPool != nil {
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
			ctxT, cancel := context.WithTimeout(ctx, time.Second*time.Duration(p.cfg.Timeout))
			err := consumer.Stop(ctxT)
			if err != nil {
				p.log.Error("stop job driver", zap.Any("driver", key), zap.Error(err))
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

	p.pipelines.Range(func(key, _ any) bool {
		p.pipelines.Delete(key)
		return true
	})

	p.consumers.Range(func(key, _ any) bool {
		p.consumers.Delete(key)
		return true
	})

	// just to be sure
	if p.jobsProcessor != nil {
		p.jobsProcessor.stop()
	}

	return nil
}

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

func (p *Plugin) Workers() []*process.State {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.workersPool == nil {
		return nil
	}

	wrk := p.workersPool.Workers()

	ps := make([]*process.State, len(wrk))

	for i := 0; i < len(wrk); i++ {
		if wrk[i] == nil {
			continue
		}
		st, err := process.WorkerProcessState(wrk[i])
		if err != nil {
			p.log.Error("jobs workers state", zap.Error(err))
			return nil
		}

		ps[i] = st
	}

	return ps
}

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
		newCtx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(p.cfg.Timeout))

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

func (p *Plugin) Name() string {
	return PluginName
}

func (p *Plugin) Reset() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	const op = errors.Op("jobs_plugin_reset")
	p.log.Info("reset signal was received")
	err := p.workersPool.Reset(context.Background())
	if err != nil {
		return errors.E(op, err)
	}
	p.log.Info("plugin was successfully reset")

	return nil
}

func (p *Plugin) Push(ctx context.Context, j jobsApi.Message) error {
	const op = errors.Op("jobs_plugin_push")

	start := time.Now().UTC()
	// get the pipeline for the job
	pipe, ok := p.pipelines.Load(j.GroupID())
	if !ok {
		return errors.E(op, errors.Errorf("no such pipeline, requested: %s", j.GroupID()))
	}

	// type conversion
	ppl := pipe.(jobsApi.Pipeline)

	d, ok := p.consumers.Load(ppl.Name())
	if !ok {
		return errors.E(op, errors.Errorf("consumer not registered for the requested driver: %s", ppl.Driver()))
	}

	p.metrics.pushJobRequestCounter.WithLabelValues(ppl.Name(), ppl.Driver(), "single").Inc()

	// if a job has no priority, inherit it from the pipeline
	if j.Priority() == 0 {
		j.UpdatePriority(ppl.Priority())
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(p.cfg.Timeout))
	defer cancel()

	err := d.(jobsApi.Driver).Push(ctx, j)
	if err != nil {
		p.metrics.CountPushErr()
		p.log.Error("job push error", zap.String("ID", j.ID()), zap.String("pipeline", ppl.Name()), zap.String("driver", ppl.Driver()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()), zap.Error(err))
		return errors.E(op, err)
	}

	p.metrics.CountPushOk()

	p.metrics.pushJobLatencyHistogram.WithLabelValues(ppl.Name(), ppl.Driver(), "single").Observe(time.Since(start).Seconds())

	p.log.Debug("job was pushed successfully", zap.String("ID", j.ID()), zap.String("pipeline", ppl.Name()), zap.String("driver", ppl.Driver()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))

	return nil
}

func (p *Plugin) PushBatch(ctx context.Context, j []jobsApi.Message) error {
	const op = errors.Op("jobs_plugin_push_batch")
	start := time.Now().UTC()

	for i := 0; i < len(j); i++ {
		operationStart := time.Now().UTC()
		// get the pipeline for the job
		pipe, ok := p.pipelines.Load(j[i].GroupID())
		if !ok {
			return errors.E(op, errors.Errorf("no such pipeline, requested: %s", j[i].GroupID()))
		}

		ppl := pipe.(jobsApi.Pipeline)

		d, ok := p.consumers.Load(ppl.Name())
		if !ok {
			return errors.E(op, errors.Errorf("consumer not registered for the requested driver: %s", ppl.Driver()))
		}

		p.metrics.pushJobRequestCounter.WithLabelValues(ppl.Name(), ppl.Driver(), "batch").Inc()

		// if a job has no priority, inherit it from the pipeline
		if j[i].Priority() == 0 {
			j[i].UpdatePriority(ppl.Priority())
		}

		ctxPush, cancel := context.WithTimeout(ctx, time.Second*time.Duration(p.cfg.Timeout))
		err := d.(jobsApi.Driver).Push(ctxPush, j[i])
		if err != nil {
			cancel()
			p.metrics.CountPushErr()
			p.log.Error("job push batch error", zap.String("ID", j[i].ID()), zap.String("pipeline", ppl.Name()), zap.String("driver", ppl.Driver()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()), zap.Error(err))
			return errors.E(op, err)
		}

		p.metrics.CountPushOk()

		p.metrics.pushJobLatencyHistogram.WithLabelValues(ppl.Name(), ppl.Driver(), "batch").Observe(time.Since(operationStart).Seconds())

		cancel()
	}

	p.log.Debug("job batch was pushed successfully", zap.Int("count", len(j)), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))

	return nil
}

func (p *Plugin) Pause(ctx context.Context, pp string) error {
	d, ppl, err := p.pipelineExists(pp)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(p.cfg.Timeout))
	defer cancel()
	// redirect call to the underlying driver
	return d.Pause(ctx, ppl.Name())
}

func (p *Plugin) Resume(ctx context.Context, pp string) error {
	d, ppl, err := p.pipelineExists(pp)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(p.cfg.Timeout))
	defer cancel()
	// redirect call to the underlying driver
	return d.Resume(ctx, ppl.Name())
}

// Declare a pipeline.
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

	// save priority as int64
	pr := pipeline.String(priorityKey, "10")
	prInt, err := strconv.Atoi(pr)
	if err != nil {
		// we can continue with a default priority
		p.log.Error(priorityKey, zap.Error(err))
	}

	pipeline.With(priorityKey, int64(prInt))

	// jobConstructors contains constructors for the drivers
	// we need here to initialize these drivers for the pipelines
	if _, ok := p.jobConstructors[dr]; ok {
		// init the driver from a pipeline
		initializedDriver, err := p.jobConstructors[dr].DriverFromPipeline(pipeline, p.queue)
		if err != nil {
			return errors.E(op, err)
		}

		// if a pipeline initialized to be consumed, call Run on it,
		// but likely for the dynamic pipelines it should be started manually
		if _, ok := p.consume[pipeline.Name()]; ok {
			ctxDeclare, cancel := context.WithTimeout(ctx, time.Second*time.Duration(p.cfg.Timeout))
			defer cancel()
			err = initializedDriver.Run(ctxDeclare, pipeline)
			if err != nil {
				return errors.E(op, err)
			}
		}

		// set how the pipeline was created
		pipeline.With(createdWithDeclare, "true")
		// add a driver to the set of the consumers (name - pipeline name, value - associated driver)
		p.consumers.Store(pipeline.Name(), initializedDriver)
		// save the pipeline
		p.pipelines.Store(pipeline.Name(), pipeline)
	}

	return nil
}

// Destroy the pipeline and release all associated resources.
func (p *Plugin) Destroy(ctx context.Context, pp string) error {
	const op = errors.Op("jobs_plugin_destroy")
	pipe, ok := p.pipelines.Load(pp)
	if !ok {
		return errors.E(op, errors.Errorf("no such pipeline, requested: %s", pp))
	}

	// type conversion
	ppl := pipe.(jobsApi.Pipeline)
	if pipe == nil {
		return errors.Str("no pipe registered, value is nil")
	}

	// delete consumer
	d, ok := p.consumers.LoadAndDelete(ppl.Name())
	if !ok {
		return errors.E(op, errors.Errorf("consumer not registered for the requested driver: %s", ppl.Driver()))
	}

	// delete old pipeline
	p.pipelines.Delete(pp)

	ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(p.cfg.Timeout))
	err := d.(jobsApi.Driver).Stop(ctx)
	if err != nil {
		cancel()
		return errors.E(op, err)
	}

	cancel()
	return nil
}

func (p *Plugin) List() []string {
	out := make([]string, 0, 10)

	p.pipelines.Range(func(key, _ any) bool {
		// we can safely convert value here as we know that we store keys as strings
		out = append(out, key.(string))
		return true
	})

	return out
}

func (p *Plugin) RPC() any {
	return &rpc{
		p: p,
	}
}

// pipelineExists used to check if the pipeline exists and return the underlying driver and pipeline or error if not exists
func (p *Plugin) pipelineExists(pp string) (jobsApi.Driver, jobsApi.Pipeline, error) {
	pipe, ok := p.pipelines.Load(pp)
	if !ok {
		p.log.Error("no such pipeline", zap.String("requested", pp))
		return nil, nil, fmt.Errorf("no such pipeline, requested: %s", pp)
	}

	if pipe == nil {
		p.log.Error("no pipe registered, value is nil")
		return nil, nil, fmt.Errorf("no pipe registered, value is nil")
	}

	ppl := pipe.(jobsApi.Pipeline)

	d, ok := p.consumers.Load(ppl.Name())
	if !ok {
		p.log.Warn("driver for the pipeline not found", zap.String("pipeline", pp))
		return nil, nil, fmt.Errorf("driver for the pipeline not found, pipeline: %s", pp)
	}

	return d.(jobsApi.Driver), ppl, nil
}

func (p *Plugin) readCommands() {
	for {
		select {
		case ev := <-p.eventsCh:
			ctx, span := p.tracer.Tracer(spanName).Start(context.Background(), "read_command", trace.WithSpanKind(trace.SpanKindServer))
			p.log.Debug("received JOBS event", zap.String("message", ev.Message()), zap.String("pipeline", ev.Plugin()))
			// message can be 'restart', 'stop'.
			switch ev.Message() {
			case stopStr:
				// by agreement, the message should contain the pipeline name
				pipeline := ev.Plugin()
				_, _, err := p.pipelineExists(pipeline)
				if err != nil {
					p.log.Warn("failed to restart the pipeline", zap.Error(err), zap.String("pipeline", pipeline))
					span.End()
					continue
				}

				// Destroy operation has its own timeout
				err = p.Destroy(context.Background(), pipeline)
				if err != nil {
					p.log.Error("failed to stop the pipeline", zap.Error(err), zap.String("pipeline", pipeline))
				}

				p.log.Info("pipeline was stopped", zap.String("pipeline", pipeline))
			case restartSrt:
				// Algorithm:
				// 1. Stop the pipeline.
				// 2. Delete the pipeline from the pipeline list.
				// 3. Delete the consumer (actual driver).
				// 4. Check how the pipeline was created (via Declare or via config).
				// 5. If the pipeline was created via config -> use jobsProcessor to add a job to create a pipeline.
				// 5a.If the pipeline was created via Declare -> use Declare again with the same parameters included in the pipeline.

				pipeline := ev.Plugin()
				drv, pipe, err := p.pipelineExists(pipeline)
				if err != nil {
					p.log.Warn("failed to restart the pipeline", zap.Error(err), zap.String("pipeline", pipeline))
					span.RecordError(err)
					span.End()
					continue
				}

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
				// 1. Stop the pipeline
				err = drv.Stop(ctx)
				if err != nil {
					p.log.Error("failed to stop the pipeline", zap.Error(err), zap.String("pipeline", pipeline))
				}
				cancel()

				// 2+3. Delete the pipeline from the pipeline list
				p.pipelines.Delete(pipeline)
				p.consumers.Delete(pipeline)

				// 4. Check how the pipeline was created
				if pipe.String(createdWithDeclare, "") == trueStr { //nolint:gocritic
					// 5. If the pipeline was created via Declare
					err = p.Declare(ctx, pipe)
					if err != nil {
						p.log.Error("failed to restart the pipeline", zap.Error(err), zap.String("pipeline", pipeline))
						span.RecordError(err)
						span.End()
						continue
					}
					// TIP: Do not need to store the pipeline and consumer, as it was done in the Declare
				} else if pipe.String(createdWithConfig, "") != "" {
					// 5a. If the pipeline was created via config
					p.jobsProcessor.add(&pjob{
						p.jobConstructors[pipe.Driver()],
						pipe,
						p.queue,
						pipe.String(createdWithConfig, ""),
						p.cfg.Timeout,
					})

					p.jobsProcessor.wait()
					if p.jobsProcessor.hasErrors() {
						// TODO(rustatian): pretty print errors
						p.log.Error("failed to restart the pipeline", zap.Errors("errors", p.jobsProcessor.errors()))
						span.End()
						continue
					}

					// Store the pipeline, consumer would be added by the processor
					p.pipelines.Store(pipeline, pipe)
				} else {
					p.log.Warn("unknown pipeline creation method", zap.String("pipeline", pipeline))
				}

				span.End()
			default:
				p.log.Warn("unknown command", zap.String("command", ev.Message()))
			}
			err := p.Destroy(ctx, ev.Plugin())
			if err != nil {
				p.log.Error("failed to destroy the pipeline", zap.Error(err), zap.String("pipeline", ev.Plugin()))
			}
			span.End()

		case <-p.stopCh:
			return
		}
	}
}

func (p *Plugin) payload(body, context []byte) *payload.Payload {
	pld := p.pldPool.Get().(*payload.Payload)
	pld.Body = body
	pld.Context = context
	pld.Codec = frame.CodecRaw
	return pld
}

func (p *Plugin) putPayload(pld *payload.Payload) {
	pld.Body = nil
	pld.Context = nil
	pld.Codec = 0
	pld.Flags = 0
	p.pldPool.Put(pld)
}

func ptrTo[T any](val T) *T {
	return &val
}
