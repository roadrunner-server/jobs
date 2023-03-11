package jobs

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	jobsApi "github.com/roadrunner-server/api/v4/plugins/v1/jobs"
	pq "github.com/roadrunner-server/api/v4/plugins/v1/priority_queue"

	"github.com/roadrunner-server/endure/v2/dep"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/goridge/v3/pkg/frame"
	rh "github.com/roadrunner-server/jobs/v4/protocol"
	"github.com/roadrunner-server/sdk/v4/payload"
	pqImpl "github.com/roadrunner-server/sdk/v4/priority_queue"
	"github.com/roadrunner-server/sdk/v4/state/process"
	"github.com/roadrunner-server/sdk/v4/utils"
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
)

type exporter struct {
	jobsOk, pushOk, jobsErr, pushErr *uint64
}

type Plugin struct {
	mu sync.RWMutex

	// Jobs plugin configuration
	cfg         *Config `structure:"jobs"`
	log         *zap.Logger
	workersPool Pool
	server      Server

	jobConstructors map[string]jobsApi.Constructor
	consumers       sync.Map // map[string]jobs.Consumer

	metrics *exporter

	// priority queue implementation
	queue pq.Queue

	// parent config for broken options. keys are pipelines names, values - pointers to the associated pipeline
	pipelines sync.Map

	// initial set of the pipelines to consume
	consume map[string]struct{}

	// signal channel to stop the pollers
	stopCh     chan struct{}
	commandsCh chan jobsApi.Commander

	// internal payloads pool
	pldPool       sync.Pool
	statsExporter *statsExporter
	respHandler   *rh.RespHandler
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
	p.commandsCh = make(chan jobsApi.Commander, 1)

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
	p.queue = pqImpl.NewBinHeap[pq.Item](p.cfg.PipelineSize)
	p.log = new(zap.Logger)
	p.log = log.NamedLogger(PluginName)
	p.metrics = &exporter{
		jobsOk:  utils.Uint64(0),
		pushOk:  utils.Uint64(0),
		jobsErr: utils.Uint64(0),
		pushErr: utils.Uint64(0),
	}

	// exporter
	p.statsExporter = newStatsExporter(p, p.metrics.jobsOk, p.metrics.pushOk, p.metrics.jobsErr, p.metrics.pushErr)
	p.respHandler = rh.NewResponseHandler(p.log)

	if err != nil {
		return errors.E(op, err)
	}

	return nil
}

func (p *Plugin) Serve() chan error {
	errCh := make(chan error, 1)
	const op = errors.Op("jobs_plugin_serve")

	go p.readCommands()

	// register initial pipelines
	p.pipelines.Range(func(key, value any) bool {
		t := time.Now()
		// pipeline name (ie test-local, sqs-aws, etc)
		name := key.(string)

		// pipeline associated with the name
		pipe := value.(jobsApi.Pipeline)
		// driver for the pipeline (ie amqp, ephemeral, etc)
		dr := pipe.Driver()

		if dr == "" {
			p.log.Warn("can't find driver name for the pipeline, please, check that the 'driver' keyword for the pipelines specified correctly, JOBS plugin will try to run the next pipeline")
			return true
		}

		// jobConstructors contains constructors for the drivers
		// we need here to initialize these drivers for the pipelines
		if _, ok := p.jobConstructors[dr]; ok {
			// v2.7 and newer
			// config key for the particular sub-driver jobs.pipelines.test-local
			configKey := fmt.Sprintf("%s.%s.%s.%s", PluginName, pipelines, name, cfgKey)

			// init the driver
			initializedDriver, err := p.jobConstructors[dr].DriverFromConfig(configKey, p.queue, pipe, p.commandsCh)
			if err != nil {
				errCh <- errors.E(op, err)
				return false
			}

			// add driver to the set of the consumers (name - pipeline name, value - associated driver)
			p.consumers.Store(name, initializedDriver)

			p.log.Debug("driver ready", zap.String("pipeline", pipe.Name()), zap.String("driver", pipe.Driver()), zap.Time("start", t), zap.Duration("elapsed", time.Since(t)))
			// if pipeline initialized to be consumed, call Run on it
			if _, ok := p.consume[name]; ok {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(p.cfg.Timeout))
				defer cancel()
				err = initializedDriver.Run(ctx, pipe)
				if err != nil {
					errCh <- errors.E(op, err)
					return false
				}
				return true
			}

			return true
		}

		return true
	})

	// do not continue processing, immediately stop if channel contains an error
	if len(errCh) > 0 {
		return errCh
	}

	go func() {
		p.mu.Lock()
		defer p.mu.Unlock()

		var err error
		p.workersPool, err = p.server.NewPool(context.Background(), p.cfg.Pool, map[string]string{RrMode: RrModeJobs}, nil)
		if err != nil {
			errCh <- err
			return
		}

		// start listening
		p.listener()
	}()

	return errCh
}

func (p *Plugin) Stop(context.Context) error {
	// this function can block forever, but we don't care, because we might have a chance to exit from the pollers,
	// but if not, this is not a problem at all.
	// The main target is to stop the drivers
	go func() {
		for i := uint8(0); i < p.cfg.NumPollers+1; i++ {
			// stop jobs plugin pollers
			p.stopCh <- struct{}{}
		}
	}()

	// range over all consumers and call stop
	p.consumers.Range(func(key, value any) bool {
		consumer := value.(jobsApi.Driver)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(p.cfg.Timeout))
		err := consumer.Stop(ctx)
		if err != nil {
			cancel()
			p.log.Error("stop job driver", zap.Any("driver", key), zap.Error(err))
			return true
		}
		cancel()
		return true
	})

	p.pipelines.Range(func(key, _ any) bool {
		p.pipelines.Delete(key)
		return true
	})

	close(p.commandsCh)

	return nil
}

func (p *Plugin) Collects() []*dep.In {
	return []*dep.In{
		dep.Fits(func(pp any) {
			consumer := pp.(jobsApi.Constructor)
			p.jobConstructors[consumer.Name()] = consumer
		}, (*jobsApi.Constructor)(nil)),
	}
}

func (p *Plugin) Workers() []*process.State {
	p.mu.RLock()
	wrk := p.workersPool.Workers()
	p.mu.RUnlock()

	ps := make([]*process.State, len(wrk))

	for i := 0; i < len(wrk); i++ {
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
	jst := make([]*jobsApi.State, 0, 2)
	var err error
	p.consumers.Range(func(key, value any) bool {
		consumer := value.(jobsApi.Driver)
		newCtx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(p.cfg.Timeout))

		var state *jobsApi.State
		state, err = consumer.State(newCtx)
		if err != nil {
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

func (p *Plugin) Push(j jobsApi.Job) error {
	const op = errors.Op("jobs_plugin_push")

	start := time.Now()
	// get the pipeline for the job
	pipe, ok := p.pipelines.Load(j.Pipeline())
	if !ok {
		return errors.E(op, errors.Errorf("no such pipeline, requested: %s", j.Pipeline()))
	}

	// type conversion
	ppl := pipe.(jobsApi.Pipeline)

	d, ok := p.consumers.Load(ppl.Name())
	if !ok {
		return errors.E(op, errors.Errorf("consumer not registered for the requested driver: %s", ppl.Driver()))
	}

	p.statsExporter.pushJobRequestCounter.WithLabelValues(ppl.Name(), ppl.Driver(), "single").Inc()

	// if job has no priority, inherit it from the pipeline
	if j.Priority() == 0 {
		j.UpdatePriority(ppl.Priority())
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(p.cfg.Timeout))
	defer cancel()

	err := d.(jobsApi.Driver).Push(ctx, j)
	if err != nil {
		atomic.AddUint64(p.metrics.pushErr, 1)
		p.log.Error("job push error", zap.String("ID", j.ID()), zap.String("pipeline", ppl.Name()), zap.String("driver", ppl.Driver()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)), zap.Error(err))
		return errors.E(op, err)
	}

	atomic.AddUint64(p.metrics.pushOk, 1)
	p.statsExporter.pushJobLatencyHistogram.WithLabelValues(ppl.Name(), ppl.Driver(), "single").Observe(time.Since(start).Seconds())

	p.log.Debug("job was pushed successfully", zap.String("ID", j.ID()), zap.String("pipeline", ppl.Name()), zap.String("driver", ppl.Driver()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))

	return nil
}

func (p *Plugin) PushBatch(j []jobsApi.Job) error {
	const op = errors.Op("jobs_plugin_push")
	start := time.Now()

	for i := 0; i < len(j); i++ {
		operationStart := time.Now()
		// get the pipeline for the job
		pipe, ok := p.pipelines.Load(j[i].Pipeline())
		if !ok {
			return errors.E(op, errors.Errorf("no such pipeline, requested: %s", j[i].Pipeline()))
		}

		ppl := pipe.(jobsApi.Pipeline)

		d, ok := p.consumers.Load(ppl.Name())
		if !ok {
			return errors.E(op, errors.Errorf("consumer not registered for the requested driver: %s", ppl.Driver()))
		}

		p.statsExporter.pushJobRequestCounter.WithLabelValues(ppl.Name(), ppl.Driver(), "single").Inc()

		// if job has no priority, inherit it from the pipeline
		if j[i].Priority() == 0 {
			j[i].UpdatePriority(ppl.Priority())
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(p.cfg.Timeout))
		err := d.(jobsApi.Driver).Push(ctx, j[i])
		if err != nil {
			cancel()
			atomic.AddUint64(p.metrics.pushErr, 1)
			p.log.Error("job push batch error", zap.String("ID", j[i].ID()), zap.String("pipeline", ppl.Name()), zap.String("driver", ppl.Driver()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)), zap.Error(err))
			return errors.E(op, err)
		}

		p.statsExporter.pushJobLatencyHistogram.WithLabelValues(ppl.Name(), ppl.Driver(), "batch").Observe(time.Since(operationStart).Seconds())

		cancel()
	}

	return nil
}

func (p *Plugin) Pause(pp string) error {
	d, ppl, err := p.check(pp)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(p.cfg.Timeout))
	defer cancel()
	// redirect call to the underlying driver
	return d.Pause(ctx, ppl.Name())
}

func (p *Plugin) Resume(pp string) error {
	d, ppl, err := p.check(pp)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(p.cfg.Timeout))
	defer cancel()
	// redirect call to the underlying driver
	return d.Resume(ctx, ppl.Name())
}

// Declare a pipeline.
func (p *Plugin) Declare(pipeline jobsApi.Pipeline) error {
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
		// init the driver from pipeline
		initializedDriver, err := p.jobConstructors[dr].DriverFromPipeline(pipeline, p.queue, p.commandsCh)
		if err != nil {
			return errors.E(op, err)
		}

		// if pipeline initialized to be consumed, call Run on it
		// but likely for the dynamic pipelines it should be started manually
		if _, ok := p.consume[pipeline.Name()]; ok {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(p.cfg.Timeout))
			defer cancel()
			err = initializedDriver.Run(ctx, pipeline)
			if err != nil {
				return errors.E(op, err)
			}
		}

		// add driver to the set of the consumers (name - pipeline name, value - associated driver)
		p.consumers.Store(pipeline.Name(), initializedDriver)
		// save the pipeline
		p.pipelines.Store(pipeline.Name(), pipeline)
	}

	return nil
}

// Destroy pipeline and release all associated resources.
func (p *Plugin) Destroy(pp string) error {
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(p.cfg.Timeout))
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

func (p *Plugin) check(pp string) (jobsApi.Driver, jobsApi.Pipeline, error) {
	pipe, ok := p.pipelines.Load(pp)
	if !ok {
		p.log.Error("no such pipeline", zap.String("requested", pp))
		return nil, nil, errors.E(errors.Errorf("no such pipeline, requested: %s", pp))
	}

	if pipe == nil {
		p.log.Error("no pipe registered, value is nil")
		return nil, nil, errors.E(errors.Str("no pipe registered, value is nil"))
	}

	ppl := pipe.(jobsApi.Pipeline)

	d, ok := p.consumers.Load(ppl.Name())
	if !ok {
		p.log.Warn("driver for the pipeline not found", zap.String("pipeline", pp))
		return nil, nil, errors.E(errors.Errorf("driver for the pipeline not found, pipeline: %s", pp))
	}

	return d.(jobsApi.Driver), ppl, nil
}

func (p *Plugin) readCommands() {
	for cmd := range p.commandsCh {
		switch cmd.Command() { //nolint:gocritic
		case jobsApi.Stop:
			err := p.Destroy(cmd.Pipeline())
			if err != nil {
				p.log.Error("failed to destroy the pipeline", zap.Error(err), zap.String("pipeline", cmd.Pipeline()))
			}
		}
	}
}

func (p *Plugin) getPayload(body, context []byte) *payload.Payload {
	pld := p.pldPool.Get().(*payload.Payload)
	pld.Body = body
	pld.Context = context
	pld.Codec = frame.CodecJSON
	return pld
}

func (p *Plugin) putPayload(pld *payload.Payload) {
	pld.Body = nil
	pld.Context = nil
	p.pldPool.Put(pld)
}
