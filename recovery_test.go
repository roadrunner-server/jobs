package jobs

import (
	"context"
	stderr "errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	jobsApi "github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/events"
	poolConfig "github.com/roadrunner-server/pool/v2/pool"
	staticPool "github.com/roadrunner-server/pool/v2/pool/static_pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestartRetainsConsumption(t *testing.T) {
	tests := []struct {
		name       string
		configured bool
		consume    bool
		action     string
		actionErr  bool
		wantRuns   int
	}{
		{name: "declared resumed", action: "resume", wantRuns: 1},
		{name: "configured resumed", configured: true, action: "resume", wantRuns: 1},
		{name: "declared paused", consume: true, action: "pause"},
		{name: "configured paused", configured: true, consume: true, action: "pause"},
		{name: "declared resume failed", action: "resume", actionErr: true},
		{name: "configured resume failed", configured: true, action: "resume", actionErr: true},
		{name: "declared pause failed", consume: true, action: "pause", actionErr: true, wantRuns: 1},
		{name: "configured pause failed", configured: true, consume: true, action: "pause", actionErr: true, wantRuns: 1},
		{name: "declared idle"},
		{name: "configured idle", configured: true},
		{name: "declared consuming", consume: true, wantRuns: 1},
		{name: "configured consuming", configured: true, consume: true, wantRuns: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const pipeline = "test-restart-state"
			cfg := &Config{}
			if tt.consume {
				cfg.Consume = []string{pipeline}
			}
			p := newTestPlugin(t, cfg)
			old := &fakeDriver{state: &jobsApi.State{Ready: false}}
			jc := &fakeConstructor{name: "memory", driver: old}
			p.jobConstructors[jc.name] = jc
			pipe := Pipeline{name: pipeline, driver: jc.name}
			if tt.configured {
				pipe.With(createdWithConfig, "jobs.pipelines."+pipeline+".config")
				p.pipelines.Store(pipeline, pipe)
				p.consumers.Store(pipeline, jobsApi.Driver(old))
			} else {
				require.NoError(t, p.Declare(t.Context(), pipe))
			}

			var err error
			switch tt.action {
			case "pause":
				if tt.actionErr {
					old.pauseErr = stderr.New("pause failed")
				}
				err = p.Pause(t.Context(), pipeline)
			case "resume":
				if tt.actionErr {
					old.resumeErr = stderr.New("resume failed")
				}
				err = p.Resume(t.Context(), pipeline)
			}
			if tt.actionErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			replacement := &fakeDriver{}
			jc.driver = replacement
			_, join := startCommands(t, p)
			p.eventsCh <- driverCommand(t, pipeline, restartSrt)
			join()

			assert.Equal(t, 1, old.Stops())
			assert.Equal(t, tt.wantRuns, replacement.Runs())
			actual, _, err := p.pipelineExists(pipeline)
			require.NoError(t, err)
			assert.Same(t, replacement, actual)
		})
	}
}

type recoveryDriver struct {
	fakeDriver
	stop func(context.Context)
}

func (d *recoveryDriver) Stop(ctx context.Context) error {
	if d.stop != nil {
		d.stop(ctx)
	}
	return d.fakeDriver.Stop(ctx)
}

type recoveryConstructor struct {
	mu      sync.Mutex
	drivers map[string][]*recoveryDriver
	blocked string
	entered chan struct{}
	release chan struct{}
}

func (*recoveryConstructor) Name() string { return "memory" }

func (c *recoveryConstructor) DriverFromConfig(_ context.Context, _ string, _ jobsApi.Queue, pipe jobsApi.Pipeline) (jobsApi.Driver, error) {
	return c.newDriver(pipe.Name()), nil
}

func (c *recoveryConstructor) DriverFromPipeline(_ context.Context, pipe jobsApi.Pipeline, _ jobsApi.Queue) (jobsApi.Driver, error) {
	return c.newDriver(pipe.Name()), nil
}

func (c *recoveryConstructor) newDriver(pipeline string) *recoveryDriver {
	c.mu.Lock()
	defer c.mu.Unlock()

	d := &recoveryDriver{}
	if pipeline == c.blocked && len(c.drivers[pipeline]) == 0 {
		entered := sync.OnceFunc(func() { close(c.entered) })
		d.stop = func(ctx context.Context) {
			entered()
			select {
			case <-c.release:
			case <-ctx.Done():
			}
		}
	}
	c.drivers[pipeline] = append(c.drivers[pipeline], d)
	return d
}

func (c *recoveryConstructor) generations(pipeline string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.drivers[pipeline])
}

func TestRestartCommandsSurviveBlockedRestart(t *testing.T) {
	pipelines := []struct {
		name       string
		configured bool
	}{
		{name: "test-restart-blocked", configured: true},
		{name: "test-restart-configured-a", configured: true},
		{name: "test-restart-configured-b", configured: true},
		{name: "test-restart-declared-a"},
		{name: "test-restart-declared-b"},
	}
	cfg := &Config{
		Pools:     map[string]*poolConfig.Config{},
		Pipelines: make(map[string]Pipeline),
	}
	for _, pipe := range pipelines {
		cfg.Consume = append(cfg.Consume, pipe.name)
		if pipe.configured {
			cfg.Pipelines[pipe.name] = Pipeline{driver: "memory"}
		}
	}
	p := newTestPlugin(t, cfg)
	c := &recoveryConstructor{
		drivers: make(map[string][]*recoveryDriver),
		blocked: pipelines[0].name,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	p.jobConstructors[c.Name()] = c
	errCh := p.Serve()
	t.Cleanup(func() { require.NoError(t, p.Stop(context.Background())) })
	unblock := sync.OnceFunc(func() { close(c.release) })
	t.Cleanup(unblock)
	for _, pipe := range pipelines {
		if !pipe.configured {
			require.NoError(t, p.Declare(t.Context(), Pipeline{name: pipe.name, driver: c.Name()}))
		}
	}

	bus, id := events.NewEventBus()
	observed := make(chan events.Event, len(pipelines)+1)
	require.NoError(t, bus.SubscribeP(id, "*.EventJOBSDriverCommand", observed))
	t.Cleanup(func() { bus.Unsubscribe(id) })
	bus.Send(driverCommand(t, c.blocked, restartSrt))
	select {
	case <-c.entered:
	case <-time.After(callTimeout):
		t.Fatal("the first restart did not reach Stop")
	}
	for _, pipe := range pipelines[1:] {
		bus.Send(driverCommand(t, pipe.name, restartSrt))
	}

	// The bus processes the sentinel after all restart commands.
	const sentinel = "test-restart-barrier"
	bus.Send(driverCommand(t, sentinel, "barrier"))
barrier:
	for {
		select {
		case ev := <-observed:
			if ev.Plugin() == sentinel {
				break barrier
			}
		case <-time.After(callTimeout):
			t.Fatal("the bus did not process the restart commands")
		}
	}
	unblock()
	require.Eventually(t, func() bool {
		for _, pipe := range pipelines {
			if c.generations(pipe.name) != 2 {
				return false
			}
		}
		return true
	}, callTimeout, time.Millisecond, "each failed pipeline must get a replacement driver")
	assert.Empty(t, errCh)
}

func TestStopCancelsPendingRestart(t *testing.T) {
	const pipeline = "test-restart-shutdown"
	p := newTestPlugin(t, &Config{
		Pools:     map[string]*poolConfig.Config{},
		Pipelines: map[string]Pipeline{pipeline: {driver: "memory"}},
		Consume:   []string{pipeline},
	})
	c := &recoveryConstructor{
		drivers: make(map[string][]*recoveryDriver),
		blocked: pipeline,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	p.jobConstructors[c.Name()] = c
	p.Serve()
	stop := sync.OnceValue(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return p.Stop(ctx)
	})
	t.Cleanup(func() {
		close(c.release)
		_ = stop()
	})

	bus, _ := events.NewEventBus()
	bus.Send(driverCommand(t, pipeline, restartSrt))
	select {
	case <-c.entered:
	case <-time.After(callTimeout):
		t.Fatal("the restart did not reach Stop")
	}
	require.NoError(t, stop())
	assert.Empty(t, p.List())
	assert.Equal(t, 1, c.generations(pipeline))
}

func TestCommandSubscriptionMatchesPipelineName(t *testing.T) {
	tests := []struct {
		name     string
		pipeline string
		other    string
	}{
		{name: "common prefix", pipeline: "queue", other: "queue-backup"},
		{name: "dot in name", pipeline: "queue", other: "queue.backup"},
		{name: "case-distinct names", pipeline: "queue", other: "Queue"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestPlugin(t, &Config{
				Pools:     map[string]*poolConfig.Config{},
				Pipelines: map[string]Pipeline{tt.pipeline: {driver: "memory"}},
			})
			c := &recoveryConstructor{drivers: make(map[string][]*recoveryDriver)}
			p.jobConstructors[c.Name()] = c
			p.Serve()
			t.Cleanup(func() { require.NoError(t, p.Stop(context.Background())) })
			require.NoError(t, p.Declare(t.Context(), Pipeline{name: tt.other, driver: c.Name()}))
			p.eventBus.Send(driverCommand(t, tt.other, restartSrt))
			require.Eventually(t, func() bool {
				return c.generations(tt.other) >= 2
			}, callTimeout, time.Millisecond, "the pipeline command was not delivered")
			require.Never(t, func() bool {
				return c.generations(tt.other) > 2
			}, time.Millisecond*100, time.Millisecond, "one event must produce one restart")
			require.Equal(t, 1, c.generations(tt.pipeline))
		})
	}
}

type recoveryServer struct {
	allocate func() error
}

func (s *recoveryServer) NewPool(context.Context, *poolConfig.Config, map[string]string, *slog.Logger) (*staticPool.Pool, error) {
	return nil, s.allocate()
}

func TestServeAllowsWorkerLifecycleCalls(t *testing.T) {
	p := newTestPlugin(t, &Config{})
	p.jobConstructors["memory"] = &fakeConstructor{name: "memory", driver: &fakeDriver{}}
	want := stderr.New("worker allocation reached")
	p.server = &recoveryServer{allocate: func() error {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := p.Declare(ctx, Pipeline{name: "worker-pipeline", driver: "memory"}); err != nil {
			return err
		}
		if err := p.Resume(ctx, "worker-pipeline"); err != nil {
			return err
		}
		return want
	}}
	done := make(chan chan error, 1)
	go func() { done <- p.Serve() }()
	select {
	case errCh := <-done:
		require.ErrorContains(t, <-errCh, want.Error())
	case <-time.After(time.Second * 2):
		t.Fatal("worker allocation blocked its lifecycle calls")
	}
}

func TestStopAllowsInFlightWorkerLifecycleCall(t *testing.T) {
	const pipeline = "worker-shutdown"
	p := newTestPlugin(t, &Config{})
	stopping := make(chan struct{})
	d := &recoveryDriver{stop: func(context.Context) { close(stopping) }}
	p.pipelines.Store(pipeline, Pipeline{name: pipeline, driver: "memory"})
	p.consumers.Store(pipeline, jobsApi.Driver(d))
	result := make(chan error, 1)
	p.pollersWg.Go(func() {
		<-stopping
		result <- p.Resume(t.Context(), pipeline)
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, p.Stop(ctx))
	require.NoError(t, ctx.Err(), "shutdown must let the worker finish its lifecycle call")
	require.ErrorIs(t, <-result, context.Canceled)
}

type blockedRecoveryConstructor struct {
	fakeConstructor
	entered chan struct{}
	release chan struct{}
}

func (c *blockedRecoveryConstructor) DriverFromPipeline(context.Context, jobsApi.Pipeline, jobsApi.Queue) (jobsApi.Driver, error) {
	close(c.entered)
	<-c.release
	return c.driver, nil
}

func TestStopDeadlineDuringDeclare(t *testing.T) {
	p := newTestPlugin(t, &Config{})
	c := &blockedRecoveryConstructor{
		name:    "memory",
		driver:  &fakeDriver{},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	p.jobConstructors[c.Name()] = c
	declared := make(chan error, 1)
	go func() {
		declared <- p.Declare(t.Context(), Pipeline{name: "slow-declare", driver: c.Name()})
	}()
	<-c.entered
	unblock := sync.OnceFunc(func() { close(c.release) })
	t.Cleanup(unblock)
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond*50)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- p.Stop(ctx) }()
	select {
	case err := <-stopped:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("Stop waited past its deadline for Declare")
	}
	unblock()
	require.ErrorIs(t, <-declared, context.Canceled)
	require.Empty(t, p.List())
	require.Equal(t, 1, c.driver.Stops())
}
