package jobs

import (
	"context"
	stderr "errors"
	"sync"
	"testing"

	jobsApi "github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startCommands runs the command loop and returns its error channel plus a join
// function. Closing the events channel drains the buffered events first, so the
// join orders every assertion after the loop has handled them.
func startCommands(t *testing.T, p *Plugin) (chan error, func()) {
	t.Helper()

	errCh := make(chan error, 1)
	done := make(chan struct{})

	go func() {
		defer close(done)
		p.readCommands(errCh)
	}()

	join := sync.OnceFunc(func() {
		close(p.eventsCh)
		<-done
	})
	t.Cleanup(join)

	return errCh, join
}

func driverCommand(t *testing.T, pipeline, message string) events.Event {
	t.Helper()

	ev := events.NewEvent(events.EventJOBSDriverCommand, pipeline, message)
	require.NotNil(t, ev)

	return ev
}

// declareTestPipeline registers a pipeline through Declare, which is what the
// restart path recognizes as created_with_declare.
func declareTestPipeline(t *testing.T, p *Plugin, name string) *fakeConstructor {
	t.Helper()

	jc := &fakeConstructor{name: "memory", driver: &fakeDriver{}}
	p.jobConstructors["memory"] = jc
	require.NoError(t, p.Declare(t.Context(), Pipeline{"name": name, "driver": "memory"}))

	return jc
}

func TestReadCommandsStop(t *testing.T) {
	p := newTestPlugin(t, &Config{})
	jc := declareTestPipeline(t, p, "test-1-memory")
	_, join := startCommands(t, p)

	p.eventsCh <- driverCommand(t, "test-1-memory", stopStr)
	join()

	assert.Empty(t, p.List())
	assert.Equal(t, 1, jc.driver.Stops())
}

// An event naming a pipeline that is gone is logged and the loop keeps running.
func TestReadCommandsUnknownPipeline(t *testing.T) {
	p := newTestPlugin(t, &Config{})
	jc := declareTestPipeline(t, p, "test-1-memory")
	_, join := startCommands(t, p)

	p.eventsCh <- driverCommand(t, "test-missing", stopStr)
	p.eventsCh <- driverCommand(t, "test-missing", restartSrt)
	p.eventsCh <- driverCommand(t, "test-1-memory", "reload")
	p.eventsCh <- driverCommand(t, "test-1-memory", stopStr)
	join()

	assert.Empty(t, p.List())
	assert.Equal(t, 1, jc.driver.Stops())
}

func TestReadCommandsRestartDeclaredPipeline(t *testing.T) {
	p := newTestPlugin(t, &Config{})
	jc := declareTestPipeline(t, p, "test-1-memory")
	_, join := startCommands(t, p)

	p.eventsCh <- driverCommand(t, "test-1-memory", restartSrt)
	join()

	assert.Equal(t, []string{"test-1-memory"}, p.List())
	assert.Equal(t, 1, jc.driver.Stops())
	assert.Equal(t, 2, jc.fromPipeline)
}

// A pipeline that came from the config is rebuilt through the pipeline
// processor, which asks the constructor for a driver by config key.
func TestReadCommandsRestartConfiguredPipeline(t *testing.T) {
	p := newTestPlugin(t, &Config{})
	jc := &fakeConstructor{name: "memory", driver: &fakeDriver{}}
	p.jobConstructors["memory"] = jc

	pipe := Pipeline{"name": "test-1-memory", "driver": "memory"}
	pipe.With(createdWithConfig, "jobs.pipelines.test-1-memory.config")
	p.pipelines.Store("test-1-memory", pipe)
	p.consumers.Store("test-1-memory", jobsApi.Driver(jc.driver))

	_, join := startCommands(t, p)

	p.eventsCh <- driverCommand(t, "test-1-memory", restartSrt)
	join()

	assert.Equal(t, []string{"test-1-memory"}, p.List())
	assert.Equal(t, 1, jc.driver.Stops())
	assert.Equal(t, 1, jc.fromConfig)
}

// A pipeline that cannot be rebuilt stops the server through the service error channel.
func TestReadCommandsRestartFailureStopsTheServer(t *testing.T) {
	tests := []struct {
		name               string
		fromConfig         bool
		resumeBefore       bool
		restartInitErr     error
		restartRunErr      error
		wantErr            string
		wantReplacementDrv bool
	}{
		{
			name:           "configured pipeline constructor error",
			fromConfig:     true,
			restartInitErr: stderr.New("connect failed"),
			wantErr:        "connect failed",
		},
		{
			name:           "declared pipeline constructor error",
			restartInitErr: stderr.New("broker unavailable"),
			wantErr:        "broker unavailable",
		},
		{
			name:               "declared pipeline run error",
			resumeBefore:       true,
			restartRunErr:      stderr.New("consume failed"),
			wantErr:            "consume failed",
			wantReplacementDrv: true,
		},
	}

	for i := range tests {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			p := newTestPlugin(t, &Config{})
			jc := &seqConstructor{name: "memory"}
			p.jobConstructors["memory"] = jc

			var initialDrv *fakeDriver
			if tc.fromConfig {
				initialDrv = jc.nextDriver()
				pipe := Pipeline{"name": "test-1-memory", "driver": "memory"}
				pipe.With(createdWithConfig, "jobs.pipelines.test-1-memory.config")
				p.pipelines.Store("test-1-memory", pipe)
				p.consumers.Store("test-1-memory", jobsApi.Driver(initialDrv))
			} else {
				require.NoError(t, p.Declare(t.Context(), Pipeline{"name": "test-1-memory", "driver": "memory"}))
				require.Len(t, jc.drivers, 1)
				initialDrv = jc.drivers[0]
			}

			if tc.resumeBefore {
				require.NoError(t, p.Resume(t.Context(), "test-1-memory"))
			}

			jc.nextInitErr = tc.restartInitErr
			jc.nextRunErr = tc.restartRunErr

			errCh, join := startCommands(t, p)

			p.eventsCh <- driverCommand(t, "test-1-memory", restartSrt)
			join()

			require.Len(t, errCh, 1)
			err := <-errCh
			require.ErrorContains(t, err, tc.wantErr)
			assert.Empty(t, p.List())
			assert.Equal(t, 1, initialDrv.Stops())
			if tc.wantReplacementDrv {
				require.Len(t, jc.drivers, 2)
				assert.Equal(t, 1, jc.drivers[1].Stops())
			}
		})
	}
}

// A pipeline restored by neither route is only logged.
func TestReadCommandsRestartUnknownCreationMethod(t *testing.T) {
	p := newTestPlugin(t, &Config{})
	drv := &fakeDriver{}
	p.pipelines.Store("test-1-memory", Pipeline{"name": "test-1-memory", "driver": "memory"})
	p.consumers.Store("test-1-memory", jobsApi.Driver(drv))

	_, join := startCommands(t, p)

	p.eventsCh <- driverCommand(t, "test-1-memory", restartSrt)
	join()

	assert.Empty(t, p.List())
	assert.Equal(t, 1, drv.Stops())
}

// Stop closes the signal channel, which ends the loop even with no event.
func TestReadCommandsStopsOnSignal(t *testing.T) {
	p := newTestPlugin(t, &Config{})
	errCh := make(chan error, 1)

	close(p.stopCh)
	requireReturns(t, func() { p.readCommands(errCh) })

	assert.Empty(t, errCh)
}

type seqConstructor struct {
	name string

	mu          sync.Mutex
	nextInitErr error
	nextRunErr  error
	drivers     []*fakeDriver
}

func (s *seqConstructor) Name() string { return s.name }

func (s *seqConstructor) nextDriver() *fakeDriver {
	s.mu.Lock()
	runErr := s.nextRunErr
	drv := &fakeDriver{state: &jobsApi.State{}, runErr: runErr}
	s.drivers = append(s.drivers, drv)
	s.mu.Unlock()

	return drv
}

func (s *seqConstructor) DriverFromConfig(_ context.Context, _ string, _ jobsApi.Queue, _ jobsApi.Pipeline) (jobsApi.Driver, error) {
	s.mu.Lock()
	err := s.nextInitErr
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}

	return s.nextDriver(), nil
}

func (s *seqConstructor) DriverFromPipeline(_ context.Context, _ jobsApi.Pipeline, _ jobsApi.Queue) (jobsApi.Driver, error) {
	s.mu.Lock()
	err := s.nextInitErr
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}

	return s.nextDriver(), nil
}

func TestReadCommandsRestartRestoresRunningState(t *testing.T) {
	tests := []struct {
		name                string
		fromConfig          bool
		consume             []string
		resumeBeforeRestart bool
		pauseBeforeRestart  bool
		readyAtFailure      bool
		wantReplacementRuns int
	}{
		{
			name:                "declared pipeline never resumed stays inactive",
			wantReplacementRuns: 0,
		},
		{
			name:                "declared pipeline resumed with ready true restarts running",
			resumeBeforeRestart: true,
			readyAtFailure:      true,
			wantReplacementRuns: 1,
		},
		{
			name:                "declared pipeline resumed with ready false at failure restarts running",
			resumeBeforeRestart: true,
			readyAtFailure:      false,
			wantReplacementRuns: 1,
		},
		{
			name:                "declared pipeline resumed then paused stays inactive",
			resumeBeforeRestart: true,
			pauseBeforeRestart:  true,
			wantReplacementRuns: 0,
		},
		{
			name:                "declared pipeline in consume stays running",
			consume:             []string{"stand"},
			wantReplacementRuns: 1,
		},
		{
			name:                "declared pipeline in consume then paused stays inactive",
			consume:             []string{"stand"},
			pauseBeforeRestart:  true,
			wantReplacementRuns: 0,
		},
		{
			name:                "configured pipeline not in consume then resumed restarts running",
			fromConfig:          true,
			resumeBeforeRestart: true,
			wantReplacementRuns: 1,
		},
		{
			name:                "configured pipeline in consume then paused stays inactive",
			fromConfig:          true,
			consume:             []string{"stand"},
			pauseBeforeRestart:  true,
			wantReplacementRuns: 0,
		},
	}

	for i := range tests {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			p := newTestPlugin(t, &Config{Consume: tc.consume})
			jc := &seqConstructor{name: "memory"}
			p.jobConstructors["memory"] = jc

			var initialDrv *fakeDriver
			if tc.fromConfig {
				initialDrv = jc.nextDriver()
				pipe := Pipeline{"name": "stand", "driver": "memory"}
				pipe.With(createdWithConfig, "jobs.pipelines.stand.config")
				p.pipelines.Store("stand", pipe)
				p.consumers.Store("stand", jobsApi.Driver(initialDrv))
			} else {
				require.NoError(t, p.Declare(t.Context(), Pipeline{"name": "stand", "driver": "memory"}))
				require.Len(t, jc.drivers, 1)
				initialDrv = jc.drivers[0]
			}

			if tc.resumeBeforeRestart {
				require.NoError(t, p.Resume(t.Context(), "stand"))
			}
			if tc.pauseBeforeRestart {
				require.NoError(t, p.Pause(t.Context(), "stand"))
			}

			initialDrv.state = &jobsApi.State{Pipeline: "stand", Driver: "memory", Ready: tc.readyAtFailure}

			errCh, join := startCommands(t, p)
			p.eventsCh <- driverCommand(t, "stand", restartSrt)
			join()

			assert.Empty(t, errCh)
			assert.Equal(t, []string{"stand"}, p.List())
			assert.Equal(t, 1, initialDrv.Stops())
			require.Len(t, jc.drivers, 2)

			replacementDrv := jc.drivers[1]
			assert.Equal(t, tc.wantReplacementRuns, replacementDrv.Runs())
			assert.Empty(t, replacementDrv.Resumed())
		})
	}
}

func TestDestroyResetsIntendedActiveState(t *testing.T) {
	p := newTestPlugin(t, &Config{})
	jc := &seqConstructor{name: "memory"}
	p.jobConstructors["memory"] = jc

	require.NoError(t, p.Declare(t.Context(), Pipeline{"name": "stand", "driver": "memory"}))
	require.NoError(t, p.Resume(t.Context(), "stand"))
	require.NoError(t, p.Destroy(t.Context(), "stand"))

	require.NoError(t, p.Declare(t.Context(), Pipeline{"name": "stand", "driver": "memory"}))
	require.Len(t, jc.drivers, 2)
	assert.Zero(t, jc.drivers[1].Runs())
}
