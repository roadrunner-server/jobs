package jobs

import (
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

// A pipeline the processor cannot rebuild takes RoadRunner down.
func TestReadCommandsRestartFailureStopsTheServer(t *testing.T) {
	p := newTestPlugin(t, &Config{})
	jc := &fakeConstructor{name: "memory", driver: &fakeDriver{}, initErr: stderr.New("connect failed")}
	p.jobConstructors["memory"] = jc

	pipe := Pipeline{"name": "test-1-memory", "driver": "memory"}
	pipe.With(createdWithConfig, "jobs.pipelines.test-1-memory.config")
	p.pipelines.Store("test-1-memory", pipe)
	p.consumers.Store("test-1-memory", jobsApi.Driver(jc.driver))

	errCh, _ := startCommands(t, p)

	p.eventsCh <- driverCommand(t, "test-1-memory", restartSrt)

	err := <-errCh
	require.ErrorContains(t, err, "connect failed")
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
