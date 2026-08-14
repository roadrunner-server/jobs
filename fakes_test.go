package jobs

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	jobsApi "github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/pool/v2/payload"
	staticPool "github.com/roadrunner-server/pool/v2/pool/static_pool"
	"github.com/roadrunner-server/pool/v2/worker"
	"github.com/stretchr/testify/require"
)

// callTimeout bounds a call a test expects to return on its own.
const callTimeout = time.Second * 10

// fakePool records what the plugin asks of a worker pool. Exec hands back the
// channel the caller is given, a nil one standing for a worker that never
// answers.
type fakePool struct {
	workers []*worker.Process

	execCh    chan *staticPool.PExec
	execErr   error
	addErr    error
	removeErr error
	resetErr  error

	execs     int
	added     int
	removed   int
	reset     int
	destroyed int
}

func (f *fakePool) Workers() []*worker.Process { return f.workers }

func (f *fakePool) Exec(_ context.Context, _ *payload.Payload, _ chan struct{}) (chan *staticPool.PExec, error) {
	f.execs++
	return f.execCh, f.execErr
}

func (f *fakePool) AddWorker() error {
	f.added++
	return f.addErr
}

func (f *fakePool) RemoveWorker(_ context.Context) error {
	f.removed++
	return f.removeErr
}

func (f *fakePool) Reset(_ context.Context) error {
	f.reset++
	return f.resetErr
}

func (f *fakePool) Destroy(_ context.Context) { f.destroyed++ }

// fakeDriver stands in for a queue driver (amqp, memory, ...). One driver serves
// every pipeline of a test, and the rpc destroys them concurrently, so the
// recorded calls are guarded.
type fakeDriver struct {
	state *jobsApi.State

	pushErr   error
	runErr    error
	stopErr   error
	pauseErr  error
	resumeErr error
	stateErr  error

	mu      sync.Mutex
	pushed  []jobsApi.Message
	runs    int
	stops   int
	paused  []string
	resumed []string
}

func (f *fakeDriver) Push(_ context.Context, msg jobsApi.Message) error {
	f.mu.Lock()
	f.pushed = append(f.pushed, msg)
	f.mu.Unlock()

	return f.pushErr
}

func (f *fakeDriver) Run(_ context.Context, _ jobsApi.Pipeline) error {
	f.mu.Lock()
	f.runs++
	f.mu.Unlock()

	return f.runErr
}

func (f *fakeDriver) Stop(_ context.Context) error {
	f.mu.Lock()
	f.stops++
	f.mu.Unlock()

	return f.stopErr
}

func (f *fakeDriver) Pause(_ context.Context, pipeline string) error {
	f.mu.Lock()
	f.paused = append(f.paused, pipeline)
	f.mu.Unlock()

	return f.pauseErr
}

func (f *fakeDriver) Resume(_ context.Context, pipeline string) error {
	f.mu.Lock()
	f.resumed = append(f.resumed, pipeline)
	f.mu.Unlock()

	return f.resumeErr
}

func (f *fakeDriver) State(_ context.Context) (*jobsApi.State, error) {
	if f.stateErr != nil {
		return nil, f.stateErr
	}
	return f.state, nil
}

// Pushed returns the jobs the driver accepted.
func (f *fakeDriver) Pushed() []jobsApi.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]jobsApi.Message(nil), f.pushed...)
}

// Runs returns how many times a pipeline was started on the driver.
func (f *fakeDriver) Runs() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runs
}

// Stops returns how many times the driver was stopped.
func (f *fakeDriver) Stops() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stops
}

// Paused returns the pipelines the driver was asked to pause.
func (f *fakeDriver) Paused() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.paused...)
}

// Resumed returns the pipelines the driver was asked to resume.
func (f *fakeDriver) Resumed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.resumed...)
}

// fakeJob is a queue message the listener hands to a worker pool. It counts the
// protocol calls the plugin makes on it.
type fakeJob struct {
	id   string
	body []byte

	acks  int
	nacks int
}

func (f *fakeJob) ID() string                   { return f.id }
func (f *fakeJob) GroupID() string              { return "" }
func (f *fakeJob) Priority() int64              { return 1 }
func (f *fakeJob) Body() []byte                 { return f.body }
func (f *fakeJob) Headers() map[string][]string { return nil }
func (f *fakeJob) Context() ([]byte, error)     { return nil, nil }

func (f *fakeJob) Ack() error {
	f.acks++
	return nil
}

func (f *fakeJob) Nack() error {
	f.nacks++
	return nil
}

func (f *fakeJob) NackWithOptions(_ bool, _ int) error {
	f.nacks++
	return nil
}

func (f *fakeJob) Requeue(_ map[string][]string, _ int) error { return nil }

// fakeConstructor is the endure-collected driver factory.
type fakeConstructor struct {
	name    string
	driver  *fakeDriver
	initErr error

	fromConfig   int
	fromPipeline int
}

func (f *fakeConstructor) Name() string { return f.name }

func (f *fakeConstructor) DriverFromConfig(_ context.Context, _ string, _ jobsApi.Queue, _ jobsApi.Pipeline) (jobsApi.Driver, error) {
	f.fromConfig++
	if f.initErr != nil {
		return nil, f.initErr
	}
	return f.driver, nil
}

func (f *fakeConstructor) DriverFromPipeline(_ context.Context, _ jobsApi.Pipeline, _ jobsApi.Queue) (jobsApi.Driver, error) {
	f.fromPipeline++
	if f.initErr != nil {
		return nil, f.initErr
	}
	return f.driver, nil
}

// stubConfigurer feeds Init a config without going through a file.
type stubConfigurer struct {
	cfg          *Config
	has          bool
	err          error
	experimental bool
}

func (s *stubConfigurer) Has(string) bool    { return s.has }
func (s *stubConfigurer) Experimental() bool { return s.experimental }
func (s *stubConfigurer) UnmarshalKey(_ string, out any) error {
	if s.err != nil {
		return s.err
	}

	if dst, ok := out.(**Config); ok {
		*dst = s.cfg
	}

	return nil
}

type stubLogger struct{}

func (stubLogger) NamedLogger(string) *slog.Logger { return slog.New(slog.DiscardHandler) }

// newTestPlugin initializes a plugin over the given config. The pipeline
// processor started by Init owns goroutines, so it is stopped with the test.
func newTestPlugin(t *testing.T, cfg *Config) *Plugin {
	t.Helper()

	p := &Plugin{}
	if err := p.Init(&stubConfigurer{cfg: cfg, has: true}, stubLogger{}, nil); err != nil {
		t.Fatalf("plugin init: %v", err)
	}

	t.Cleanup(p.jobsProcessor.stop)

	return p
}

// requireReturns fails the test when fn is still running after callTimeout, so a
// call that stops returning is named instead of hanging until the package
// timeout fires.
func requireReturns(t *testing.T, fn func()) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()

	timer := time.NewTimer(callTimeout)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		require.FailNow(t, "the call did not return", "waited %s", callTimeout)
	}
}
