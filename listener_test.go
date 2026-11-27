package jobs

import (
	"context"
	stderr "errors"
	"testing"
	"time"

	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/pool/v2/payload"
	staticPool "github.com/roadrunner-server/pool/v2/pool/static_pool"
	"github.com/roadrunner-server/pool/v2/worker"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"
)

// testSpan starts a span on the plugin tracer, the way the listener does before
// it dispatches a job.
func testSpan(p *Plugin) trace.Span {
	_, span := p.tracer.Tracer(PluginName).Start(context.Background(), "test")
	return span
}

// A pool that refuses the job is a protocol level error: the job is negatively
// acknowledged and counted as failed.
func TestPluginExecutePoolError(t *testing.T) {
	p := newTestPlugin(t, &Config{})
	pool := &fakePool{execErr: stderr.New("no free workers")}
	jb := &fakeJob{id: "job-1", body: []byte("body")}

	p.Execute([]byte("ctx"), pool, jb, testSpan(p), time.Now().UTC())

	assert.Equal(t, 1, pool.execs)
	assert.Equal(t, 1, jb.nacks)
	assert.Zero(t, jb.acks)
	assert.Equal(t, uint64(1), p.metrics.jobsErr.Load())
	assert.Zero(t, p.metrics.jobsOk.Load())
}

// A worker that never writes a response leaves Execute on its own timer, which
// nacks the job rather than blocking the poller forever.
func TestPluginExecuteNoResponse(t *testing.T) {
	p := newTestPlugin(t, &Config{})
	pool := &fakePool{}
	jb := &fakeJob{id: "job-1", body: []byte("body")}

	requireReturns(t, func() {
		p.Execute([]byte("ctx"), pool, jb, testSpan(p), time.Now().UTC())
	})

	assert.Equal(t, 1, pool.execs)
	assert.Equal(t, 1, jb.nacks)
	assert.Zero(t, jb.acks)
	assert.Equal(t, uint64(1), p.metrics.jobsErr.Load())
	assert.Zero(t, p.metrics.jobsOk.Load())
}

type seqExecPool struct {
	errs   []error
	onCall func(call int)
	calls  int
}

func (s *seqExecPool) Workers() []*worker.Process         { return nil }
func (s *seqExecPool) AddWorker() error                   { return nil }
func (s *seqExecPool) RemoveWorker(context.Context) error { return nil }
func (s *seqExecPool) Reset(context.Context) error        { return nil }
func (s *seqExecPool) Destroy(context.Context)            {}

func (s *seqExecPool) Exec(_ context.Context, _ *payload.Payload, _ chan struct{}) (chan *staticPool.PExec, error) {
	s.calls++
	if s.onCall != nil {
		s.onCall(s.calls)
	}
	if s.calls <= len(s.errs) {
		return nil, s.errs[s.calls-1]
	}

	return nil, errors.E(errors.Op("test_pool"), errors.SoftJob)
}

func TestPluginExecuteNoFreeWorkers(t *testing.T) {
	noFreeErr := errors.E(errors.Op("static_pool_exec"), errors.NoFreeWorkers)
	softJobErr := errors.E(errors.Op("static_pool_exec"), errors.SoftJob)

	tests := []struct {
		name          string
		errs          []error
		stopOnCall    int
		cancelOnCall  int
		wantExecCalls int
		wantNacks     int
		wantJobErrs   uint64
	}{
		{
			name:          "retries NoFreeWorkers until non-allocation result",
			errs:          []error{noFreeErr, noFreeErr, softJobErr},
			wantExecCalls: 3,
			wantNacks:     1,
			wantJobErrs:   1,
		},
		{
			name:          "stops retrying NoFreeWorkers when plugin is stopping",
			errs:          []error{noFreeErr, noFreeErr},
			stopOnCall:    1,
			wantExecCalls: 1,
			wantNacks:     1,
			wantJobErrs:   1,
		},
		{
			name:          "stops retrying NoFreeWorkers when exec context is canceled",
			errs:          []error{noFreeErr, noFreeErr},
			cancelOnCall:  1,
			wantExecCalls: 1,
			wantNacks:     1,
			wantJobErrs:   1,
		},
		{
			name:          "does not retry non-NoFreeWorkers pool error",
			errs:          []error{softJobErr, softJobErr},
			wantExecCalls: 1,
			wantNacks:     1,
			wantJobErrs:   1,
		},
	}

	for i := range tests {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			p := newTestPlugin(t, &Config{})
			pool := &seqExecPool{
				errs: tc.errs,
				onCall: func(call int) {
					if tc.stopOnCall == call {
						p.stopping.Store(true)
					}
					if tc.cancelOnCall == call {
						p.execCancel()
					}
				},
			}
			jb := &fakeJob{id: "job-1", body: []byte("body")}

			p.Execute([]byte("ctx"), pool, jb, testSpan(p), time.Now().UTC())

			assert.Equal(t, tc.wantExecCalls, pool.calls)
			assert.Equal(t, tc.wantNacks, jb.nacks)
			assert.Zero(t, jb.acks)
			assert.Equal(t, tc.wantJobErrs, p.metrics.jobsErr.Load())
		})
	}
}
