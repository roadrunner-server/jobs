package jobs

import (
	"context"
	stderr "errors"
	"testing"
	"time"

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
