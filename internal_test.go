package jobs

import (
	"context"
	"log/slog"
	"testing"

	jobsApi "github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/goridge/v4/pkg/frame"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPluginPipelineExists(t *testing.T) {
	t.Run("unknown pipeline", func(t *testing.T) {
		p := &Plugin{log: slog.New(slog.DiscardHandler)}

		_, _, err := p.pipelineExists("test-1-memory")
		require.ErrorContains(t, err, "no such pipeline")
	})

	t.Run("nil pipeline", func(t *testing.T) {
		p := &Plugin{log: slog.New(slog.DiscardHandler)}
		p.pipelines.Store("test-1-memory", nil)

		_, _, err := p.pipelineExists("test-1-memory")
		require.ErrorContains(t, err, "no pipe registered")
	})

	t.Run("pipeline without a driver", func(t *testing.T) {
		p := &Plugin{log: slog.New(slog.DiscardHandler)}
		p.pipelines.Store("test-1-memory", Pipeline{name: "test-1-memory", driver: "memory"})

		_, _, err := p.pipelineExists("test-1-memory")
		require.ErrorContains(t, err, "driver for the pipeline not found")
	})

	t.Run("registered pipeline", func(t *testing.T) {
		p := &Plugin{log: slog.New(slog.DiscardHandler)}
		pipe := Pipeline{name: "test-1-memory", driver: "memory"}
		drv := &fakeDriver{}
		p.pipelines.Store("test-1-memory", pipe)
		p.consumers.Store("test-1-memory", jobsApi.Driver(drv))

		d, ppl, err := p.pipelineExists("test-1-memory")
		require.NoError(t, err)
		assert.Same(t, drv, d)
		assert.Equal(t, "test-1-memory", ppl.Name())
	})
}

// Payloads are pooled, so putPayload has to clear every field the listener set.
func TestPluginPayloadPool(t *testing.T) {
	p := newTestPlugin(t, &Config{})

	pld := p.payload([]byte("body"), []byte("context"))
	assert.Equal(t, []byte("body"), pld.Body)
	assert.Equal(t, []byte("context"), pld.Context)
	assert.Equal(t, frame.CodecRaw, pld.Codec)

	pld.Flags = frame.STREAM
	p.putPayload(pld)

	assert.Nil(t, pld.Body)
	assert.Nil(t, pld.Context)
	assert.Zero(t, pld.Codec)
	assert.Zero(t, pld.Flags)
}

func TestPluginWaitPollersFinish(t *testing.T) {
	t.Run("returns when the pollers finish", func(t *testing.T) {
		p := &Plugin{log: slog.New(slog.DiscardHandler)}
		p.pollersWg.Add(1)

		go p.pollersWg.Done()

		requireReturns(t, func() { p.waitPollersFinish(t.Context()) })
	})

	// A poller that never finishes is bounded by the context Stop passes in.
	t.Run("returns on a canceled context", func(t *testing.T) {
		p := &Plugin{log: slog.New(slog.DiscardHandler)}
		p.pollersWg.Add(1)
		defer p.pollersWg.Done()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		requireReturns(t, func() { p.waitPollersFinish(ctx) })
	})
}
