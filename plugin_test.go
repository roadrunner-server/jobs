package jobs

import (
	stderr "errors"
	"testing"

	jobsApi "github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/errors"
	poolImpl "github.com/roadrunner-server/pool/v2/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPluginInit(t *testing.T) {
	t.Run("without a jobs section", func(t *testing.T) {
		p := &Plugin{}

		err := p.Init(&stubConfigurer{}, stubLogger{}, nil)
		require.True(t, errors.Is(errors.Disabled, err))
	})

	t.Run("unmarshal error", func(t *testing.T) {
		p := &Plugin{}

		err := p.Init(&stubConfigurer{has: true, err: stderr.New("broken config")}, stubLogger{}, nil)
		require.ErrorContains(t, err, "broken config")
	})

	t.Run("invalid config", func(t *testing.T) {
		p := &Plugin{}
		cfg := &Config{
			Pool:  &poolImpl.Config{NumWorkers: 1},
			Pools: map[string]*poolImpl.Config{"default": {NumWorkers: 1}},
		}

		err := p.Init(&stubConfigurer{has: true, cfg: cfg}, stubLogger{}, nil)
		require.ErrorContains(t, err, "both pool and pools options cannot be set")
	})

	t.Run("ready to serve", func(t *testing.T) {
		p := newTestPlugin(t, &Config{Consume: []string{"test-1-memory"}})

		assert.Equal(t, PluginName, p.Name())
		assert.NotNil(t, p.metrics)
		assert.NotNil(t, p.queue)
		assert.Contains(t, p.consume, "test-1-memory")

		r, ok := p.RPC().(*rpc)
		require.True(t, ok)
		assert.Same(t, p, r.p)
	})
}

// Pipelines from the config are registered before Serve, which is what List
// reports.
func TestPluginInitRegistersConfigPipelines(t *testing.T) {
	p := newTestPlugin(t, &Config{Pipelines: map[string]Pipeline{
		"test-1-memory": {driver: "memory"},
		"test-2-memory": {driver: "memory"},
	}})

	assert.ElementsMatch(t, []string{"test-1-memory", "test-2-memory"}, p.List())
}

func TestPluginReset(t *testing.T) {
	t.Run("no pools configured", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})

		require.ErrorContains(t, p.Reset(), "no worker pools configured")
	})

	t.Run("single pool", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})
		fp := &fakePool{}
		p.workersPool = fp

		require.NoError(t, p.Reset())
		assert.Equal(t, 1, fp.reset)
	})

	t.Run("every named pool", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})
		first, second := &fakePool{}, &fakePool{}
		p.workersPools = map[string]Pool{"default": first, "high": second}

		require.NoError(t, p.Reset())
		assert.Equal(t, 1, first.reset)
		assert.Equal(t, 1, second.reset)
	})

	t.Run("pool errors are named and joined", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})
		p.workersPools = map[string]Pool{
			"default": &fakePool{resetErr: stderr.New("spawn failed")},
			"high":    &fakePool{resetErr: stderr.New("allocate failed")},
		}

		err := p.Reset()
		require.Error(t, err)
		assert.ErrorContains(t, err, "failed to reset pool default")
		assert.ErrorContains(t, err, "failed to reset pool high")
	})
}

func TestPluginWorkersWithoutPools(t *testing.T) {
	p := newTestPlugin(t, &Config{})

	assert.Nil(t, p.Workers())
}

func TestPluginDeclare(t *testing.T) {
	t.Run("pipeline without a driver", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})

		err := p.Declare(t.Context(), Pipeline{name: "test-1-memory"})
		require.ErrorContains(t, err, "no associated driver")
	})

	t.Run("pipeline already registered", func(t *testing.T) {
		p := newTestPlugin(t, &Config{Pipelines: map[string]Pipeline{"test-1-memory": {driver: "memory"}}})

		err := p.Declare(t.Context(), Pipeline{name: "test-1-memory", driver: "memory"})
		require.ErrorContains(t, err, "pipeline already exists")
	})

	t.Run("driver without a constructor", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})

		err := p.Declare(t.Context(), Pipeline{name: "test-1-amqp", driver: "amqp"})
		require.ErrorContains(t, err, "no constructor registered for driver")
	})

	t.Run("constructor error", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})
		p.jobConstructors["memory"] = &fakeConstructor{name: "memory", initErr: stderr.New("connect failed")}

		err := p.Declare(t.Context(), Pipeline{name: "test-1-memory", driver: "memory"})
		require.ErrorContains(t, err, "connect failed")
	})

	t.Run("declared pipeline is registered but not consumed", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})
		drv := &fakeDriver{}
		p.jobConstructors["memory"] = &fakeConstructor{name: "memory", driver: drv}

		pipe := Pipeline{name: "test-1-memory", driver: "memory"}
		require.NoError(t, p.Declare(t.Context(), pipe))

		assert.Equal(t, []string{"test-1-memory"}, p.List())
		assert.Zero(t, drv.Runs())
		assert.Equal(t, defaultPriority, pipe.Get(priority))
		assert.Equal(t, trueStr, pipe.Get(createdWithDeclare))
	})

	t.Run("pipeline listed under consume is started", func(t *testing.T) {
		p := newTestPlugin(t, &Config{Consume: []string{"test-1-memory"}})
		drv := &fakeDriver{}
		p.jobConstructors["memory"] = &fakeConstructor{name: "memory", driver: drv}

		require.NoError(t, p.Declare(t.Context(), Pipeline{name: "test-1-memory", driver: "memory", priority: "5"}))
		assert.Equal(t, 1, drv.Runs())
	})

	t.Run("run error", func(t *testing.T) {
		p := newTestPlugin(t, &Config{Consume: []string{"test-1-memory"}})
		p.jobConstructors["memory"] = &fakeConstructor{name: "memory", driver: &fakeDriver{runErr: stderr.New("consume failed")}}

		err := p.Declare(t.Context(), Pipeline{name: "test-1-memory", driver: "memory"})
		require.ErrorContains(t, err, "consume failed")
	})

	// an unparsable priority is logged and replaced by the default
	t.Run("malformed priority", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})
		p.jobConstructors["memory"] = &fakeConstructor{name: "memory", driver: &fakeDriver{}}

		pipe := Pipeline{name: "test-1-memory", driver: "memory", priority: "high"}
		require.NoError(t, p.Declare(t.Context(), pipe))
		assert.Equal(t, defaultPriority, pipe.Get(priority))
	})
}

func TestPluginDestroy(t *testing.T) {
	t.Run("unknown pipeline", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})

		require.ErrorContains(t, p.Destroy(t.Context(), "test-1-memory"), "no such pipeline")
	})

	t.Run("nil pipeline", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})
		p.pipelines.Store("test-1-memory", nil)

		require.ErrorContains(t, p.Destroy(t.Context(), "test-1-memory"), "no pipe registered")
	})

	t.Run("pipeline without a consumer", func(t *testing.T) {
		p := newTestPlugin(t, &Config{Pipelines: map[string]Pipeline{"test-1-memory": {driver: "memory", name: "test-1-memory"}}})

		require.ErrorContains(t, p.Destroy(t.Context(), "test-1-memory"), "consumer not registered")
	})

	t.Run("driver stop error", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})
		drv := &fakeDriver{stopErr: stderr.New("close failed")}
		p.jobConstructors["memory"] = &fakeConstructor{name: "memory", driver: drv}
		require.NoError(t, p.Declare(t.Context(), Pipeline{name: "test-1-memory", driver: "memory"}))

		require.ErrorContains(t, p.Destroy(t.Context(), "test-1-memory"), "close failed")
		assert.Empty(t, p.List())
	})

	t.Run("pipeline is stopped and removed", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})
		drv := &fakeDriver{}
		p.jobConstructors["memory"] = &fakeConstructor{name: "memory", driver: drv}
		require.NoError(t, p.Declare(t.Context(), Pipeline{name: "test-1-memory", driver: "memory"}))

		require.NoError(t, p.Destroy(t.Context(), "test-1-memory"))
		assert.Equal(t, 1, drv.Stops())
		assert.Empty(t, p.List())
	})
}

func TestPluginPush(t *testing.T) {
	t.Run("unknown pipeline", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})

		err := p.Push(t.Context(), &Job{Ident: "id-1", Options: &Options{Pipeline: "test-1-memory"}})
		require.ErrorContains(t, err, "no such pipeline")
	})

	t.Run("driver error", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})
		drv := &fakeDriver{pushErr: stderr.New("queue full")}
		p.jobConstructors["memory"] = &fakeConstructor{name: "memory", driver: drv}
		require.NoError(t, p.Declare(t.Context(), Pipeline{name: "test-1-memory", driver: "memory"}))

		err := p.Push(t.Context(), &Job{Ident: "id-1", Options: &Options{Pipeline: "test-1-memory"}})
		require.ErrorContains(t, err, "queue full")
	})

	// a job without a priority inherits the pipeline one, and the pool the
	// pipeline is bound to travels in the headers
	t.Run("job is stamped and pushed", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})
		drv := &fakeDriver{}
		p.jobConstructors["memory"] = &fakeConstructor{name: "memory", driver: drv}
		require.NoError(t, p.Declare(t.Context(), Pipeline{name: "test-1-memory", driver: "memory", priority: "7", pool: "high"}))

		job := &Job{Ident: "id-1", Hdr: map[string][]string{}, Options: &Options{Pipeline: "test-1-memory"}}
		require.NoError(t, p.Push(t.Context(), job))

		require.Len(t, drv.Pushed(), 1)
		assert.Equal(t, int64(7), job.Priority())
		assert.Equal(t, []string{"high"}, job.Headers()[pool])
	})
}

func TestPluginPushBatch(t *testing.T) {
	t.Run("unknown pipeline", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})

		err := p.PushBatch(t.Context(), []jobsApi.Message{&Job{Ident: "id-1", Options: &Options{Pipeline: "test-1-memory"}}})
		require.ErrorContains(t, err, "no such pipeline")
	})

	t.Run("stops on the first driver error", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})
		drv := &fakeDriver{pushErr: stderr.New("queue full")}
		p.jobConstructors["memory"] = &fakeConstructor{name: "memory", driver: drv}
		require.NoError(t, p.Declare(t.Context(), Pipeline{name: "test-1-memory", driver: "memory"}))

		err := p.PushBatch(t.Context(), []jobsApi.Message{
			&Job{Ident: "id-1", Options: &Options{Pipeline: "test-1-memory"}},
			&Job{Ident: "id-2", Options: &Options{Pipeline: "test-1-memory"}},
		})
		require.ErrorContains(t, err, "queue full")
		assert.Len(t, drv.Pushed(), 1)
	})

	t.Run("every job is pushed", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})
		drv := &fakeDriver{}
		p.jobConstructors["memory"] = &fakeConstructor{name: "memory", driver: drv}
		require.NoError(t, p.Declare(t.Context(), Pipeline{name: "test-1-memory", driver: "memory", priority: "7"}))

		jobs := []jobsApi.Message{
			&Job{Ident: "id-1", Options: &Options{Pipeline: "test-1-memory"}},
			&Job{Ident: "id-2", Options: &Options{Pipeline: "test-1-memory", Priority: 1}},
		}
		require.NoError(t, p.PushBatch(t.Context(), jobs))

		assert.Len(t, drv.Pushed(), 2)
		assert.Equal(t, int64(7), jobs[0].Priority())
		assert.Equal(t, int64(1), jobs[1].Priority())
	})
}

func TestPluginPauseResume(t *testing.T) {
	t.Run("unknown pipeline", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})

		require.Error(t, p.Pause(t.Context(), "test-1-memory"))
		require.Error(t, p.Resume(t.Context(), "test-1-memory"))
	})

	t.Run("calls are redirected to the driver", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})
		drv := &fakeDriver{}
		p.jobConstructors["memory"] = &fakeConstructor{name: "memory", driver: drv}
		require.NoError(t, p.Declare(t.Context(), Pipeline{name: "test-1-memory", driver: "memory"}))

		require.NoError(t, p.Pause(t.Context(), "test-1-memory"))
		require.NoError(t, p.Resume(t.Context(), "test-1-memory"))

		assert.Equal(t, []string{"test-1-memory"}, drv.Paused())
		assert.Equal(t, []string{"test-1-memory"}, drv.Resumed())
	})
}

func TestPluginJobsState(t *testing.T) {
	t.Run("state of every driver", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})
		drv := &fakeDriver{state: &jobsApi.State{Pipeline: "test-1-memory", Driver: "memory", Ready: true}}
		p.jobConstructors["memory"] = &fakeConstructor{name: "memory", driver: drv}
		require.NoError(t, p.Declare(t.Context(), Pipeline{name: "test-1-memory", driver: "memory"}))

		state, err := p.JobsState(t.Context())
		require.NoError(t, err)
		require.Len(t, state, 1)
		assert.Equal(t, "test-1-memory", state[0].Pipeline)
		assert.True(t, state[0].Ready)
	})

	t.Run("driver error", func(t *testing.T) {
		p := newTestPlugin(t, &Config{})
		drv := &fakeDriver{stateErr: stderr.New("broker unreachable")}
		p.jobConstructors["memory"] = &fakeConstructor{name: "memory", driver: drv}
		require.NoError(t, p.Declare(t.Context(), Pipeline{name: "test-1-memory", driver: "memory"}))

		_, err := p.JobsState(t.Context())
		require.ErrorContains(t, err, "broker unreachable")
	})
}
