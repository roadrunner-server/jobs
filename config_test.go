package jobs

import (
	"runtime"
	"testing"
	"time"

	poolImpl "github.com/roadrunner-server/pool/v2/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigInitDefaultsRejectsPoolAndPools(t *testing.T) {
	c := &Config{
		Pool:  &poolImpl.Config{NumWorkers: 2},
		Pools: map[string]*poolImpl.Config{"default": {NumWorkers: 2}},
	}

	require.Error(t, c.InitDefaults())
}

// The number of pollers is derived from the worker count and never read from the
// config, whichever of the three pool shapes is used.
func TestConfigInitDefaultsNumPollers(t *testing.T) {
	tests := []struct {
		name       string
		cfg        *Config
		numPollers int
	}{
		{
			name:       "no pool configured",
			cfg:        &Config{},
			numPollers: runtime.NumCPU() + 2,
		},
		{
			name:       "pool with workers",
			cfg:        &Config{Pool: &poolImpl.Config{NumWorkers: 4}},
			numPollers: 6,
		},
		{
			// the worker count is read before the pool defaults are applied, so an
			// omitted num_workers counts as zero here
			name:       "pool without workers",
			cfg:        &Config{Pool: &poolImpl.Config{}},
			numPollers: 2,
		},
		{
			name: "named pools",
			cfg: &Config{Pools: map[string]*poolImpl.Config{
				"default": {NumWorkers: 3},
				"high":    {NumWorkers: 7},
			}},
			numPollers: 12,
		},
		{
			name:       "value from the config is discarded",
			cfg:        &Config{NumPollers: 42, Pool: &poolImpl.Config{NumWorkers: 1}},
			numPollers: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, tt.cfg.InitDefaults())
			assert.Equal(t, tt.numPollers, tt.cfg.NumPollers)
		})
	}
}

// A config without a pool section gets one, initialized by the pool package
// rather than left at its zero value.
func TestConfigInitDefaultsAllocatesPool(t *testing.T) {
	c := &Config{}

	require.NoError(t, c.InitDefaults())

	require.NotNil(t, c.Pool)
	assert.NotZero(t, c.Pool.NumWorkers)
	assert.NotZero(t, c.Pool.AllocateTimeout)
}

// Every named pool is initialized, and the values it sets are kept.
func TestConfigInitDefaultsAppliesPoolDefaults(t *testing.T) {
	c := &Config{Pools: map[string]*poolImpl.Config{
		"default": {NumWorkers: 3},
		"high":    {},
	}}

	require.NoError(t, c.InitDefaults())

	assert.EqualValues(t, 3, c.Pools["default"].NumWorkers)
	assert.NotZero(t, c.Pools["default"].DestroyTimeout)
	assert.NotZero(t, c.Pools["high"].NumWorkers)
}

func TestConfigInitDefaultsValues(t *testing.T) {
	tests := []struct {
		name            string
		cfg             *Config
		parallelism     int
		pipelineSize    uint64
		timeout         int
		timeoutDuration time.Duration
	}{
		{
			name:            "empty config",
			cfg:             &Config{},
			parallelism:     10,
			pipelineSize:    1_000_000,
			timeout:         60,
			timeoutDuration: time.Second * 60,
		},
		{
			name:            "zero parallelism",
			cfg:             &Config{CfgOptions: &CfgOptions{}},
			parallelism:     5,
			pipelineSize:    1_000_000,
			timeout:         60,
			timeoutDuration: time.Second * 60,
		},
		{
			name:            "explicit values are kept",
			cfg:             &Config{CfgOptions: &CfgOptions{Parallelism: 2}, PipelineSize: 15, Timeout: 3},
			parallelism:     2,
			pipelineSize:    15,
			timeout:         3,
			timeoutDuration: time.Second * 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, tt.cfg.InitDefaults())

			assert.Equal(t, tt.parallelism, tt.cfg.CfgOptions.Parallelism)
			assert.Equal(t, tt.pipelineSize, tt.cfg.PipelineSize)
			assert.Equal(t, tt.timeout, tt.cfg.Timeout)
			assert.Equal(t, tt.timeoutDuration, tt.cfg.TimeoutDuration())
		})
	}
}

// Every pipeline is stamped with its own map key as the name, a priority as
// int64, and the pool name when one is set.
func TestConfigInitDefaultsStampsPipelines(t *testing.T) {
	c := &Config{Pipelines: map[string]Pipeline{
		"with-pool":     {driver: "memory", pool: "high"},
		"with-priority": {driver: "memory", priority: 5},
		"bare":          {driver: "memory"},
	}}

	require.NoError(t, c.InitDefaults())

	assert.Equal(t, "with-pool", c.Pipelines["with-pool"].Name())
	assert.Equal(t, "high", c.Pipelines["with-pool"].Get(pool))
	assert.Equal(t, int64(10), c.Pipelines["with-pool"].Get(priority))

	assert.Equal(t, int64(5), c.Pipelines["with-priority"].Get(priority))

	assert.Equal(t, "bare", c.Pipelines["bare"].Name())
	assert.Nil(t, c.Pipelines["bare"].Get(pool))
}
