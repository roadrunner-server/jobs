package tests

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"tests/helpers"

	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v1"
	jobsApi "github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/events"
	"github.com/roadrunner-server/jobs/v6"
	"github.com/roadrunner-server/memory/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/require"
)

type restartMemory struct {
	memory.Plugin
	created atomic.Uint32
}

func (m *restartMemory) DriverFromConfig(ctx context.Context, key string, queue jobsApi.Queue, pipe jobsApi.Pipeline) (jobsApi.Driver, error) {
	d, err := m.Plugin.DriverFromConfig(ctx, key, queue, pipe)
	if err == nil {
		m.created.Add(1)
	}
	return d, err
}

func (m *restartMemory) DriverFromPipeline(ctx context.Context, pipe jobsApi.Pipeline, queue jobsApi.Queue) (jobsApi.Driver, error) {
	d, err := m.Plugin.DriverFromPipeline(ctx, pipe, queue)
	if err == nil {
		m.created.Add(1)
	}
	return d, err
}

func TestPipelineRestartRetainsRuntimeState(t *testing.T) {
	tests := []struct {
		name       string
		config     string
		address    string
		configured bool
		consume    bool
		paused     bool
	}{
		{
			name:    "declared resumed",
			config:  "configs/.rr-jobs-restart-declared-resumed.yaml",
			address: "127.0.0.1:6385",
		},
		{
			name:       "configured resumed",
			config:     "configs/.rr-jobs-restart-configured-resumed.yaml",
			address:    "127.0.0.1:6386",
			configured: true,
		},
		{
			name:    "declared paused",
			config:  "configs/.rr-jobs-restart-declared-paused.yaml",
			address: "127.0.0.1:6387",
			consume: true,
			paused:  true,
		},
		{
			name:       "configured paused",
			config:     "configs/.rr-jobs-restart-configured-paused.yaml",
			address:    "127.0.0.1:6388",
			configured: true,
			consume:    true,
			paused:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const pipeline = "test-recovery"
			constructor := &restartMemory{}
			rr, _ := helpers.Start(t, tt.config, []any{
				&server.Plugin{},
				&rpcPlugin.Plugin{},
				&jobs.Plugin{},
				constructor,
			}, helpers.WithObservedLogger(), helpers.WithRPCProbe(tt.address))
			client := helpers.NewJobsClient(t, tt.address)
			if !tt.configured {
				helpers.Declare(t, client, map[string]string{
					"name":     pipeline,
					"driver":   "memory",
					"prefetch": "100",
				})
			}
			if !tt.consume {
				helpers.Resume(t, client, pipeline)
			}
			if tt.paused {
				helpers.Pause(t, client, pipeline)
			}
			require.Equal(t, uint32(1), constructor.created.Load())

			bus, _ := events.NewEventBus()
			bus.Send(events.NewEvent(events.EventJOBSDriverCommand, pipeline, "restart"))
			require.Eventually(t, func() bool {
				if constructor.created.Load() != 2 {
					return false
				}
				registered := &jobsProto.Pipelines{}
				if err := client.Call("jobs.List", &jobsProto.Empty{}, registered); err != nil || !slices.Contains(registered.GetPipelines(), pipeline) {
					return false
				}
				out := &jobsProto.Stats{}
				if err := client.Call("jobs.Stat", &jobsProto.Empty{}, out); err != nil {
					return false
				}
				for _, stat := range out.GetStats() {
					if stat.GetPipeline() == pipeline {
						return stat.GetReady() == !tt.paused
					}
				}
				return false
			}, time.Second*10, time.Millisecond*10, "the replacement driver lost its runtime consumption state")

			helpers.Push(t, client, pipeline, []byte("job"))
			if tt.paused {
				require.Never(t, func() bool {
					return rr.Logs.FilterMessageSnippet("job was processed successfully").Len() > 0
				}, time.Second, time.Millisecond*20)
				helpers.Resume(t, client, pipeline)
			}
			helpers.WaitLogged(t, rr.Logs, "job was processed successfully", 1)
			helpers.DestroyPipelines(t, client, pipeline)
		})
	}
}
