package tests

import (
	"testing"

	"tests/helpers"

	"github.com/roadrunner-server/informer/v6"
	"github.com/roadrunner-server/jobs/v6"
	"github.com/roadrunner-server/memory/v6"
	"github.com/roadrunner-server/resetter/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/require"
)

// Three pipelines, each bound to its own worker pool by the `pool` key.
func TestJobsPools(t *testing.T) {
	pipes := []string{"test-1-memory", "test-2-memory", "test-3-memory"}

	rr, stop := helpers.Start(t, "configs/.rr-pools.yaml", []any{
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&memory.Plugin{},
	}, helpers.WithObservedLogger(), helpers.WithConfigVersion("2025.2.0"), helpers.WithPipelinesReady(rpcAddr, len(pipes)))

	client := helpers.NewJobsClient(t, rpcAddr)

	// the informer reports the workers of every pool: 10 + 5 + 10
	requireWorkers(t, client, 25)

	for _, pipe := range pipes {
		helpers.Push(t, client, pipe, []byte(pipe))
	}

	helpers.WaitLogged(t, rr.Logs, "job was processed successfully", len(pipes))
	helpers.DestroyPipelines(t, client, pipes...)

	stop()

	require.Equal(t, len(pipes), rr.Logs.FilterMessageSnippet("pipeline was started").Len())
	require.Equal(t, len(pipes), rr.Logs.FilterMessageSnippet("pipeline was stopped").Len())
	require.Equal(t, len(pipes), rr.Logs.FilterMessageSnippet("job processing was started").Len())
	require.Equal(t, len(pipes), rr.Logs.FilterMessageSnippet("job was processed successfully").Len())
	require.Zero(t, rr.Logs.FilterMessageSnippet("invalid worker pool name").Len())
}
