package tests

import (
	"testing"
	"time"

	"tests/helpers"

	"github.com/roadrunner-server/jobs/v6"
	"github.com/roadrunner-server/memory/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/require"
)

// Declare, list, push, stat, pause, resume and destroy over rpc, against the
// memory driver so no broker is involved.
func TestPipelineLifecycle(t *testing.T) {
	rr, _ := helpers.Start(t, "configs/.rr-jobs-memory.yaml", []any{
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&memory.Plugin{},
	}, helpers.WithObservedLogger(), helpers.WithPipelinesReady(rpcAddr, 1))

	client := helpers.NewJobsClient(t, rpcAddr)
	require.Equal(t, []string{"test-consumed"}, helpers.ListPipelines(t, client))

	// a pipeline declared at runtime is registered, but only the pipelines listed
	// under `consume` are started
	helpers.Declare(t, client, map[string]string{
		"driver":   "memory",
		"name":     "test-declared",
		"priority": "3",
		"prefetch": "100",
	})
	require.ElementsMatch(t, []string{"test-consumed", "test-declared"}, helpers.ListPipelines(t, client))

	declared := helpers.Stat(t, client, "test-declared")
	require.Equal(t, "memory", declared.GetDriver())
	require.EqualValues(t, 3, declared.GetPriority())
	require.False(t, declared.GetReady())

	helpers.Resume(t, client, "test-declared")
	require.True(t, helpers.Stat(t, client, "test-declared").GetReady())

	helpers.Push(t, client, "test-declared", []byte("job"))
	helpers.WaitLogged(t, rr.Logs, "job was processed successfully", 1)

	// a paused driver holds the job instead of handing it to a worker
	helpers.Pause(t, client, "test-declared")
	require.False(t, helpers.Stat(t, client, "test-declared").GetReady())

	helpers.Push(t, client, "test-declared", []byte("job"))
	require.Never(t, func() bool {
		return rr.Logs.FilterMessageSnippet("job was processed successfully").Len() > 1
	}, time.Second, time.Millisecond*50, "the job was processed while the pipeline was paused")

	helpers.Resume(t, client, "test-declared")
	helpers.WaitLogged(t, rr.Logs, "job was processed successfully", 2)

	helpers.DestroyPipelines(t, client, "test-declared", "test-consumed")
	require.Empty(t, helpers.ListPipelines(t, client))
}
