package tests

import (
	"net/rpc"
	"testing"

	"tests/helpers"

	"github.com/roadrunner-server/informer/v6"
	"github.com/roadrunner-server/jobs/v6"
	"github.com/roadrunner-server/memory/v6"
	"github.com/roadrunner-server/pool/v2/state/process"
	"github.com/roadrunner-server/resetter/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/require"
)

// workersList is the reply of the informer.Workers rpc call.
type workersList struct {
	Workers []process.State `json:"workers"`
}

func TestJobsWorkersAndReset(t *testing.T) {
	rr, _ := helpers.Start(t, "configs/.rr-jobs-memory.yaml", []any{
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&memory.Plugin{},
		&informer.Plugin{},
		&resetter.Plugin{},
	}, helpers.WithObservedLogger(), helpers.WithPipelinesReady(rpcAddr, 1))

	client := helpers.NewJobsClient(t, rpcAddr)

	pids := requireWorkers(t, client, 2)

	var added bool
	require.NoError(t, client.Call("informer.AddWorker", "jobs", &added))
	requireWorkers(t, client, 3)

	var removed bool
	require.NoError(t, client.Call("informer.RemoveWorker", "jobs", &removed))
	requireWorkers(t, client, 2)

	var services []string
	require.NoError(t, client.Call("resetter.List", nil, &services))
	require.Contains(t, services, "jobs")

	var reset bool
	require.NoError(t, client.Call("resetter.Reset", "jobs", &reset))
	require.True(t, reset)

	// the pool is replaced, so none of the workers survives the reset
	for _, pid := range requireWorkers(t, client, 2) {
		require.NotContains(t, pids, pid)
	}

	// the fresh workers still consume
	helpers.Push(t, client, "test-consumed", []byte("job"))
	helpers.WaitLogged(t, rr.Logs, "job was processed successfully", 1)
}

// requireWorkers requires the jobs pools to hold exactly n workers and returns
// their pids.
func requireWorkers(t *testing.T, client *rpc.Client, n int) []int64 {
	t.Helper()

	var list workersList
	require.NoError(t, client.Call("informer.Workers", "jobs", &list))
	require.Len(t, list.Workers, n)

	pids := make([]int64, 0, len(list.Workers))
	for _, w := range list.Workers {
		pids = append(pids, w.Pid)
	}

	return pids
}
