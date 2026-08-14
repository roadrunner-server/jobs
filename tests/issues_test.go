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

// https://github.com/roadrunner-server/roadrunner/issues/2085
// server.on_init runs a script that calls the jobs rpc while the container is
// still booting, with exit_on_error set, so a container that serves and consumes
// afterwards is the assertion.
func TestIssue2085(t *testing.T) {
	// the on_init script opens an rpc connection through goridge
	helpers.RequirePHPExtension(t, "sockets")

	rr, _ := helpers.Start(t, "configs/.rr-issue2085.yaml", []any{
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&memory.Plugin{},
	}, helpers.WithObservedLogger(), helpers.WithPipelinesReady(rpcAddr, 1))

	client := helpers.NewJobsClient(t, rpcAddr)
	require.Equal(t, []string{"test-1-memory"}, helpers.ListPipelines(t, client))

	helpers.Push(t, client, "test-1-memory", []byte("job"))
	helpers.WaitLogged(t, rr.Logs, "job was processed successfully", 1)

	helpers.DestroyPipelines(t, client, "test-1-memory")
}
