package tests

import (
	"testing"

	"tests/helpers"

	"github.com/roadrunner-server/jobs/v6"
	"github.com/roadrunner-server/memory/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/require"
)

func jobsPlugins() []any {
	return []any{
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&memory.Plugin{},
	}
}

// A single pool and named pools describe two different runtimes, so the plugin
// refuses to start with both.
func TestPoolAndPoolsAreExclusive(t *testing.T) {
	err := helpers.StartExpectInitError(t, "configs/.rr-jobs-pool-and-pools.yaml", jobsPlugins())
	require.ErrorContains(t, err, "both pool and pools options cannot be set at the same time")
}
