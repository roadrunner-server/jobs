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
	cfg := `
version: '3'

rpc:
  listen: tcp://127.0.0.1:6381

server:
  command: "php php_test_files/jobs/jobs_ok.php"
  relay: "pipes"

jobs:
  pool:
    num_workers: 1
  pools:
    default:
      num_workers: 1
`

	err := helpers.StartExpectInitError(t, "", jobsPlugins(), helpers.WithInlineConfig(cfg))
	require.ErrorContains(t, err, "both pool and pools options cannot be set at the same time")
}
