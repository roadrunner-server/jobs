package tests

import (
	"context"
	"errors"
	"testing"

	"tests/helpers"

	jobsApi "github.com/roadrunner-server/api-plugins/v6/jobs"
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

// https://github.com/roadrunner-server/roadrunner/issues/2378
// A driver that fails to construct makes Serve stop the processor on its error
// path; stopping the container afterwards stops the processor again, which used
// to panic on a double channel close.
func TestIssue2378(t *testing.T) {
	err := helpers.StartExpectServeError(t, "", []any{
		&server.Plugin{},
		&jobs.Plugin{},
		&brokenDriver{},
	}, helpers.WithInlineConfig(`
version: '3'

server:
  command: "php php_test_files/jobs/jobs_ok.php"
  relay: "pipes"

jobs:
  pool:
    num_workers: 1
  pipelines:
    broken:
      driver: broken
      config: {}
  consume: [ "broken" ]
`))

	require.ErrorContains(t, err, "the broken driver cannot be constructed")
}

// brokenDriver is a jobs driver constructor that always fails, standing in for
// a driver whose backend is unreachable at boot.
type brokenDriver struct{}

func (b *brokenDriver) Init() error { return nil }

func (b *brokenDriver) Name() string { return "broken" }

func (b *brokenDriver) DriverFromConfig(context.Context, string, jobsApi.Queue, jobsApi.Pipeline) (jobsApi.Driver, error) {
	return nil, errors.New("the broken driver cannot be constructed")
}

func (b *brokenDriver) DriverFromPipeline(context.Context, jobsApi.Pipeline, jobsApi.Queue) (jobsApi.Driver, error) {
	return nil, errors.New("the broken driver cannot be constructed")
}
