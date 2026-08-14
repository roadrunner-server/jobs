package tests

import (
	"testing"

	"tests/helpers"

	"github.com/google/uuid"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v1"
	"github.com/roadrunner-server/jobs/v6"
	"github.com/roadrunner-server/memory/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/require"
)

// The worker compares the traceparent header it receives with the one carried in
// the payload and errors the task when they differ, so a lost header turns into a
// protocol error record instead of a processed job.
func TestTracePropagation(t *testing.T) {
	const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	rr, _ := helpers.Start(t, "configs/.rr-trace-propagation.yaml", []any{
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&memory.Plugin{},
	}, helpers.WithObservedLogger(), helpers.WithPipelinesReady(rpcAddr, 1))

	client := helpers.NewJobsClient(t, rpcAddr)

	req := &jobsProto.PushRequest{Job: &jobsProto.Job{
		Job:     "test/trace",
		Id:      uuid.NewString(),
		Payload: []byte(`{"traceparent":"` + traceparent + `"}`),
		Headers: map[string]*jobsProto.HeaderValue{
			"traceparent": {Value: []string{traceparent}},
		},
		Options: &jobsProto.Options{
			Priority: 1,
			Pipeline: "test-trace",
		},
	}}
	require.NoError(t, client.Call("jobs.Push", req, &jobsProto.Empty{}))

	helpers.WaitLogged(t, rr.Logs, "job was processed successfully", 1)
	require.Zero(t, rr.Logs.FilterMessageSnippet("jobs protocol error").Len())

	helpers.DestroyPipelines(t, client, "test-trace")
}
