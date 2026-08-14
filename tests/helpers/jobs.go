package helpers

import (
	"net"
	"net/rpc"
	"slices"
	"testing"
	"time"

	mocklogger "tests/mock"

	"github.com/google/uuid"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v1"
	goridgeRpc "github.com/roadrunner-server/goridge/v4/pkg/rpc"
	"github.com/stretchr/testify/require"
)

const (
	// callTimeout caps the polling helpers that retry an rpc call or wait for a
	// log record; it only bounds the failure path.
	callTimeout = time.Second * 60
	callTick    = time.Millisecond * 50
)

// NewJobsClient dials the rpc plugin at address. The client, and with it the
// connection, is closed by t.Cleanup.
func NewJobsClient(t *testing.T, address string) *rpc.Client {
	t.Helper()

	var d net.Dialer
	conn, err := d.DialContext(t.Context(), "tcp", address)
	require.NoError(t, err)

	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	t.Cleanup(func() { _ = client.Close() })

	return client
}

// Push sends a single job to the pipeline.
func Push(t *testing.T, client *rpc.Client, pipeline string, payload []byte) {
	t.Helper()

	req := &jobsProto.PushRequest{Job: newJob(pipeline, payload)}
	require.NoError(t, client.Call("jobs.Push", req, &jobsProto.Empty{}))
}

// PushDelayed sends a job the driver may only hand over after delay seconds.
func PushDelayed(t *testing.T, client *rpc.Client, pipeline string, delay int64) {
	t.Helper()

	job := newJob(pipeline, []byte(`{"hello":"world"}`))
	job.Options.Delay = delay

	require.NoError(t, client.Call("jobs.Push", &jobsProto.PushRequest{Job: job}, &jobsProto.Empty{}))
}

// PushBatch sends count identical jobs to the pipeline in one call.
func PushBatch(t *testing.T, client *rpc.Client, pipeline string, count int, payload []byte) {
	t.Helper()

	batch := make([]*jobsProto.Job, count)
	for i := range count {
		batch[i] = newJob(pipeline, payload)
	}

	require.NoError(t, client.Call("jobs.PushBatch", &jobsProto.PushBatchRequest{Jobs: batch}, &jobsProto.Empty{}))
}

// Declare registers a pipeline at runtime. The map carries the driver name and
// the driver options, as the PHP client sends them.
func Declare(t *testing.T, client *rpc.Client, pipeline map[string]string) {
	t.Helper()

	req := &jobsProto.DeclareRequest{Pipeline: pipeline}
	require.NoError(t, client.Call("jobs.Declare", req, &jobsProto.Empty{}))
}

// Resume starts consuming the pipelines.
func Resume(t *testing.T, client *rpc.Client, pipes ...string) {
	t.Helper()

	req := &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}
	require.NoError(t, client.Call("jobs.Resume", req, &jobsProto.Empty{}))
}

// Pause stops consuming the pipelines; pushes to them are still accepted.
func Pause(t *testing.T, client *rpc.Client, pipes ...string) {
	t.Helper()

	req := &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}
	require.NoError(t, client.Call("jobs.Pause", req, &jobsProto.Empty{}))
}

// ListPipelines returns the names of the registered pipelines.
func ListPipelines(t *testing.T, client *rpc.Client) []string {
	t.Helper()

	out := &jobsProto.Pipelines{}
	require.NoError(t, client.Call("jobs.List", &jobsProto.Empty{}, out))

	return out.GetPipelines()
}

// Stat returns the driver state of the named pipeline.
func Stat(t *testing.T, client *rpc.Client, pipeline string) *jobsProto.Stat {
	t.Helper()

	out := &jobsProto.Stats{}
	require.NoError(t, client.Call("jobs.Stat", &jobsProto.Empty{}, out))

	for _, st := range out.GetStats() {
		if st.GetPipeline() == pipeline {
			return st
		}
	}

	require.Failf(t, "pipeline is missing from the stats", "pipeline: %s", pipeline)
	return nil
}

// DestroyPipelines stops the pipelines and removes them from the plugin. A
// driver that is still starting rejects the call, so it is retried.
func DestroyPipelines(t *testing.T, client *rpc.Client, pipes ...string) {
	t.Helper()

	req := &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}
	require.Eventually(t, func() bool {
		return client.Call("jobs.Destroy", req, &jobsProto.Pipelines{}) == nil
	}, callTimeout, callTick, "pipelines were not destroyed: %v", pipes)
}

// WaitLogged waits until at least n records containing snippet were logged.
func WaitLogged(t *testing.T, logs *mocklogger.ObservedLogs, snippet string, n int) {
	t.Helper()

	require.Eventually(t, func() bool {
		return logs.FilterMessageSnippet(snippet).Len() >= n
	}, callTimeout, callTick, "expected %d records containing %q", n, snippet)
}

func newJob(pipeline string, payload []byte) *jobsProto.Job {
	return &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      uuid.NewString(),
		Payload: payload,
		Headers: map[string]*jobsProto.HeaderValue{"test": {Value: []string{"test2"}}},
		Options: &jobsProto.Options{
			Priority: 1,
			Pipeline: pipeline,
			Topic:    pipeline,
		},
	}
}
