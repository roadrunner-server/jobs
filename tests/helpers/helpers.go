package helpers

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"testing"
	"time"

	"github.com/google/uuid"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v2"
	jobState "github.com/roadrunner-server/api-plugins/v6/jobs"
	goridgeRpc "github.com/roadrunner-server/goridge/v4/pkg/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	push      string = "jobs.Push"
	pause     string = "jobs.Pause"
	pushBatch string = "jobs.PushBatch"
	destroy   string = "jobs.Destroy"
	resume    string = "jobs.Resume"
	stat      string = "jobs.Stat"
)

// newRPCClient creates a goridge RPC client connected to the given address.
// The connection is automatically closed when the test finishes.
func newRPCClient(t *testing.T, address string) *rpc.Client {
	t.Helper()
	d := net.Dialer{}
	conn, err := d.DialContext(t.Context(), "tcp", address)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
}

func callPipelineMethod(address, method string, pipes []string) func(t *testing.T) {
	return func(t *testing.T) {
		client := newRPCClient(t, address)

		pipe := &jobsProto.Pipelines{Pipelines: make([]string, len(pipes))}
		for i := range pipes {
			pipe.GetPipelines()[i] = pipes[i]
		}

		er := &jobsProto.JobResponse{}
		err := client.Call(method, pipe, er)
		require.NoError(t, err)
	}
}

func ResumePipes(address string, pipes ...string) func(t *testing.T) {
	return callPipelineMethod(address, resume, pipes)
}

func PushToPipeDelayed(address string, pipeline string, delay int64) func(t *testing.T) {
	return func(t *testing.T) {
		client := newRPCClient(t, address)

		req := &jobsProto.PushRequest{Job: &jobsProto.Job{
			Job:     "some/php/namespace",
			Id:      uuid.NewString(),
			Payload: []byte(`{"hello":"world"}`),
			Headers: map[string]*jobsProto.JobHeaderValue{"test": {Values: []string{"test2"}}},
			Options: &jobsProto.Options{
				Priority: 1,
				Pipeline: pipeline,
				Delay:    delay,
			},
		}}

		er := &jobsProto.JobResponse{}
		err := client.Call(push, req, er)
		assert.NoError(t, err)
	}
}

func PushToPipeBatch(address string, pipeline string, count int, autoAck bool, payload []byte) func(t *testing.T) {
	return func(t *testing.T) {
		client := newRPCClient(t, address)

		jobs := make([]*jobsProto.Job, count)
		for i := range count {
			jobs[i] = createDummyJob(pipeline, autoAck, payload)
		}

		req := &jobsProto.PushBatchRequest{
			Jobs: jobs,
		}

		er := &jobsProto.JobResponse{}
		err := client.Call(pushBatch, req, er)
		assert.NoError(t, err)
	}
}

func PushToPipe(pipeline string, autoAck bool, address string, payload []byte) func(t *testing.T) {
	return func(t *testing.T) {
		client := newRPCClient(t, address)

		req := &jobsProto.PushRequest{Job: createDummyJob(pipeline, autoAck, payload)}

		er := &jobsProto.JobResponse{}
		err := client.Call(push, req, er)
		require.NoError(t, err)
	}
}

func PausePipelines(address string, pipes ...string) func(t *testing.T) {
	return callPipelineMethod(address, pause, pipes)
}

func DestroyPipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := newRPCClient(t, address)

		pipe := &jobsProto.Pipelines{Pipelines: make([]string, len(pipes))}
		for i := range pipes {
			pipe.GetPipelines()[i] = pipes[i]
		}

		for range 10 {
			er := &jobsProto.JobResponse{}
			err := client.Call(destroy, pipe, er)
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			assert.NoError(t, err)
			break
		}
	}
}

func Stats(address string, state *jobState.State) func(t *testing.T) {
	return func(t *testing.T) {
		client := newRPCClient(t, address)

		st := &jobsProto.Stats{}
		er := &jobsProto.JobResponse{}

		err := client.Call(stat, er, st)
		require.NoError(t, err)
		require.NotNil(t, st)

		state.Queue = st.Stats[0].Queue
		state.Pipeline = st.Stats[0].Pipeline
		state.Driver = st.Stats[0].Driver
		state.Active = st.Stats[0].Active
		state.Delayed = st.Stats[0].Delayed
		state.Reserved = st.Stats[0].Reserved
		state.Ready = st.Stats[0].Ready
		state.Priority = st.Stats[0].Priority
	}
}

func setProxy(name string, enabled bool, t *testing.T) {
	t.Helper()
	buf := new(bytes.Buffer)
	fmt.Fprintf(buf, `{"enabled":%t}`, enabled)

	resp, err := http.Post("http://127.0.0.1:8474/proxies/"+name, "application/json", buf) //nolint:noctx
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
}

func EnableProxy(name string, t *testing.T) {
	setProxy(name, true, t)
}

func DisableProxy(name string, t *testing.T) {
	setProxy(name, false, t)
}

func DeleteProxy(name string, t *testing.T) {
	client := &http.Client{}

	req, err := http.NewRequest(http.MethodDelete, "http://127.0.0.1:8474/proxies/"+name, nil) //nolint:noctx
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)

	require.NoError(t, err)
	require.Equal(t, 204, resp.StatusCode)
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
}

func createDummyJob(pipeline string, autoAck bool, payload []byte) *jobsProto.Job {
	return &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      uuid.NewString(),
		Payload: payload,
		Headers: map[string]*jobsProto.JobHeaderValue{"test": {Values: []string{"test2"}}},
		Options: &jobsProto.Options{
			AutoAck:  autoAck,
			Priority: 1,
			Pipeline: pipeline,
			Topic:    pipeline,
		},
	}
}
