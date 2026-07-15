package helpers

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v2"
	jobState "github.com/roadrunner-server/api-plugins/v6/jobs"
	goridgeRpc "github.com/roadrunner-server/goridge/v4/pkg/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

func NewJobsClient(t *testing.T, address string) *rpc.Client {
	t.Helper()
	conn, err := new(net.Dialer).DialContext(t.Context(), "tcp", address)
	require.NoError(t, err)
	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func ResumePipes(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		err := client.Call("jobs.Resume", &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}, &jobsProto.JobsHandlerResponse{})
		require.NoError(t, err)
	}
}

func PausePipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		err := client.Call("jobs.Pause", &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}, &jobsProto.JobsHandlerResponse{})
		require.NoError(t, err)
	}
}

func PushToPipeDelayed(address string, pipeline string, delay int64) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)

		req := &jobsProto.PushRequest{Job: &jobsProto.Job{
			Job:     "some/php/namespace",
			Id:      uuid.NewString(),
			Payload: []byte(`{"hello":"world"}}`),
			Headers: map[string]*jobsProto.JobHeaderValue{"test": {Values: []string{"test2"}}},
			Options: &jobsProto.Options{
				Priority: 1,
				Pipeline: pipeline,
				Delay:    delay,
			},
		}}

		err := client.Call("jobs.Push", req, &jobsProto.JobsHandlerResponse{})
		assert.NoError(t, err)
	}
}

func PushToPipeBatch(address string, pipeline string, count int, autoAck bool, payload []byte) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)

		jobs := make([]*jobsProto.Job, count)
		for i := range count {
			jobs[i] = createDummyJob(pipeline, autoAck, payload)
		}

		err := client.Call("jobs.PushBatch", &jobsProto.PushBatchRequest{Jobs: jobs}, &jobsProto.JobsHandlerResponse{})
		assert.NoError(t, err)
	}
}

func PushToPipe(pipeline string, autoAck bool, address string, payload []byte) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		err := client.Call("jobs.Push", &jobsProto.PushRequest{Job: createDummyJob(pipeline, autoAck, payload)}, &jobsProto.JobsHandlerResponse{})
		require.NoError(t, err)
	}
}

func DestroyPipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)

		req := &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}

		var lastErr error
		for range 10 {
			err := client.Call("jobs.Destroy", req, &jobsProto.Pipelines{})
			if err == nil {
				return
			}
			lastErr = err
			time.Sleep(time.Second)
		}
		assert.NoError(t, lastErr)
	}
}

func Stats(address string, state *jobState.State) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)

		resp := &jobsProto.Stats{}
		err := client.Call("jobs.GetStats", &emptypb.Empty{}, resp)
		require.NoError(t, err)
		require.NotEmpty(t, resp.GetStats())

		st := resp.GetStats()[0]
		state.Queue = st.GetQueue()
		state.Pipeline = st.GetPipeline()
		state.Driver = st.GetDriver()
		state.Active = st.GetActive()
		state.Delayed = st.GetDelayed()
		state.Reserved = st.GetReserved()
		state.Ready = st.GetReady()
		state.Priority = st.GetPriority()
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
