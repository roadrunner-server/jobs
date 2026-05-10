package helpers

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v2"
	"github.com/roadrunner-server/api-go/v6/jobs/v2/jobsV2connect"
	jobState "github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/types/known/emptypb"
)

// NewJobsClient builds an h2c Connect client for the migrated jobs.v2.JobsService.
// Exported so callers in sibling test packages (e.g. tests/general) can reuse it.
// The rpc plugin runs HTTP/2 cleartext on rpc.listen (127.0.0.1:6001 by default).
// Idle HTTP/2 connections are closed on test cleanup to avoid leaks across tests.
func NewJobsClient(t *testing.T, address string) jobsV2connect.JobsServiceClient {
	t.Helper()
	httpc := &http.Client{Transport: &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return new(net.Dialer).DialContext(ctx, network, addr)
		},
	}}
	t.Cleanup(httpc.CloseIdleConnections)
	return jobsV2connect.NewJobsServiceClient(httpc, "http://"+address)
}

func ResumePipes(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		_, err := client.Resume(t.Context(), connect.NewRequest(&jobsProto.Pipelines{Pipelines: append([]string(nil), pipes...)}))
		require.NoError(t, err)
	}
}

func PausePipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		_, err := client.Pause(t.Context(), connect.NewRequest(&jobsProto.Pipelines{Pipelines: append([]string(nil), pipes...)}))
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

		_, err := client.Push(t.Context(), connect.NewRequest(req))
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

		_, err := client.PushBatch(t.Context(), connect.NewRequest(&jobsProto.PushBatchRequest{Jobs: jobs}))
		assert.NoError(t, err)
	}
}

func PushToPipe(pipeline string, autoAck bool, address string, payload []byte) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		_, err := client.Push(t.Context(), connect.NewRequest(&jobsProto.PushRequest{Job: createDummyJob(pipeline, autoAck, payload)}))
		require.NoError(t, err)
	}
}

func DestroyPipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)

		req := &jobsProto.Pipelines{Pipelines: append([]string(nil), pipes...)}

		var lastErr error
		for range 10 {
			_, err := client.Destroy(t.Context(), connect.NewRequest(req))
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

		resp, err := client.GetStats(t.Context(), connect.NewRequest(&emptypb.Empty{}))
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotEmpty(t, resp.Msg.GetStats())

		st := resp.Msg.GetStats()[0]
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
