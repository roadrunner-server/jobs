package jobs

import (
	"errors"
	"testing"

	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v1"
	jobsApi "github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestRPCPushRejectsNilJob(t *testing.T) {
	r := &rpc{}
	err := r.Push(&jobsProto.PushRequest{}, &jobsProto.Empty{})
	if err == nil {
		t.Fatal("expected error for nil job")
	}
}

func TestRPCPushRejectsEmptyJobID(t *testing.T) {
	r := &rpc{}
	err := r.Push(&jobsProto.PushRequest{Job: &jobsProto.Job{}}, &jobsProto.Empty{})
	if err == nil {
		t.Fatal("expected error for empty job ID")
	}
}

func TestRPCContextFromHeadersLowercaseTraceparent(t *testing.T) {
	withTraceContextPropagator(t)

	ctx := rpcContextFromHeaders(t.Context(), map[string]*jobsProto.HeaderValue{
		"traceparent": headerValue("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
	})

	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		t.Fatal("expected valid span context")
	}

	if got, want := sc.TraceID().String(), "4bf92f3577b34da6a3ce929d0e0e4736"; got != want {
		t.Fatalf("unexpected trace id, got %q, want %q", got, want)
	}

	if got, want := sc.SpanID().String(), "00f067aa0ba902b7"; got != want {
		t.Fatalf("unexpected span id, got %q, want %q", got, want)
	}

	if !sc.IsRemote() {
		t.Fatal("expected remote span context")
	}
}

func TestRPCContextFromHeadersCanonicalTraceparent(t *testing.T) {
	withTraceContextPropagator(t)

	ctx := rpcContextFromHeaders(t.Context(), map[string]*jobsProto.HeaderValue{
		"Traceparent": headerValue("00-11111111111111111111111111111111-2222222222222222-01"),
	})

	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		t.Fatal("expected valid span context")
	}

	if got, want := sc.TraceID().String(), "11111111111111111111111111111111"; got != want {
		t.Fatalf("unexpected trace id, got %q, want %q", got, want)
	}
}

func TestRPCContextFromHeadersFallbackOnInvalidTraceparent(t *testing.T) {
	withTraceContextPropagator(t)

	tests := []struct {
		name    string
		headers map[string]*jobsProto.HeaderValue
	}{
		{
			name:    "nil headers",
			headers: nil,
		},
		{
			name:    "empty headers",
			headers: map[string]*jobsProto.HeaderValue{},
		},
		{
			name: "invalid traceparent",
			headers: map[string]*jobsProto.HeaderValue{
				"traceparent": headerValue("invalid"),
			},
		},
		{
			name: "empty traceparent",
			headers: map[string]*jobsProto.HeaderValue{
				"traceparent": headerValue(),
			},
		},
		{
			name: "nil header value",
			headers: map[string]*jobsProto.HeaderValue{
				"traceparent": nil,
			},
		},
	}

	for i := range tests {
		t.Run(tests[i].name, func(t *testing.T) {
			sc := trace.SpanContextFromContext(rpcContextFromHeaders(t.Context(), tests[i].headers))
			if sc.IsValid() {
				t.Fatal("expected invalid span context")
			}
		})
	}
}

func TestRPCContextFromJobsUsesFirstValidContext(t *testing.T) {
	withTraceContextPropagator(t)

	ctx := rpcContextFromJobs(t.Context(), []*jobsProto.Job{
		nil,
		{
			Headers: map[string]*jobsProto.HeaderValue{
				"traceparent": headerValue("invalid"),
			},
		},
		{
			Headers: map[string]*jobsProto.HeaderValue{
				"traceparent": headerValue("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"),
			},
		},
		{
			Headers: map[string]*jobsProto.HeaderValue{
				"traceparent": headerValue("00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01"),
			},
		},
	})

	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		t.Fatal("expected valid span context")
	}

	if got, want := sc.TraceID().String(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; got != want {
		t.Fatalf("unexpected trace id, got %q, want %q", got, want)
	}
}

func TestRPCContextFromJobsFallbackWhenNoValidContext(t *testing.T) {
	withTraceContextPropagator(t)

	ctx := rpcContextFromJobs(t.Context(), []*jobsProto.Job{
		{},
		{
			Headers: map[string]*jobsProto.HeaderValue{
				"traceparent": headerValue("invalid"),
			},
		},
	})

	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		t.Fatal("expected invalid span context")
	}
}

func withTraceContextPropagator(t *testing.T) {
	t.Helper()

	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})

	t.Cleanup(func() {
		otel.SetTextMapPropagator(previous)
	})
}

func headerValue(v ...string) *jobsProto.HeaderValue {
	return &jobsProto.HeaderValue{Value: v}
}

func newTestRPC(t *testing.T, cfg *Config) (*rpc, *fakeDriver) {
	t.Helper()

	p := newTestPlugin(t, cfg)
	drv := &fakeDriver{state: &jobsApi.State{}}
	p.jobConstructors["memory"] = &fakeConstructor{name: "memory", driver: drv}

	return &rpc{p: p}, drv
}

func declareRPCPipeline(t *testing.T, r *rpc, name string) {
	t.Helper()

	req := &jobsProto.DeclareRequest{Pipeline: map[string]string{"driver": "memory", "name": name}}
	require.NoError(t, r.Declare(req, &jobsProto.Empty{}))
}

func TestRPCPushRejectsUnknownPipeline(t *testing.T) {
	r, _ := newTestRPC(t, &Config{})

	req := &jobsProto.PushRequest{Job: &jobsProto.Job{Id: "id-1", Options: &jobsProto.Options{Pipeline: "test-1-memory"}}}
	require.ErrorContains(t, r.Push(req, &jobsProto.Empty{}), "no such pipeline")
}

// The proto job is converted into the internal one, headers and options included.
func TestRPCPush(t *testing.T) {
	r, drv := newTestRPC(t, &Config{})
	declareRPCPipeline(t, r, "test-1-memory")

	req := &jobsProto.PushRequest{Job: &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      "id-1",
		Payload: []byte("payload"),
		Headers: map[string]*jobsProto.HeaderValue{"test": {Value: []string{"test2"}}},
		Options: &jobsProto.Options{
			Priority:  3,
			Pipeline:  "test-1-memory",
			Delay:     5,
			AutoAck:   true,
			Topic:     "topic",
			Metadata:  "metadata",
			Offset:    12,
			Partition: 2,
		},
	}}
	require.NoError(t, r.Push(req, &jobsProto.Empty{}))

	require.Len(t, drv.Pushed(), 1)
	job := drv.Pushed()[0]
	require.Equal(t, "id-1", job.ID())
	require.Equal(t, "some/php/namespace", job.Name())
	require.Equal(t, []byte("payload"), job.Payload())
	require.Equal(t, map[string][]string{"test": {"test2"}}, job.Headers())
	require.Equal(t, int64(3), job.Priority())
	require.Equal(t, int64(5), job.Delay())
	require.True(t, job.AutoAck())
	require.Equal(t, "topic", job.Topic())
	require.Equal(t, "metadata", job.Metadata())
	require.Equal(t, int64(12), job.Offset())
	require.Equal(t, int32(2), job.Partition())
}

func TestRPCPushBatch(t *testing.T) {
	r, drv := newTestRPC(t, &Config{})
	declareRPCPipeline(t, r, "test-1-memory")

	req := &jobsProto.PushBatchRequest{Jobs: []*jobsProto.Job{
		{Id: "id-1", Options: &jobsProto.Options{Pipeline: "test-1-memory"}},
		{Id: "id-2", Options: &jobsProto.Options{Pipeline: "test-1-memory"}},
	}}
	require.NoError(t, r.PushBatch(req, &jobsProto.Empty{}))
	require.Len(t, drv.Pushed(), 2)

	req.Jobs[0].Options.Pipeline = "test-missing"
	require.Error(t, r.PushBatch(req, &jobsProto.Empty{}))
}

func TestRPCPauseResume(t *testing.T) {
	r, drv := newTestRPC(t, &Config{})
	declareRPCPipeline(t, r, "test-1-memory")

	pipes := &jobsProto.Pipelines{Pipelines: []string{"test-1-memory"}}
	require.NoError(t, r.Pause(pipes, &jobsProto.Empty{}))
	require.NoError(t, r.Resume(pipes, &jobsProto.Empty{}))

	require.Equal(t, []string{"test-1-memory"}, drv.Paused())
	require.Equal(t, []string{"test-1-memory"}, drv.Resumed())

	missing := &jobsProto.Pipelines{Pipelines: []string{"test-missing"}}
	require.Error(t, r.Pause(missing, &jobsProto.Empty{}))
	require.Error(t, r.Resume(missing, &jobsProto.Empty{}))
}

func TestRPCListAndDeclare(t *testing.T) {
	r, _ := newTestRPC(t, &Config{})

	out := &jobsProto.Pipelines{}
	require.NoError(t, r.List(&jobsProto.Empty{}, out))
	require.Empty(t, out.GetPipelines())

	declareRPCPipeline(t, r, "test-1-memory")

	require.NoError(t, r.List(&jobsProto.Empty{}, out))
	require.Equal(t, []string{"test-1-memory"}, out.GetPipelines())

	// a pipeline without a driver never reaches a constructor
	bad := &jobsProto.DeclareRequest{Pipeline: map[string]string{"name": "test-2-memory"}}
	require.Error(t, r.Declare(bad, &jobsProto.Empty{}))
}

func TestRPCDestroy(t *testing.T) {
	r, drv := newTestRPC(t, &Config{})
	declareRPCPipeline(t, r, "test-1-memory")
	declareRPCPipeline(t, r, "test-2-memory")

	out := &jobsProto.Pipelines{}
	require.NoError(t, r.Destroy(&jobsProto.Pipelines{Pipelines: []string{"test-1-memory", "test-2-memory"}}, out))
	require.ElementsMatch(t, []string{"test-1-memory", "test-2-memory"}, out.GetPipelines())
	require.Equal(t, 2, drv.Stops())

	require.Error(t, r.Destroy(&jobsProto.Pipelines{Pipelines: []string{"test-1-memory"}}, out))
}

func TestRPCStat(t *testing.T) {
	r, drv := newTestRPC(t, &Config{})
	drv.state = &jobsApi.State{
		Pipeline: "test-1-memory",
		Driver:   "memory",
		Queue:    "test-1-memory",
		Priority: 3,
		Active:   1,
		Delayed:  2,
		Reserved: 4,
		Ready:    true,
	}
	declareRPCPipeline(t, r, "test-1-memory")

	out := &jobsProto.Stats{}
	require.NoError(t, r.Stat(&jobsProto.Empty{}, out))

	require.Len(t, out.GetStats(), 1)
	stat := out.GetStats()[0]
	require.Equal(t, "test-1-memory", stat.GetPipeline())
	require.Equal(t, "memory", stat.GetDriver())
	require.Equal(t, "test-1-memory", stat.GetQueue())
	require.Equal(t, uint64(3), stat.GetPriority())
	require.Equal(t, int64(1), stat.GetActive())
	require.Equal(t, int64(2), stat.GetDelayed())
	require.Equal(t, int64(4), stat.GetReserved())
	require.True(t, stat.GetReady())

	drv.stateErr = errors.New("broker unreachable")
	require.Error(t, r.Stat(&jobsProto.Empty{}, out))
}
