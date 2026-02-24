package jobs

import (
	"testing"

	jobsProto "github.com/roadrunner-server/api/v4/build/jobs/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestRPCContextFromHeadersLowercaseTraceparent(t *testing.T) {
	withTraceContextPropagator(t)

	ctx := rpcContextFromHeaders(map[string]*jobsProto.HeaderValue{
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

	ctx := rpcContextFromHeaders(map[string]*jobsProto.HeaderValue{
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
			sc := trace.SpanContextFromContext(rpcContextFromHeaders(tests[i].headers))
			if sc.IsValid() {
				t.Fatal("expected invalid span context")
			}
		})
	}
}

func TestRPCContextFromJobsUsesFirstValidContext(t *testing.T) {
	withTraceContextPropagator(t)

	ctx := rpcContextFromJobs([]*jobsProto.Job{
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

	ctx := rpcContextFromJobs([]*jobsProto.Job{
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
