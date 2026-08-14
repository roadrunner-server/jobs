package protocol

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/roadrunner-server/pool/v2/payload"
)

// fakeJob records which terminal action the handler invoked on it and can fail
// any of them.
type fakeJob struct {
	acked    bool
	nacked   bool
	requeued bool

	ackErr     error
	nackErr    error
	requeueErr error

	requeueHeaders map[string][]string
	requeueDelay   int
}

func (f *fakeJob) ID() string                          { return "test-id" }
func (f *fakeJob) GroupID() string                     { return "" }
func (f *fakeJob) Priority() int64                     { return 10 }
func (f *fakeJob) Ack() error                          { f.acked = true; return f.ackErr }
func (f *fakeJob) Nack() error                         { f.nacked = true; return f.nackErr }
func (f *fakeJob) NackWithOptions(_ bool, _ int) error { f.nacked = true; return f.nackErr }
func (f *fakeJob) Requeue(headers map[string][]string, delay int) error {
	f.requeued = true
	f.requeueHeaders = headers
	f.requeueDelay = delay
	return f.requeueErr
}
func (f *fakeJob) Body() []byte                 { return nil }
func (f *fakeJob) Context() ([]byte, error)     { return nil, nil }
func (f *fakeJob) Headers() map[string][]string { return nil }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestHandleOutcome(t *testing.T) {
	tests := []struct {
		name         string
		body         string
		wantOutcome  Outcome
		wantAcked    bool
		wantRequeued bool
		wantNacked   bool
	}{
		{name: "no_error_acks", body: `{"type":0}`, wantOutcome: OutcomeOK, wantAcked: true},
		{name: "ack", body: `{"type":2}`, wantOutcome: OutcomeOK, wantAcked: true},
		{name: "error_with_requeue", body: `{"type":1,"data":{"requeue":true}}`, wantOutcome: OutcomeRequeued, wantRequeued: true},
		{name: "error_without_requeue_fails", body: `{"type":1,"data":{"requeue":false}}`, wantOutcome: OutcomeFailed, wantAcked: true},
		{name: "nack_with_requeue", body: `{"type":3,"data":{"requeue":true}}`, wantOutcome: OutcomeRequeued, wantNacked: true},
		{name: "nack_without_requeue_fails", body: `{"type":3,"data":{"requeue":false}}`, wantOutcome: OutcomeFailed, wantNacked: true},
		{name: "requeue", body: `{"type":4,"data":{}}`, wantOutcome: OutcomeRequeued, wantRequeued: true},
		// anything the worker sends outside the known types is acknowledged
		{name: "unknown_type_acks", body: `{"type":42}`, wantOutcome: OutcomeOK, wantAcked: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rh := NewResponseHandler(discardLogger())
			jb := &fakeJob{}

			outcome, err := rh.Handle(&payload.Payload{Body: []byte(tc.body)}, jb)
			if err != nil {
				t.Fatalf("Handle returned unexpected error: %v", err)
			}

			if outcome != tc.wantOutcome {
				t.Errorf("outcome = %s, want %s", outcome, tc.wantOutcome)
			}
			if jb.acked != tc.wantAcked {
				t.Errorf("acked = %v, want %v", jb.acked, tc.wantAcked)
			}
			if jb.requeued != tc.wantRequeued {
				t.Errorf("requeued = %v, want %v", jb.requeued, tc.wantRequeued)
			}
			if jb.nacked != tc.wantNacked {
				t.Errorf("nacked = %v, want %v", jb.nacked, tc.wantNacked)
			}
		})
	}
}

// A requeue that fails at the driver surfaces an error (the outcome is then irrelevant).
func TestHandleRequeueError(t *testing.T) {
	rh := NewResponseHandler(discardLogger())
	jb := &fakeJob{requeueErr: errors.New("requeue failed")}

	if _, err := rh.Handle(&payload.Payload{Body: []byte(`{"type":4,"data":{}}`)}, jb); err == nil {
		t.Fatal("expected an error when Requeue fails, got nil")
	}
}

// A response that is not the protocol envelope cannot be acted on.
func TestHandleMalformedResponse(t *testing.T) {
	rh := NewResponseHandler(discardLogger())
	jb := &fakeJob{}

	if _, err := rh.Handle(&payload.Payload{Body: []byte(`not json`)}, jb); err == nil {
		t.Fatal("expected an error for a body that is not the protocol envelope, got nil")
	}

	if jb.acked || jb.nacked || jb.requeued {
		t.Error("expected the job to be left untouched")
	}
}

// An acknowledge that fails at the driver is reported to the caller, on every
// arm that acknowledges.
func TestHandleAckError(t *testing.T) {
	bodies := []string{`{"type":0}`, `{"type":2}`, `{"type":42}`}

	for _, body := range bodies {
		t.Run(body, func(t *testing.T) {
			rh := NewResponseHandler(discardLogger())
			jb := &fakeJob{ackErr: errors.New("ack failed")}

			if _, err := rh.Handle(&payload.Payload{Body: []byte(body)}, jb); err == nil {
				t.Fatal("expected an error when Ack fails, got nil")
			}
		})
	}
}

func TestOutcomeString(t *testing.T) {
	tests := []struct {
		outcome Outcome
		want    string
	}{
		{outcome: OutcomeOK, want: "ok"},
		{outcome: OutcomeFailed, want: "failed"},
		{outcome: OutcomeRequeued, want: "requeued"},
		{outcome: Outcome(42), want: "unknown"},
	}

	for _, tc := range tests {
		if got := tc.outcome.String(); got != tc.want {
			t.Errorf("Outcome(%d).String() = %q, want %q", tc.outcome, got, tc.want)
		}
	}
}
