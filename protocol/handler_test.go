package protocol

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/roadrunner-server/pool/v2/payload"
)

// fakeJob records which terminal action the handler invoked on it.
type fakeJob struct {
	acked      bool
	nacked     bool
	requeued   bool
	requeueErr error
}

func (f *fakeJob) ID() string                                 { return "test-id" }
func (f *fakeJob) GroupID() string                            { return "" }
func (f *fakeJob) Priority() int64                            { return 10 }
func (f *fakeJob) Ack() error                                 { f.acked = true; return nil }
func (f *fakeJob) Nack() error                                { f.nacked = true; return nil }
func (f *fakeJob) NackWithOptions(_ bool, _ int) error        { f.nacked = true; return nil }
func (f *fakeJob) Requeue(_ map[string][]string, _ int) error { f.requeued = true; return f.requeueErr }
func (f *fakeJob) Body() []byte                               { return nil }
func (f *fakeJob) Context() ([]byte, error)                   { return nil, nil }
func (f *fakeJob) Headers() map[string][]string               { return nil }

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
