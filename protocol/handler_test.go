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

// fakeCounter counts CountJobRequeue calls.
type fakeCounter struct{ requeues int }

func (f *fakeCounter) CountJobRequeue() { f.requeues++ }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestHandleRequeueMetric(t *testing.T) {
	tests := []struct {
		name         string
		body         string
		wantRequeues int
		wantAcked    bool
		wantRequeued bool
		wantNacked   bool
	}{
		{name: "no_error_acks", body: `{"type":0}`, wantAcked: true},
		{name: "ack", body: `{"type":2}`, wantAcked: true},
		{name: "error_with_requeue", body: `{"type":1,"data":{"requeue":true}}`, wantRequeues: 1, wantRequeued: true},
		{name: "error_without_requeue_acks", body: `{"type":1,"data":{"requeue":false}}`, wantAcked: true},
		{name: "nack_with_requeue", body: `{"type":3,"data":{"requeue":true}}`, wantRequeues: 1, wantNacked: true},
		{name: "nack_without_requeue", body: `{"type":3,"data":{"requeue":false}}`, wantNacked: true},
		{name: "requeue", body: `{"type":4,"data":{}}`, wantRequeues: 1, wantRequeued: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			counter := &fakeCounter{}
			rh := NewResponseHandler(discardLogger(), counter)
			jb := &fakeJob{}

			requeued, err := rh.Handle(&payload.Payload{Body: []byte(tc.body)}, jb)
			if err != nil {
				t.Fatalf("Handle returned unexpected error: %v", err)
			}

			if wantRequeued := tc.wantRequeues > 0; requeued != wantRequeued {
				t.Errorf("Handle requeued = %v, want %v", requeued, wantRequeued)
			}
			if counter.requeues != tc.wantRequeues {
				t.Errorf("requeue count = %d, want %d", counter.requeues, tc.wantRequeues)
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

// A requeue that fails at the driver must not be counted.
func TestHandleRequeueErrorNotCounted(t *testing.T) {
	counter := &fakeCounter{}
	rh := NewResponseHandler(discardLogger(), counter)
	jb := &fakeJob{requeueErr: errors.New("requeue failed")}

	requeued, err := rh.Handle(&payload.Payload{Body: []byte(`{"type":4,"data":{}}`)}, jb)
	if err == nil {
		t.Fatal("expected an error when Requeue fails, got nil")
	}
	if requeued {
		t.Error("Handle requeued = true, want false on a failed re-queue")
	}
	if counter.requeues != 0 {
		t.Errorf("requeue count = %d, want 0 (a failed requeue must not be counted)", counter.requeues)
	}
}
