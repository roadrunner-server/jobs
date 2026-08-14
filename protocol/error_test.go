package protocol

import (
	"errors"
	"testing"

	"github.com/roadrunner-server/pool/v2/payload"
)

// The error response carries the headers and the delay the job is re-queued with.
func TestHandleErrRespRequeue(t *testing.T) {
	rh := NewResponseHandler(discardLogger())
	jb := &fakeJob{}

	body := `{"type":1,"data":{"message":"boom","requeue":true,"delay_seconds":5,"headers":{"retry":["1"]}}}`
	outcome, err := rh.Handle(&payload.Payload{Body: []byte(body)}, jb)
	if err != nil {
		t.Fatalf("Handle returned unexpected error: %v", err)
	}

	if outcome != OutcomeRequeued {
		t.Errorf("outcome = %s, want %s", outcome, OutcomeRequeued)
	}
	if jb.requeueDelay != 5 {
		t.Errorf("delay = %d, want 5", jb.requeueDelay)
	}
	if got := jb.requeueHeaders["retry"]; len(got) != 1 || got[0] != "1" {
		t.Errorf("headers = %v, want map with retry=[1]", jb.requeueHeaders)
	}
}

func TestHandleErrRespRequeueError(t *testing.T) {
	rh := NewResponseHandler(discardLogger())
	jb := &fakeJob{requeueErr: errors.New("requeue failed")}

	body := `{"type":1,"data":{"message":"boom","requeue":true}}`
	if _, err := rh.Handle(&payload.Payload{Body: []byte(body)}, jb); err == nil {
		t.Fatal("expected an error when Requeue fails, got nil")
	}
}

// Without requeue the job is acknowledged so it does not loop; a failing
// acknowledge is logged and the job still counts as failed.
func TestHandleErrRespAckError(t *testing.T) {
	rh := NewResponseHandler(discardLogger())
	jb := &fakeJob{ackErr: errors.New("ack failed")}

	body := `{"type":1,"data":{"message":"boom"}}`
	outcome, err := rh.Handle(&payload.Payload{Body: []byte(body)}, jb)
	if err != nil {
		t.Fatalf("Handle returned unexpected error: %v", err)
	}

	if outcome != OutcomeFailed {
		t.Errorf("outcome = %s, want %s", outcome, OutcomeFailed)
	}
	if !jb.acked {
		t.Error("expected the job to be acknowledged")
	}
}

func TestHandleNackError(t *testing.T) {
	rh := NewResponseHandler(discardLogger())
	jb := &fakeJob{nackErr: errors.New("nack failed")}

	body := `{"type":3,"data":{"message":"boom","requeue":true}}`
	if _, err := rh.Handle(&payload.Payload{Body: []byte(body)}, jb); err == nil {
		t.Fatal("expected an error when NackWithOptions fails, got nil")
	}
}

// The data member of every branch is decoded into the same error response, and
// a member that is not an object stops the branch.
func TestHandleMalformedData(t *testing.T) {
	bodies := map[string]string{
		"error":   `{"type":1,"data":"boom"}`,
		"nack":    `{"type":3,"data":"boom"}`,
		"requeue": `{"type":4,"data":"boom"}`,
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			rh := NewResponseHandler(discardLogger())
			jb := &fakeJob{}

			if _, err := rh.Handle(&payload.Payload{Body: []byte(body)}, jb); err == nil {
				t.Fatal("expected an error for a data member that is not an object, got nil")
			}

			if jb.acked || jb.nacked || jb.requeued {
				t.Error("expected the job to be left untouched")
			}
		})
	}
}
