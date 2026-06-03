package protocol

import (
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/pool/v2/payload"
)

type Type uint32

const (
	NoError Type = iota
	Error
	ACK
	NACK
	REQUEUE
)

// internal worker protocol (jobs mode)
type protocol struct {
	// message type, see Type
	T Type `json:"type"`
	// Payload
	Data json.RawMessage `json:"data"`
}

// Outcome is the result of handling a worker response; the caller uses it to
// record exactly one of jobs_ok / jobs_err / jobs_requeue.
type Outcome uint8

const (
	OutcomeOK       Outcome = iota // job processed successfully (ack)
	OutcomeFailed                  // terminal failure (worker error or nack without requeue)
	OutcomeRequeued                // job was re-queued
)

func (o Outcome) String() string {
	switch o {
	case OutcomeOK:
		return "ok"
	case OutcomeFailed:
		return "failed"
	case OutcomeRequeued:
		return "requeued"
	default:
		return "unknown"
	}
}

type RespHandler struct {
	log *slog.Logger
	// response pools
	ePool sync.Pool
	pPool sync.Pool
}

func NewResponseHandler(log *slog.Logger) *RespHandler {
	return &RespHandler{
		log: log,

		pPool: sync.Pool{
			New: func() any {
				return new(protocol)
			},
		},

		ePool: sync.Pool{
			New: func() any {
				return new(errorResp)
			},
		},
	}
}

// Handle processes a single worker response — acking, nacking, or re-queueing the job —
// and returns the Outcome so the caller can record exactly one of jobs_ok / jobs_err /
// jobs_requeue. On error the returned Outcome is meaningless (callers check err first).
func (rh *RespHandler) Handle(pld *payload.Payload, jb jobs.Job) (Outcome, error) {
	const op = errors.Op("jobs_handle_response")
	p := rh.getProtocol()
	defer rh.putProtocol(p)

	err := json.Unmarshal(pld.Body, p)
	if err != nil {
		return OutcomeOK, errors.E(op, err)
	}

	switch p.T {
	// likely case
	case NoError:
		if err = jb.Ack(); err != nil {
			return OutcomeOK, errors.E(op, err)
		}
		return OutcomeOK, nil
		// error returned from the PHP
	case Error:
		outcome, errH := rh.handleErrResp(p.Data, jb)
		if errH != nil {
			return OutcomeOK, errors.E(op, errH)
		}
		return outcome, nil
	case ACK:
		if err = jb.Ack(); err != nil {
			return OutcomeOK, errors.E(op, err)
		}
		return OutcomeOK, nil
	case NACK:
		return rh.handleNackResponse(p.Data, jb)
	case REQUEUE:
		return rh.requeue(p.Data, jb)
	default:
		rh.log.Warn("unknown response type, acknowledging the JOB", "type", uint32(p.T))
		if err = jb.Ack(); err != nil {
			return OutcomeOK, errors.E(op, err)
		}
	}

	return OutcomeOK, nil
}

func (rh *RespHandler) getProtocol() *protocol {
	return rh.pPool.Get().(*protocol)
}

func (rh *RespHandler) putProtocol(p *protocol) {
	p.T = 0
	p.Data = nil
	rh.pPool.Put(p)
}

func (rh *RespHandler) getErrResp() *errorResp {
	return rh.ePool.Get().(*errorResp)
}

func (rh *RespHandler) putErrResp(p *errorResp) {
	p.Msg = ""
	p.Headers = nil
	p.Delay = 0
	p.Requeue = false
	rh.ePool.Put(p)
}
