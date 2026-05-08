package protocol

import (
	"encoding/json"

	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/errors"
)

func (rh *RespHandler) handleErrResp(data []byte, jb jobs.Job) error {
	er := rh.getErrResp()
	defer rh.putErrResp(er)

	err := json.Unmarshal(data, er)
	if err != nil {
		return err
	}

	if er.Msg != "" {
		rh.log.Error("jobs protocol error", "error", errors.E(er.Msg), "delay", er.Delay, "requeue", er.Requeue)
	}

	// requeue the job
	if er.Requeue {
		err = jb.Requeue(er.Headers, er.Delay)
		if err != nil {
			return err
		}
		rh.log.Info("job was re-queued", "error", errors.E(er.Msg), "delay", er.Delay, "requeue", er.Requeue)
		return nil
	}

	// the user doesn't want to requeue the job - silently ACK and return nil
	errAck := jb.Ack()
	if errAck != nil {
		rh.log.Error("job acknowledge was failed", "error", errors.E(er.Msg), "error", errAck)
		// do not return any error
	}

	rh.log.Debug("requeue was not set, acknowledging the job", "error", errors.E(er.Msg))

	return nil
}

func (rh *RespHandler) handleNackResponse(data []byte, jb jobs.Job) error {
	er := rh.getErrResp()
	defer rh.putErrResp(er)

	err := json.Unmarshal(data, er)
	if err != nil {
		return err
	}

	// we have an error message
	if er.Msg != "" {
		rh.log.Error("jobs nack request", "error", errors.E(er.Msg), "delay", er.Delay, "requeue", er.Requeue)
	}

	err = jb.NackWithOptions(er.Requeue, er.Delay)
	if err != nil {
		return err
	}

	return nil
}

func (rh *RespHandler) requeue(data []byte, jb jobs.Job) error {
	er := rh.getErrResp()
	defer rh.putErrResp(er)

	err := json.Unmarshal(data, er)
	if err != nil {
		return err
	}

	err = jb.Requeue(er.Headers, er.Delay)
	if err != nil {
		return err
	}

	rh.log.Info("job was re-queued",
		"message", er.Msg,
		"delay", er.Delay,
		"requeue", er.Requeue,
	)

	return nil
}
