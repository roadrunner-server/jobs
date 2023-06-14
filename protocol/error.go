package protocol

import (
	"github.com/goccy/go-json"
	"github.com/roadrunner-server/api/v4/plugins/v1/jobs"
	"github.com/roadrunner-server/errors"
	"go.uber.org/zap"
)

func (rh *RespHandler) handleErrResp(data []byte, jb jobs.Acknowledger) error {
	er := rh.getErrResp()
	defer rh.putErrResp(er)

	err := json.Unmarshal(data, er)
	if err != nil {
		return err
	}

	rh.log.Error("jobs protocol error", zap.Error(errors.E(er.Msg)), zap.Int64("delay", er.Delay), zap.Bool("requeue", er.Requeue))

	// requeue the job
	if er.Requeue {
		err = jb.Requeue(er.Headers, er.Delay)
		if err != nil {
			return err
		}
		rh.log.Info("job was re-queued", zap.Error(errors.E(er.Msg)), zap.Int64("delay", er.Delay), zap.Bool("requeue", er.Requeue))
		return nil
	}

	// user don't want to requeue the job - silently ACK and return nil
	errAck := jb.Ack()
	if errAck != nil {
		rh.log.Error("job acknowledge was failed", zap.Error(errors.E(er.Msg)), zap.Error(errAck))
		// do not return any error
	}

	return nil
}
