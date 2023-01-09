package protocol

import (
	"github.com/goccy/go-json"
	"github.com/roadrunner-server/api/v3/plugins/v1/jobs"
	"github.com/roadrunner-server/sdk/v4/utils"
)

// data - data to redirect to the queue
func (rh *RespHandler) handleQueueResp(data []byte, jb jobs.Acknowledger) error {
	qs := rh.getQResp()
	defer rh.putQResp(qs)

	err := json.Unmarshal(data, qs)
	if err != nil {
		return err
	}

	err = jb.Respond(utils.AsBytes(qs.Payload), qs.Queue)
	if err != nil {
		return err
	}

	return nil
}
