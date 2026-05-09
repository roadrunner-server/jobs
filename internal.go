package jobs

import (
	"context"
	"fmt"

	jobsApi "github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/goridge/v4/pkg/frame"
	"github.com/roadrunner-server/pool/v2/payload"
	"github.com/roadrunner-server/pool/v2/state/process"
	"github.com/roadrunner-server/pool/v2/worker"
)

func (p *Plugin) processState(workers []*worker.Process) []*process.State {
	ps := make([]*process.State, len(workers))
	for i := range workers {
		if workers[i] == nil {
			continue
		}
		st, err := process.WorkerProcessState(workers[i])
		if err != nil {
			p.log.Error("jobs workers state", "error", err)
			return nil
		}

		ps[i] = st
	}

	return ps
}

// pipelineExists used to check if the pipeline exists and return the underlying driver and pipeline or error if not exists
func (p *Plugin) pipelineExists(pp string) (jobsApi.Driver, jobsApi.Pipeline, error) {
	pipe, ok := p.pipelines.Load(pp)
	if !ok {
		p.log.Error("no such pipeline", "requested", pp)
		return nil, nil, fmt.Errorf("no such pipeline, requested: %s", pp)
	}

	if pipe == nil {
		p.log.Error("no pipe registered, value is nil")
		return nil, nil, fmt.Errorf("no pipe registered, value is nil")
	}

	ppl := pipe.(jobsApi.Pipeline)

	d, ok := p.consumers.Load(ppl.Name())
	if !ok {
		p.log.Warn("driver for the pipeline not found", "pipeline", pp)
		return nil, nil, fmt.Errorf("driver for the pipeline not found, pipeline: %s", pp)
	}

	return d.(jobsApi.Driver), ppl, nil
}

func (p *Plugin) payload(body, context []byte) *payload.Payload {
	pld := p.pldPool.Get().(*payload.Payload)
	pld.Body = body
	pld.Context = context
	pld.Codec = frame.CodecRaw
	return pld
}

func (p *Plugin) putPayload(pld *payload.Payload) {
	pld.Body = nil
	pld.Context = nil
	pld.Codec = 0
	pld.Flags = 0
	p.pldPool.Put(pld)
}

func (p *Plugin) waitPollersFinish(ctx context.Context) {
	p.log.Debug("waiting for pollers to be finished")
	done := make(chan struct{})
	go func() {
		p.pollersWg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return
	case <-done:
		return
	}
}
