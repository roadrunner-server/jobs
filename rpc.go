package jobs

import (
	"context"
	"time"

	"github.com/roadrunner-server/api/v2/plugins/jobs"
	"github.com/roadrunner-server/api/v2/plugins/jobs/pipeline"
	"github.com/roadrunner-server/errors"
	jobsProto "go.buf.build/protocolbuffers/go/roadrunner-server/api/proto/jobs/v1"
)

type rpc struct {
	p *Plugin
}

func (r *rpc) Push(j *jobsProto.PushRequest, _ *jobsProto.Empty) error {
	const op = errors.Op("rpc_push")

	// convert transport entity into domain
	// how we can do this quickly

	if j.GetJob().GetId() == "" {
		return errors.E(op, errors.Str("empty ID field not allowed"))
	}

	err := r.p.Push(from(j.GetJob()))
	if err != nil {
		return errors.E(op, err)
	}

	return nil
}

func (r *rpc) PushBatch(j *jobsProto.PushBatchRequest, _ *jobsProto.Empty) error {
	const op = errors.Op("rpc_push_batch")

	l := len(j.GetJobs())

	batch := make([]*jobs.Job, l)

	for i := 0; i < l; i++ {
		// convert transport entity into domain
		// how we can do this quickly
		batch[i] = from(j.GetJobs()[i])
	}

	err := r.p.PushBatch(batch)
	if err != nil {
		return errors.E(op, err)
	}

	return nil
}

func (r *rpc) Pause(req *jobsProto.Pipelines, _ *jobsProto.Empty) error {
	for i := 0; i < len(req.GetPipelines()); i++ {
		r.p.Pause(req.GetPipelines()[i])
	}

	return nil
}

func (r *rpc) Resume(req *jobsProto.Pipelines, _ *jobsProto.Empty) error {
	for i := 0; i < len(req.GetPipelines()); i++ {
		r.p.Resume(req.GetPipelines()[i])
	}

	return nil
}

func (r *rpc) List(_ *jobsProto.Empty, resp *jobsProto.Pipelines) error {
	resp.Pipelines = r.p.List()
	return nil
}

// Declare pipeline used to dynamically declare any type of the pipeline
// Mandatory fields:
// 1. Driver
// 2. Pipeline name
// 3. Options related to the particular pipeline
func (r *rpc) Declare(req *jobsProto.DeclareRequest, _ *jobsProto.Empty) error {
	const op = errors.Op("rpc_declare_pipeline")
	pipe := &pipeline.Pipeline{}

	for i := range req.GetPipeline() {
		(*pipe)[i] = req.GetPipeline()[i]
	}

	err := r.p.Declare(pipe)
	if err != nil {
		return errors.E(op, err)
	}

	return nil
}

func (r *rpc) Destroy(req *jobsProto.Pipelines, resp *jobsProto.Pipelines) error {
	const op = errors.Op("rpc_declare_pipeline")

	var destroyed []string //nolint:prealloc
	for i := 0; i < len(req.GetPipelines()); i++ {
		err := r.p.Destroy(req.GetPipelines()[i])
		if err != nil {
			return errors.E(op, err)
		}
		destroyed = append(destroyed, req.GetPipelines()[i])
	}

	// return destroyed pipelines
	resp.Pipelines = destroyed

	return nil
}

func (r *rpc) Stat(_ *jobsProto.Empty, resp *jobsProto.Stats) error {
	const op = errors.Op("rpc_stats")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	state, err := r.p.JobsState(ctx)
	if err != nil {
		return errors.E(op, err)
	}

	for i := 0; i < len(state); i++ {
		resp.Stats = append(resp.Stats, &jobsProto.Stat{
			Pipeline: state[i].Pipeline,
			Priority: state[i].Priority,
			Driver:   state[i].Driver,
			Queue:    state[i].Queue,
			Active:   state[i].Active,
			Delayed:  state[i].Delayed,
			Reserved: state[i].Reserved,
			Ready:    state[i].Ready,
		})
	}

	return nil
}

// from converts from transport entity to domain
func from(j *jobsProto.Job) *jobs.Job {
	headers := make(map[string][]string, len(j.GetHeaders()))

	for k, v := range j.GetHeaders() {
		headers[k] = v.GetValue()
	}

	jb := &jobs.Job{
		Job:     j.GetJob(),
		Headers: headers,
		Ident:   j.GetId(),
		Payload: j.GetPayload(),
		Options: &jobs.Options{
			AutoAck:  j.GetOptions().GetAutoAck(),
			Priority: j.GetOptions().GetPriority(),
			Pipeline: j.GetOptions().GetPipeline(),
			Delay:    j.GetOptions().GetDelay(),
		},
	}

	return jb
}
