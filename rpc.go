package jobs

import (
	"context"
	"sync"
	"time"

	jobsProto "github.com/roadrunner-server/api/v4/build/jobs/v1"
	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/roadrunner-server/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
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
	ctx, span := r.p.tracer.Tracer(spanName).Start(context.Background(), "push", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	err := r.p.Push(ctx, from(j.GetJob()))
	if err != nil {
		return errors.E(op, err)
	}

	return nil
}

func (r *rpc) PushBatch(j *jobsProto.PushBatchRequest, _ *jobsProto.Empty) error {
	const op = errors.Op("rpc_push_batch")

	ctx, span := r.p.tracer.Tracer(spanName).Start(context.Background(), "push_batch", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	l := len(j.GetJobs())

	batch := make([]jobs.Message, l)

	for i := range l {
		// convert transport entity into domain
		// how we can do this quickly
		batch[i] = from(j.GetJobs()[i])
	}

	err := r.p.PushBatch(ctx, batch)
	if err != nil {
		span.SetAttributes(attribute.KeyValue{
			Key:   "error",
			Value: attribute.StringValue(err.Error()),
		})
		return errors.E(op, err)
	}

	return nil
}

func (r *rpc) Pause(req *jobsProto.Pipelines, _ *jobsProto.Empty) error {
	for i := range req.GetPipelines() {
		ctx, span := r.p.tracer.Tracer(spanName).Start(context.Background(), "pause_pipeline", trace.WithSpanKind(trace.SpanKindServer))
		err := r.p.Pause(ctx, req.GetPipelines()[i])
		if err != nil {
			span.SetAttributes(attribute.KeyValue{
				Key:   "error",
				Value: attribute.StringValue(err.Error()),
			})

			span.End()
			return err
		}
		span.End()
	}

	return nil
}

func (r *rpc) Resume(req *jobsProto.Pipelines, _ *jobsProto.Empty) error {
	ctx, span := r.p.tracer.Tracer(spanName).Start(context.Background(), "resume_pipeline", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	for i := range req.GetPipelines() {
		err := r.p.Resume(ctx, req.GetPipelines()[i])
		if err != nil {
			span.SetAttributes(attribute.KeyValue{
				Key:   "error",
				Value: attribute.StringValue(err.Error()),
			})
			return err
		}
	}

	return nil
}

func (r *rpc) List(_ *jobsProto.Empty, resp *jobsProto.Pipelines) error {
	_, span := r.p.tracer.Tracer(spanName).Start(context.Background(), "list_pipelines", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
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
	pipe := Pipeline{}

	ctx, span := r.p.tracer.Tracer(spanName).Start(context.Background(), "declare_pipeline", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	for i := range req.GetPipeline() {
		pipe[i] = req.GetPipeline()[i]
	}

	err := r.p.Declare(ctx, pipe)
	if err != nil {
		span.SetAttributes(attribute.KeyValue{
			Key:   "error",
			Value: attribute.StringValue(err.Error()),
		})
		return errors.E(op, err)
	}

	return nil
}

func (r *rpc) Destroy(req *jobsProto.Pipelines, resp *jobsProto.Pipelines) error {
	const op = errors.Op("rpc_destroy_pipeline")

	mu := sync.Mutex{}

	errg := errgroup.Group{}
	errg.SetLimit(r.p.cfg.CfgOptions.Parallelism)

	var destroyed []string
	for i := range req.GetPipelines() {
		errg.Go(func() error {
			ctx, span := r.p.tracer.Tracer(spanName).Start(context.Background(), "destroy_pipeline", trace.WithSpanKind(trace.SpanKindServer))
			err := r.p.Destroy(ctx, req.GetPipelines()[i])

			if err != nil {
				span.SetAttributes(attribute.KeyValue{
					Key:   "error",
					Value: attribute.StringValue(err.Error()),
				})
				span.End()
				return errors.E(op, err)
			}
			mu.Lock()
			destroyed = append(destroyed, req.GetPipelines()[i])
			mu.Unlock()
			span.End()
			return nil
		})
	}

	err := errg.Wait()
	if err != nil {
		// return destroyed pipelines
		resp.Pipelines = destroyed
		return err
	}

	// return destroyed pipelines
	resp.Pipelines = destroyed

	return nil
}

func (r *rpc) Stat(_ *jobsProto.Empty, resp *jobsProto.Stats) error {
	const op = errors.Op("rpc_stats")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	ctx, span := r.p.tracer.Tracer(spanName).Start(ctx, "stat", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	state, err := r.p.JobsState(ctx)
	if err != nil {
		span.SetAttributes(attribute.KeyValue{
			Key:   "error",
			Value: attribute.StringValue(err.Error()),
		})
		return errors.E(op, err)
	}

	for i := range state {
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
func from(j *jobsProto.Job) *Job {
	headers := make(map[string][]string, len(j.GetHeaders()))

	for k, v := range j.GetHeaders() {
		headers[k] = v.GetValue()
	}

	jb := &Job{
		Job:   j.GetJob(),
		Ident: j.GetId(),
		Pld:   j.GetPayload(),
		Hdr:   headers,
		Options: &Options{
			Priority:  j.GetOptions().GetPriority(),
			Pipeline:  j.GetOptions().GetPipeline(),
			Delay:     j.GetOptions().GetDelay(),
			AutoAck:   j.GetOptions().GetAutoAck(),
			Topic:     j.GetOptions().GetTopic(),
			Metadata:  j.GetOptions().GetMetadata(),
			Partition: j.GetOptions().GetPartition(),
			Offset:    j.GetOptions().GetOffset(),
		},
	}

	return jb
}
