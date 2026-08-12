package jobs

import (
	"context"
	"net/textproto"
	"sync"
	"time"

	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v1"
	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

type rpc struct {
	p *Plugin
}

func spanError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// Single-job semantics enforced by the proto (PushRequest.job); no runtime length guard.
func (r *rpc) Push(in *jobsProto.PushRequest, _ *jobsProto.Empty) error {
	const op = errors.Op("rpc_push")

	job := in.GetJob()
	if job == nil {
		return errors.E(op, errors.Str("job is required"))
	}
	if job.GetId() == "" {
		return errors.E(op, errors.Str("empty ID field not allowed"))
	}

	spanCtx, span := r.p.tracer.Tracer(PluginName).Start(rpcContextFromJob(context.Background(), job), "push", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	if err := r.p.Push(spanCtx, from(job)); err != nil {
		spanError(span, err)
		return errors.E(op, err)
	}

	return nil
}

func (r *rpc) PushBatch(in *jobsProto.PushBatchRequest, _ *jobsProto.Empty) error {
	const op = errors.Op("rpc_push_batch")

	jobsIn := in.GetJobs()

	spanCtx, span := r.p.tracer.Tracer(PluginName).Start(rpcContextFromJobs(context.Background(), jobsIn), "push_batch", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	batch := make([]jobs.Message, len(jobsIn))
	for i := range jobsIn {
		batch[i] = from(jobsIn[i])
	}

	if err := r.p.PushBatch(spanCtx, batch); err != nil {
		spanError(span, err)
		return errors.E(op, err)
	}

	return nil
}

func (r *rpc) Pause(in *jobsProto.Pipelines, _ *jobsProto.Empty) error {
	const op = errors.Op("rpc_pause")

	for _, name := range in.GetPipelines() {
		spanCtx, span := r.p.tracer.Tracer(PluginName).Start(context.Background(), "pause_pipeline", trace.WithSpanKind(trace.SpanKindServer))
		err := r.p.Pause(spanCtx, name)
		if err != nil {
			spanError(span, err)
			span.End()
			return errors.E(op, err)
		}
		span.End()
	}

	return nil
}

func (r *rpc) Resume(in *jobsProto.Pipelines, _ *jobsProto.Empty) error {
	const op = errors.Op("rpc_resume")

	spanCtx, span := r.p.tracer.Tracer(PluginName).Start(context.Background(), "resume_pipeline", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	for _, name := range in.GetPipelines() {
		if err := r.p.Resume(spanCtx, name); err != nil {
			spanError(span, err)
			return errors.E(op, err)
		}
	}

	return nil
}

func (r *rpc) List(_ *jobsProto.Empty, out *jobsProto.Pipelines) error {
	_, span := r.p.tracer.Tracer(PluginName).Start(context.Background(), "list_pipelines", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	out.Pipelines = r.p.List()
	return nil
}

func (r *rpc) Declare(in *jobsProto.DeclareRequest, _ *jobsProto.Empty) error {
	const op = errors.Op("rpc_declare_pipeline")

	spanCtx, span := r.p.tracer.Tracer(PluginName).Start(context.Background(), "declare_pipeline", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	pipe := Pipeline{}
	for k, v := range in.GetPipeline() {
		pipe[k] = v
	}

	if err := r.p.Declare(spanCtx, pipe); err != nil {
		spanError(span, err)
		return errors.E(op, err)
	}

	return nil
}

func (r *rpc) Destroy(in *jobsProto.Pipelines, out *jobsProto.Pipelines) error {
	const op = errors.Op("rpc_destroy_pipeline")

	errg := errgroup.Group{}
	errg.SetLimit(r.p.cfg.CfgOptions.Parallelism)

	var (
		destroyed []string
		localMu   sync.Mutex
	)

	for _, name := range in.GetPipelines() {
		errg.Go(func() error {
			spanCtx, span := r.p.tracer.Tracer(PluginName).Start(context.Background(), "destroy_pipeline", trace.WithSpanKind(trace.SpanKindServer))
			defer span.End()

			if err := r.p.Destroy(spanCtx, name); err != nil {
				spanError(span, err)
				return errors.E(op, err)
			}

			localMu.Lock()
			destroyed = append(destroyed, name)
			localMu.Unlock()
			return nil
		})
	}

	if err := errg.Wait(); err != nil {
		return errors.E(op, err)
	}

	out.Pipelines = destroyed
	return nil
}

func (r *rpc) Stat(_ *jobsProto.Empty, out *jobsProto.Stats) error {
	const op = errors.Op("rpc_stats")

	statCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	statCtx, span := r.p.tracer.Tracer(PluginName).Start(statCtx, "stat", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	state, err := r.p.JobsState(statCtx)
	if err != nil {
		spanError(span, err)
		return errors.E(op, err)
	}

	out.Stats = make([]*jobsProto.Stat, 0, len(state))
	for i := range state {
		out.Stats = append(out.Stats, &jobsProto.Stat{
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

func from(j *jobsProto.Job) *Job {
	headers := make(map[string][]string, len(j.GetHeaders()))

	for k, v := range j.GetHeaders() {
		headers[k] = v.GetValue()
	}

	return &Job{
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
}

// Layers extracted trace context on parent so cancellation/deadline propagate;
// falls back to parent if no job carries a valid traceparent.
func rpcContextFromJobs(parent context.Context, batch []*jobsProto.Job) context.Context {
	for i := range batch {
		ctx := rpcContextFromJob(parent, batch[i])
		if trace.SpanContextFromContext(ctx).IsValid() {
			return ctx
		}
	}

	return parent
}

func rpcContextFromJob(parent context.Context, job *jobsProto.Job) context.Context {
	if job == nil {
		return parent
	}

	return rpcContextFromHeaders(parent, job.GetHeaders())
}

func rpcContextFromHeaders(parent context.Context, headers map[string]*jobsProto.HeaderValue) context.Context {
	if len(headers) == 0 {
		return parent
	}

	carrier := make(propagation.HeaderCarrier, len(headers))

	for k, v := range headers {
		if v == nil {
			continue
		}

		values := v.GetValue()
		if len(values) == 0 {
			continue
		}

		canonical := textproto.CanonicalMIMEHeaderKey(k)
		if canonical == "" {
			continue
		}

		carrier[canonical] = append(carrier[canonical], values...)
	}

	return otel.GetTextMapPropagator().Extract(parent, carrier)
}
