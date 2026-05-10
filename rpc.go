package jobs

import (
	"context"
	"net/textproto"
	"sync"
	"time"

	"connectrpc.com/connect"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v2"
	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/emptypb"
)

type rpc struct {
	p  *Plugin
	mu sync.RWMutex
}

func spanError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// Push sends a single job. The proto shape (PushRequest { Job job = 1; }) enforces
// single-job semantics — no runtime length guard needed.
func (r *rpc) Push(_ context.Context, req *connect.Request[jobsProto.PushRequest]) (*connect.Response[jobsProto.JobsHandlerResponse], error) {
	const op = errors.Op("rpc_push")

	r.mu.Lock()
	defer r.mu.Unlock()

	job := req.Msg.GetJob()
	if job == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.E(op, errors.Str("job is required")))
	}
	if job.GetId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.E(op, errors.Str("empty ID field not allowed")))
	}

	// Derive trace context from the job's own headers (cross-process propagation
	// from the producing PHP worker), not from the inbound RPC context.
	spanCtx, span := r.p.tracer.Tracer(PluginName).Start(rpcContextFromJob(job), "push", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	if err := r.p.Push(spanCtx, from(job)); err != nil {
		spanError(span, err)
		return nil, connect.NewError(connect.CodeInternal, errors.E(op, err))
	}

	return connect.NewResponse(&jobsProto.JobsHandlerResponse{}), nil
}

func (r *rpc) PushBatch(_ context.Context, req *connect.Request[jobsProto.PushBatchRequest]) (*connect.Response[jobsProto.JobsHandlerResponse], error) {
	const op = errors.Op("rpc_push_batch")

	r.mu.Lock()
	defer r.mu.Unlock()

	in := req.Msg.GetJobs()

	spanCtx, span := r.p.tracer.Tracer(PluginName).Start(rpcContextFromJobs(in), "push_batch", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	batch := make([]jobs.Message, len(in))
	for i := range in {
		batch[i] = from(in[i])
	}

	if err := r.p.PushBatch(spanCtx, batch); err != nil {
		spanError(span, err)
		return nil, connect.NewError(connect.CodeInternal, errors.E(op, err))
	}

	return connect.NewResponse(&jobsProto.JobsHandlerResponse{}), nil
}

func (r *rpc) Pause(ctx context.Context, req *connect.Request[jobsProto.Pipelines]) (*connect.Response[jobsProto.JobsHandlerResponse], error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, name := range req.Msg.GetPipelines() {
		spanCtx, span := r.p.tracer.Tracer(PluginName).Start(ctx, "pause_pipeline", trace.WithSpanKind(trace.SpanKindServer))
		err := r.p.Pause(spanCtx, name)
		if err != nil {
			spanError(span, err)
			span.End()
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		span.End()
	}

	return connect.NewResponse(&jobsProto.JobsHandlerResponse{}), nil
}

func (r *rpc) Resume(ctx context.Context, req *connect.Request[jobsProto.Pipelines]) (*connect.Response[jobsProto.JobsHandlerResponse], error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	spanCtx, span := r.p.tracer.Tracer(PluginName).Start(ctx, "resume_pipeline", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	for _, name := range req.Msg.GetPipelines() {
		if err := r.p.Resume(spanCtx, name); err != nil {
			spanError(span, err)
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	return connect.NewResponse(&jobsProto.JobsHandlerResponse{}), nil
}

func (r *rpc) List(ctx context.Context, _ *connect.Request[emptypb.Empty]) (*connect.Response[jobsProto.Pipelines], error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, span := r.p.tracer.Tracer(PluginName).Start(ctx, "list_pipelines", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	return connect.NewResponse(&jobsProto.Pipelines{Pipelines: r.p.List()}), nil
}

// Declare dynamically registers a pipeline. Mandatory fields in the request map:
//  1. driver
//  2. pipeline name
//  3. driver-specific options
func (r *rpc) Declare(ctx context.Context, req *connect.Request[jobsProto.DeclareRequest]) (*connect.Response[jobsProto.JobsHandlerResponse], error) {
	const op = errors.Op("rpc_declare_pipeline")

	r.mu.Lock()
	defer r.mu.Unlock()

	spanCtx, span := r.p.tracer.Tracer(PluginName).Start(ctx, "declare_pipeline", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	pipe := Pipeline{}
	for k, v := range req.Msg.GetPipeline() {
		pipe[k] = v
	}

	if err := r.p.Declare(spanCtx, pipe); err != nil {
		spanError(span, err)
		return nil, connect.NewError(connect.CodeInternal, errors.E(op, err))
	}

	return connect.NewResponse(&jobsProto.JobsHandlerResponse{}), nil
}

func (r *rpc) Destroy(ctx context.Context, req *connect.Request[jobsProto.Pipelines]) (*connect.Response[jobsProto.Pipelines], error) {
	const op = errors.Op("rpc_destroy_pipeline")

	r.mu.Lock()
	defer r.mu.Unlock()

	errg := errgroup.Group{}
	errg.SetLimit(r.p.cfg.CfgOptions.Parallelism)

	var (
		destroyed []string
		localMu   sync.Mutex
	)

	for _, name := range req.Msg.GetPipelines() {
		errg.Go(func() error {
			spanCtx, span := r.p.tracer.Tracer(PluginName).Start(ctx, "destroy_pipeline", trace.WithSpanKind(trace.SpanKindServer))
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
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&jobsProto.Pipelines{Pipelines: destroyed}), nil
}

func (r *rpc) GetStats(ctx context.Context, _ *connect.Request[emptypb.Empty]) (*connect.Response[jobsProto.Stats], error) {
	const op = errors.Op("rpc_stats")

	r.mu.RLock()
	defer r.mu.RUnlock()

	statCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	statCtx, span := r.p.tracer.Tracer(PluginName).Start(statCtx, "stat", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	state, err := r.p.JobsState(statCtx)
	if err != nil {
		spanError(span, err)
		return nil, connect.NewError(connect.CodeInternal, errors.E(op, err))
	}

	resp := &jobsProto.Stats{Stats: make([]*jobsProto.Stat, 0, len(state))}
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

	return connect.NewResponse(resp), nil
}

// from converts a wire Job into the plugin's domain Job.
func from(j *jobsProto.Job) *Job {
	headers := make(map[string][]string, len(j.GetHeaders()))

	for k, v := range j.GetHeaders() {
		headers[k] = v.GetValues()
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

func rpcContextFromJobs(batch []*jobsProto.Job) context.Context {
	for i := range batch {
		ctx := rpcContextFromJob(batch[i])
		if trace.SpanContextFromContext(ctx).IsValid() {
			return ctx
		}
	}

	return context.Background()
}

func rpcContextFromJob(job *jobsProto.Job) context.Context {
	if job == nil {
		return context.Background()
	}

	return rpcContextFromHeaders(job.GetHeaders())
}

func rpcContextFromHeaders(headers map[string]*jobsProto.JobHeaderValue) context.Context {
	if len(headers) == 0 {
		return context.Background()
	}

	carrier := make(propagation.HeaderCarrier, len(headers))

	for k, v := range headers {
		if v == nil {
			continue
		}

		values := v.GetValues()
		if len(values) == 0 {
			continue
		}

		canonical := textproto.CanonicalMIMEHeaderKey(k)
		if canonical == "" {
			continue
		}

		carrier[canonical] = append(carrier[canonical], values...)
	}

	return otel.GetTextMapPropagator().Extract(context.Background(), carrier)
}
