package jobs

import (
	"context"
	"time"

	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/goridge/v4/pkg/frame"
	"github.com/roadrunner-server/pool/v2/payload"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// non blocking listener
func (p *Plugin) extractMin() {
	// the logic is simple, we should convert blocking call to a blocking but on the channel
	// so we introduce a channel which will be filled with the minimum job from the queue
	// in the listener we will read from the channel instead of reading from the queue directly
	go func() {
		for {
			p.minCh <- p.queue.ExtractMin()
		}
	}()
}

func (p *Plugin) nackJob(jb jobs.Job) {
	if err := jb.Nack(); err != nil {
		p.log.Error("negatively acknowledge failed", "ID", jb.ID(), "error", err)
	}
}

func (p *Plugin) listener() {
	// start the converter from blocking to non-blocking
	p.extractMin()
	for range p.cfg.NumPollers {
		p.pollersWg.Go(func() {
			for {
				select {
				case <-p.stopCh:
					p.log.Debug("------> job poller was stopped <------")
					return
					// get prioritized JOB from the queue
				case jb := <-p.minCh:
					start := time.Now().UTC()

					traceCtx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(jb.Headers()))
					_, span := p.tracer.Tracer(PluginName).Start(traceCtx, "jobs_listener")

					// parse the context
					// for each job, the context contains:
					/*
						1. Job class
						2. Job ID provided from the outside
						3. Job Headers map[string][]string
						4. Timeout in seconds
						5. Pipeline name
					*/

					p.log.Debug("job processing was started", "ID", jb.ID(), "start", start, "elapsed", time.Since(start).Milliseconds())

					ctx, err := jb.Context()
					if err != nil {
						p.metrics.CountJobErr()
						p.log.Error("job marshal error", "error", err, "ID", jb.ID(), "start", start, "elapsed", time.Since(start).Milliseconds())
						p.nackJob(jb)
						span.End()
						continue
					}

					headers := jb.Headers()
					// check if the job should be executed on the different pool
					if len(headers) > 0 && len(headers[pool]) > 0 && headers[pool][0] != "" {
						currPool := p.workersPools[headers[pool][0]]
						// we actually should also check if the boxed type is not nil
						if currPool == nil {
							// invalid pool name, nack the job
							p.metrics.CountJobErr()
							p.log.Error("invalid worker pool name", "pool", headers[pool][0], "ID", jb.ID(), "start", start, "elapsed", time.Since(start).Milliseconds())
							p.nackJob(jb)
							span.End()
							jb = nil
							continue
						}

						p.Execute(ctx, currPool, jb, span, start)
					} else {
						if p.workersPool == nil {
							p.metrics.CountJobErr()
							p.log.Error("no default worker pool configured; job requires a 'pool' header in multi-pool mode",
								"ID", jb.ID(),
								"start", start,
								"elapsed", time.Since(start).Milliseconds())
							p.nackJob(jb)
							span.End()
							jb = nil
							continue
						}
						p.Execute(ctx, p.workersPool, jb, span, start)
					}
				}
			}
		})
	}
}

// Execute dispatches a job to the given worker pool, waits for the response, and handles the result
// according to the RoadRunner jobs protocol (ack, nack, requeue, or delay).
func (p *Plugin) Execute(pldCtx []byte, pool Pool, jb jobs.Job, span trace.Span, start time.Time) {
	// get payload from the sync.Pool
	exec := p.payload(jb.Body(), pldCtx)

	// protect from the pool reset
	// TODO: use a structure with pool and mutex to protect the particular pool
	p.mu.RLock()
	re, err := pool.Exec(context.Background(), exec, nil)
	p.mu.RUnlock()

	if err != nil {
		p.metrics.CountJobErr()

		p.log.Error("job processed with errors", "error", err, "ID", jb.ID(), "start", start, "elapsed", time.Since(start).Milliseconds())
		// RR protocol level error, Nack the job
		p.nackJob(jb)
		p.putPayload(exec)
		jb = nil
		span.End()
		return
	}

	var resp *payload.Payload

	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	select {
	case pld := <-re:
		if pld.Error() != nil {
			p.metrics.CountJobErr()

			p.log.Error("job processed with errors", "error", pld.Error(), "ID", jb.ID(), "start", start, "elapsed", time.Since(start).Milliseconds())
			// RR protocol level error, Nack the job
			p.nackJob(jb)

			p.putPayload(exec)
			jb = nil
			span.End()
			return
		}

		// streaming is not supported
		if pld.Payload().Flags&frame.STREAM != 0 {
			p.metrics.CountJobErr()

			p.log.Warn("streaming is not supported",
				"ID", jb.ID(),
				"start", start,
				"elapsed", time.Since(start).Milliseconds())

			p.nackJob(jb)

			p.putPayload(exec)
			jb = nil
			span.End()
			return
		}

		// assign the payload
		resp = pld.Payload()
	case <-timer.C:
		// timeout
		p.metrics.CountJobErr()
		p.log.Error("worker null response, this is not expected")
		p.nackJob(jb)
		p.putPayload(exec)
		jb = nil
		span.End()
		return
	}

	// if the response is nil or body is nil, acknowledge the job
	if resp == nil || resp.Body == nil {
		p.putPayload(exec)
		err = jb.Ack()
		if err != nil {
			p.metrics.CountJobErr()

			p.log.Error("acknowledge error, job might be missed", "error", err, "ID", jb.ID(), "start", start, "elapsed", time.Since(start).Milliseconds())
			jb = nil
			span.End()
			return
		}

		p.log.Debug("job was processed successfully", "ID", jb.ID(), "start", start, "elapsed", time.Since(start).Milliseconds())

		p.metrics.CountJobOk()

		jb = nil
		span.End()
		return
	}

	// handle the response protocol
	requeued, err := p.respHandler.Handle(resp, jb)
	if err != nil {
		p.metrics.CountJobErr()
		p.log.Error("response handler error", "error", err, "ID", jb.ID(), "response", resp.Body, "start", start, "elapsed", time.Since(start).Milliseconds())
		p.putPayload(exec)
		// we don't need to use ACK to prevent endless loop here, since the ACK is controlled on the PHP side.
		// When experimental features are enabled, skip further processing of the current job.
		if p.experimental {
			jb = nil
			span.End()
			return
		}

		/*
			Job malformed, acknowledge it to prevent endless loop
		*/
		errAck := jb.Ack()
		if errAck != nil {
			p.log.Error("acknowledge failed, job might be lost", "ID", jb.ID(), "error", err, "error", errAck)
			jb = nil
			span.End()
			return
		}

		p.log.Error("job acknowledged, but contains error", "error", err)
		jb = nil
		span.End()
		return
	}

	// a re-queued job is already counted via CountJobRequeue in the response handler;
	// only a non-re-queued completion counts as a successfully processed job.
	if !requeued {
		p.metrics.CountJobOk()
	}

	p.log.Debug("job was processed successfully", "ID", jb.ID(), "start", start, "elapsed", time.Since(start).Milliseconds())

	// return payload
	p.putPayload(exec)
	jb = nil
	span.End()
}
