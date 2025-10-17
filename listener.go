package jobs

import (
	"context"
	"time"

	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/roadrunner-server/goridge/v3/pkg/frame"
	"github.com/roadrunner-server/pool/payload"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// non blocking listener
func (p *Plugin) extractMin() {
	// the logic is simple, we should convert blocking call to a bloking but on the channel
	// so we introduce a channel which will be filled with the minimum job from the queue
	// in the listener we will read from the channel instead of reading from the queue directly
	go func() {
		for {
			p.minCh <- p.queue.ExtractMin()
		}
	}()
}

func (p *Plugin) listener() {
	p.pollersWg.Add(p.cfg.NumPollers)
	// start the converter from blocking to non-blocking
	p.extractMin()
	for range p.cfg.NumPollers {
		go func() {
			// defer the wait group, used to track the number of active pollers
			defer p.pollersWg.Done()
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

					p.log.Debug("job processing was started", zap.String("ID", jb.ID()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))

					ctx, err := jb.Context()
					if err != nil {
						p.metrics.CountJobErr()
						p.log.Error("job marshal error", zap.Error(err), zap.String("ID", jb.ID()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))
						errNack := jb.Nack()
						if errNack != nil {
							p.log.Error("negatively acknowledge was failed", zap.String("ID", jb.ID()), zap.Error(errNack))
						}
						span.End()
						continue
					}

					var currPool Pool
					headers := jb.Headers()
					// check if the job should be executed on the different pool
					if len(headers) > 0 && len(headers[pool]) > 0 && headers[pool][0] != "" {
						currPool = p.workersPools[headers[pool][0]]
						// we actually should also check if the boxed type is not nil
						if currPool == nil {
							panic("pool not found")
						}

						p.Execute(ctx, currPool, jb, span, start)
					} else {
						p.Execute(ctx, p.workersPool, jb, span, start)
					}
				}
			}
		}()
	}
}

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

		p.log.Error("job processed with errors", zap.Error(err), zap.String("ID", jb.ID()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))
		// RR protocol level error, Nack the job
		errNack := jb.Nack()
		if errNack != nil {
			p.log.Error("negatively acknowledge failed", zap.String("ID", jb.ID()), zap.Error(errNack))
		}

		p.putPayload(exec)
		jb = nil
		span.End()
		return
	}

	var resp *payload.Payload

	select {
	case pld := <-re:
		if pld.Error() != nil {
			p.metrics.CountJobErr()

			p.log.Error("job processed with errors", zap.Error(err), zap.String("ID", jb.ID()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))
			// RR protocol level error, Nack the job
			errNack := jb.Nack()
			if errNack != nil {
				p.log.Error("negatively acknowledge failed", zap.String("ID", jb.ID()), zap.Error(errNack))
			}

			p.putPayload(exec)
			jb = nil
			span.End()
			return
		}

		// streaming is not supported
		if pld.Payload().Flags&frame.STREAM != 0 {
			p.metrics.CountJobErr()

			p.log.Warn("streaming is not supported",
				zap.String("ID", jb.ID()),
				zap.Time("start", start),
				zap.Int64("elapsed", time.Since(start).Milliseconds()))

			errNack := jb.Nack()
			if errNack != nil {
				p.log.Error("negatively acknowledge failed", zap.String("ID", jb.ID()), zap.Error(errNack))
			}

			p.log.Error("job execute failed", zap.Error(err))
			p.putPayload(exec)
			jb = nil
			span.End()
			return
		}

		// assign the payload
		resp = pld.Payload()
	case <-time.After(time.Second):
		// timeout
		p.metrics.CountJobErr()
		p.log.Error("worker null response, this is not expected")
		errNack := jb.Nack()
		if errNack != nil {
			p.log.Error("negatively acknowledge failed", zap.String("ID", jb.ID()), zap.Error(errNack))
		}
		p.putPayload(exec)
		jb = nil
		span.End()
	}

	// if the response is nil or body is nil, acknowledge the job
	if resp == nil || resp.Body == nil {
		p.putPayload(exec)
		err = jb.Ack()
		if err != nil {
			p.metrics.CountJobErr()

			p.log.Error("acknowledge error, job might be missed", zap.Error(err), zap.String("ID", jb.ID()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))
			jb = nil
			span.End()
			return
		}

		p.log.Debug("job was processed successfully", zap.String("ID", jb.ID()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))

		p.metrics.CountJobOk()

		jb = nil
		span.End()
		return
	}

	// handle the response protocol
	err = p.respHandler.Handle(resp, jb)
	if err != nil {
		p.metrics.CountJobErr()
		p.log.Error("response handler error", zap.Error(err), zap.String("ID", jb.ID()), zap.ByteString("response", resp.Body), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))
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
			p.log.Error("acknowledge failed, job might be lost", zap.String("ID", jb.ID()), zap.Error(err), zap.Error(errAck))
			jb = nil
			span.End()
			return
		}

		p.log.Error("job acknowledged, but contains error", zap.Error(err))
		jb = nil
		span.End()
		return
	}

	p.metrics.CountJobOk()

	p.log.Debug("job was processed successfully", zap.String("ID", jb.ID()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))

	// return payload
	p.putPayload(exec)
	jb = nil
	span.End()
}
