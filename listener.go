package jobs

import (
	"context"
	"time"

	"github.com/roadrunner-server/api/v4/plugins/v1/jobs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

// non blocking listener
func (p *Plugin) listener() { //nolint:gocognit
	for i := 0; i < p.cfg.NumPollers; i++ {
		go func() {
			for {
				select {
				case <-p.stopCh:
					p.log.Debug("------> job poller was stopped <------")
					return
				default:
					start := time.Now().UTC()
					// get prioritized JOB from the queue
					jb := p.queue.ExtractMin()

					traceCtx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(jb.Metadata()))
					_, span := p.tracer.Tracer(PluginName).Start(traceCtx, "jobs_listener")

					// parse the context
					// for each job, context contains:
					/*
						1. Job class
						2. Job ID provided from the outside
						3. Job Headers map[string][]string
						4. Timeout in seconds
						5. Pipeline name
					*/

					p.log.Debug("job processing was started", zap.String("ID", jb.ID()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))

					ctx, err := jb.Context()
					if err != nil {
						p.metrics.JobErr()

						p.log.Error("job marshal error", zap.Error(err), zap.String("ID", jb.ID()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))

						errNack := jb.(jobs.Acknowledger).Nack()
						if errNack != nil {
							p.log.Error("negatively acknowledge was failed", zap.String("ID", jb.ID()), zap.Error(errNack))
						}
						span.End()
						continue
					}

					// get payload from the sync.Pool
					exec := p.getPayload(jb.Body(), ctx)

					// protect from the pool reset
					p.mu.RLock()
					resp, err := p.workersPool.Exec(context.Background(), exec)
					p.mu.RUnlock()

					if err != nil {
						p.metrics.JobErr()

						p.log.Error("job processed with errors", zap.Error(err), zap.String("ID", jb.ID()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
						if _, ok := jb.(jobs.Acknowledger); !ok {
							p.log.Error("job execute failed, job is not a Acknowledger, skipping Ack/Nack")
							p.putPayload(exec)
							span.End()
							continue
						}
						// RR protocol level error, Nack the job
						errNack := jb.(jobs.Acknowledger).Nack()
						if errNack != nil {
							p.log.Error("negatively acknowledge failed", zap.String("ID", jb.ID()), zap.Error(errNack))
						}

						p.log.Error("job execute failed", zap.Error(err))
						p.putPayload(exec)
						jb = nil
						span.End()
						continue
					}

					if _, ok := jb.(jobs.Acknowledger); !ok {
						// can't acknowledge, just continue
						p.putPayload(exec)
						span.End()
						continue
					}

					// if response is nil or body is nil, just acknowledge the job
					if resp == nil || resp.Body == nil {
						p.putPayload(exec)
						err = jb.(jobs.Acknowledger).Ack()
						if err != nil {
							p.metrics.JobErr()

							p.log.Error("acknowledge error, job might be missed", zap.Error(err), zap.String("ID", jb.ID()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
							jb = nil
							span.End()
							continue
						}

						p.log.Debug("job was processed successfully", zap.String("ID", jb.ID()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
						// collector
						p.metrics.JobOk()

						jb = nil
						span.End()
						continue
					}

					// handle the response protocol
					err = p.respHandler.Handle(resp, jb.(jobs.Acknowledger))
					if err != nil {
						p.metrics.JobErr()
						p.log.Error("response handler error", zap.Error(err), zap.String("ID", jb.ID()), zap.ByteString("response", resp.Body), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
						p.putPayload(exec)
						/*
							Job malformed, acknowledge it to prevent endless loop
						*/
						errAck := jb.(jobs.Acknowledger).Ack()
						if errAck != nil {
							p.log.Error("acknowledge failed, job might be lost", zap.String("ID", jb.ID()), zap.Error(err), zap.Error(errAck))
							jb = nil
							span.End()
							continue
						}

						p.log.Error("job acknowledged, but contains error", zap.Error(err))
						jb = nil
						span.End()
						continue
					}

					p.metrics.JobOk()

					p.log.Debug("job was processed successfully", zap.String("ID", jb.ID()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))

					// return payload
					p.putPayload(exec)
					jb = nil
					span.End()
				}
			}
		}()
	}
}
