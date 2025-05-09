package jobs

import (
	"context"
	"time"

	"github.com/roadrunner-server/goridge/v3/pkg/frame"
	"github.com/roadrunner-server/pool/payload"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

// non blocking listener
func (p *Plugin) listener() {
	p.pollersWg.Add(p.cfg.NumPollers)
	for range p.cfg.NumPollers {
		go func() {
			// defer the wait group, used to track the number of active pollers
			defer p.pollersWg.Done()
			for {
				select {
				case <-p.stopCh:
					p.log.Debug("------> job poller was stopped <------")
					return
				default:
					start := time.Now().UTC()
					// get prioritized JOB from the queue
					jb := p.queue.ExtractMin()

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

					// get payload from the sync.Pool
					exec := p.payload(jb.Body(), ctx)

					// protect from the pool reset
					p.mu.RLock()
					re, err := p.workersPool.Exec(context.Background(), exec, nil)
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
						continue
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
							continue
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
							continue
						}

						// assign the payload
						resp = pld.Payload()
					default:
						// should never happen
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
							continue
						}

						p.log.Debug("job was processed successfully", zap.String("ID", jb.ID()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))

						p.metrics.CountJobOk()

						jb = nil
						span.End()
						continue
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
							continue
						}

						/*
							Job malformed, acknowledge it to prevent endless loop
						*/
						errAck := jb.Ack()
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

					p.metrics.CountJobOk()

					p.log.Debug("job was processed successfully", zap.String("ID", jb.ID()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))

					// return payload
					p.putPayload(exec)
					jb = nil
					span.End()
				}
			}
		}()
	}
}
