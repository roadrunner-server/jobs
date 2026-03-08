package jobs

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func (p *Plugin) readCommands(errCh chan error) {
	for {
		select {
		case ev, ok := <-p.eventsCh:
			if !ok {
				p.log.Warn("events channel was closed")
				return
			}
			ctx, span := p.tracer.Tracer(PluginName).Start(context.Background(), "read_command", trace.WithSpanKind(trace.SpanKindServer))
			p.log.Debug("received JOBS event", zap.String("message", ev.Message()), zap.String("pipeline", ev.Plugin()))
			// message can be 'restart', 'stop'.
			switch ev.Message() {
			case stopStr:
				// by agreement, the message should contain the pipeline name
				pipeline := ev.Plugin()
				_, _, err := p.pipelineExists(pipeline)
				if err != nil {
					p.log.Warn("failed to restart the pipeline", zap.Error(err), zap.String("pipeline", pipeline))
					span.End()
					continue
				}

				// Destroy operation has its own timeout
				err = p.Destroy(ctx, pipeline)
				if err != nil {
					p.log.Error("failed to stop the pipeline", zap.Error(err), zap.String("pipeline", pipeline))
					span.RecordError(err)
				} else {
					p.log.Info("pipeline was stopped", zap.String("pipeline", pipeline))
				}

				span.End()
				continue
			case restartSrt:
				// Algorithm:
				// 1. Stop the pipeline.
				// 2. Delete the pipeline from the pipeline list.
				// 3. Delete the consumer (actual driver).
				// 4. Check how the pipeline was created (via Declare or via config).
				// 5. If the pipeline was created via config -> use jobsProcessor to add a job to create a pipeline.
				// 5a.If the pipeline was created via Declare -> use Declare again with the same parameters included in the pipeline.

				pipeline := ev.Plugin()
				drv, pipe, err := p.pipelineExists(pipeline)
				if err != nil {
					p.log.Warn("failed to restart the pipeline", zap.Error(err), zap.String("pipeline", pipeline))
					span.RecordError(err)
					span.End()
					continue
				}

				stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second*30)
				// 1. Stop the pipeline
				err = drv.Stop(stopCtx)
				if err != nil {
					p.log.Error("failed to stop the pipeline", zap.Error(err), zap.String("pipeline", pipeline))
				}
				stopCancel()

				// 2+3. Delete the pipeline from the pipeline list
				p.pipelines.Delete(pipeline)
				p.consumers.Delete(pipeline)

				// fresh context for restart operations (the stop context was already canceled)
				restartCtx, restartCancel := context.WithTimeout(context.Background(), time.Second*30)

				// 4. Check how the pipeline was created
				if pipe.String(createdWithDeclare, "") == trueStr { //nolint:gocritic
					// 5. If the pipeline was created via Declare
					err = p.Declare(restartCtx, pipe)
					if err != nil {
						restartCancel()
						p.log.Error("failed to restart the pipeline", zap.Error(err), zap.String("pipeline", pipeline))
						span.RecordError(err)
						span.End()
						continue
					}
					restartCancel()
					// TIP: Do not need to store the pipeline and consumer, as it was done in the Declare
				} else if pipe.String(createdWithConfig, "") != "" {
					// 5a. If the pipeline was created via config
					p.jobsProcessor.add(&pjob{
						p.jobConstructors[pipe.Driver()],
						pipe,
						p.queue,
						pipe.String(createdWithConfig, ""),
						p.cfg.Timeout,
						restartCtx,
					})

					p.jobsProcessor.wait()
					restartCancel()
					// if we have errors when restarting the pipeline, we should stop RR
					if p.jobsProcessor.hasErrors() {
						span.End()
						errCh <- fmt.Errorf("failed to restart the pipeline, errors: %v", p.jobsProcessor.errors())
						return
					}

					// Store the pipeline, consumer would be added by the processor
					p.pipelines.Store(pipeline, pipe)
				} else {
					restartCancel()
					p.log.Warn("unknown pipeline creation method", zap.String("pipeline", pipeline))
				}

				span.End()
				continue
			default:
				p.log.Warn("unknown command", zap.String("command", ev.Message()))
				span.End()
				continue
			}

		case <-p.stopCh:
			return
		}
	}
}
