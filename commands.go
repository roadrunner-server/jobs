package jobs

import (
	"context"
	"fmt"
	"time"

	"github.com/roadrunner-server/events"
	"go.opentelemetry.io/otel/trace"
)

func (p *Plugin) readCommands(errCh chan error) {
	for {
		select {
		case ev, ok := <-p.eventsCh:
			if !ok {
				p.log.Warn("events channel was closed")
				return
			}
			if err := p.handleCommand(ev); err != nil {
				select {
				case errCh <- err:
				case <-p.stopCh:
				}
				return
			}
		case <-p.stopCh:
			return
		case <-p.commandsCtx.Done():
			return
		}
	}
}

func (p *Plugin) cancelCommands() {
	p.commandsCancel()
	p.eventBus.Unsubscribe(p.id)
}

func (p *Plugin) handleCommand(ev events.Event) error {
	if err := p.pipelineMu.Acquire(p.commandsCtx, 1); err != nil {
		return nil
	}
	defer p.pipelineMu.Release(1)
	ctx, span := p.tracer.Tracer(PluginName).Start(p.commandsCtx, "read_command", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	p.log.Debug("received JOBS event", "message", ev.Message(), "pipeline", ev.Plugin())

	pipeline := ev.Plugin()
	switch ev.Message() {
	case stopStr:
		err := p.destroy(ctx, pipeline)
		if err != nil {
			p.log.Error("failed to stop the pipeline", "error", err, "pipeline", pipeline)
			span.RecordError(err)
		} else {
			p.log.Info("pipeline was stopped", "pipeline", pipeline)
		}
	case restartSrt:
		drv, pipe, err := p.pipelineExists(pipeline)
		if err != nil {
			p.log.Warn("failed to restart the pipeline", "error", err, "pipeline", pipeline)
			span.RecordError(err)
			return nil
		}

		stopCtx, stopCancel := context.WithTimeout(ctx, time.Second*30)
		err = drv.Stop(stopCtx)
		stopCancel()
		if err != nil {
			p.log.Error("failed to stop the pipeline", "error", err, "pipeline", pipeline)
		}
		p.pipelines.Delete(pipeline)
		p.consumers.Delete(pipeline)

		if ctx.Err() != nil {
			return nil
		}
		restartCtx, restartCancel := context.WithTimeout(ctx, time.Second*30)
		defer restartCancel()

		switch {
		case pipe.String(createdWithDeclare, "") == trueStr:
			if err = p.declare(restartCtx, pipe); err != nil {
				p.log.Error("failed to restart the pipeline", "error", err, "pipeline", pipeline)
				span.RecordError(err)
			}
		case pipe.Has(createdWithConfig):
			p.jobsProcessor.add(&pjob{
				jc:        p.jobConstructors[pipe.Driver()],
				pipe:      pipe,
				queue:     p.queue,
				configKey: pipe.String(createdWithConfig, ""),
				timeout:   p.cfg.Timeout,
				ctx:       restartCtx,
				consume:   p.shouldConsume(pipeline),
			})
			p.jobsProcessor.wait()
			if p.jobsProcessor.hasErrors() {
				return fmt.Errorf("failed to restart the pipeline, errors: %v", p.jobsProcessor.errors())
			}
			p.pipelines.Store(pipeline, pipe)
		default:
			p.log.Warn("unknown pipeline creation method", "pipeline", pipeline)
		}
	default:
		p.log.Warn("unknown command", "command", ev.Message())
	}
	return nil
}
