package jobs

import (
	"context"

	"github.com/roadrunner-server/errors"
)

func (p *Plugin) AddWorker() error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.workersPools) == 0 && p.workersPool == nil {
		return errors.Str("single worker pool is not configured, can't add worker")
	}

	switch {
	case p.workersPool != nil:
		return p.workersPool.AddWorker()
	case len(p.workersPools) > 0:
		for _, wp := range p.workersPools {
			if err := wp.AddWorker(); err != nil {
				return err
			}
		}
	}

	return p.workersPool.AddWorker()
}

func (p *Plugin) RemoveWorker(ctx context.Context) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.workersPools) == 0 && p.workersPool == nil {
		return errors.Str("single worker pool is not configured, can't remove worker")
	}

	switch {
	case p.workersPool != nil:
		return p.workersPool.RemoveWorker(ctx)
	case len(p.workersPools) > 0:
		for _, wp := range p.workersPools {
			if err := wp.RemoveWorker(ctx); err != nil {
				return err
			}
		}
	}

	return p.workersPool.RemoveWorker(ctx)
}
