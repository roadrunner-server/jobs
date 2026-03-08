package jobs

import (
	"net/http"

	"github.com/roadrunner-server/api-plugins/v6/status"
	"github.com/roadrunner-server/pool/fsm"
	"github.com/roadrunner-server/pool/worker"
)

// Status return status of the particular plugin
func (p *Plugin) Status() (*status.Status, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	allWorkers := p.allPoolWorkers()
	for i := range allWorkers {
		if allWorkers[i].State().IsActive() {
			return &status.Status{
				Code: http.StatusOK,
			}, nil
		}
	}
	// if there are no workers, threat this as error
	return &status.Status{
		Code: http.StatusServiceUnavailable,
	}, nil
}

// Ready return readiness status of the particular plugin
func (p *Plugin) Ready() (*status.Status, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	allWorkers := p.allPoolWorkers()
	for i := range allWorkers {
		// If state of the worker is ready (at least 1)
		// we assume, that plugin's worker pool is ready
		if allWorkers[i].State().Compare(fsm.StateReady) {
			return &status.Status{
				Code: http.StatusOK,
			}, nil
		}
	}
	// if there are no workers, threat this as no content error
	return &status.Status{
		Code: http.StatusServiceUnavailable,
	}, nil
}

// allPoolWorkers returns workers from either the single pool or all named pools.
// Must be called under p.mu.RLock().
func (p *Plugin) allPoolWorkers() []*worker.Process {
	if len(p.workersPools) > 0 {
		var all []*worker.Process
		for _, wp := range p.workersPools {
			all = append(all, wp.Workers()...)
		}
		return all
	}

	if p.workersPool != nil {
		return p.workersPool.Workers()
	}

	return nil
}
