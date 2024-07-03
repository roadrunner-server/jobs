package jobs

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	jobsApi "github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"go.uber.org/zap"
)

type processor struct {
	wg         sync.WaitGroup
	mu         sync.Mutex
	consumers  *sync.Map
	runners    *map[string]struct{}
	log        *zap.Logger
	queueCh    chan *pjob
	maxWorkers int
	errs       []error
	stopped    *int64
}

type pjob struct {
	jc        jobsApi.Constructor
	pipe      jobsApi.Pipeline
	queue     jobsApi.Queue
	configKey string
	timeout   int
}

// args:
// log - logger
// consumers - sync.Map with all drivers (consumers) for pipelines
// runners - map with all pipelines that should be consumed (started immediately)
// maxWorkers - number of parallel workers which will start pipelines
func newPipesProc(log *zap.Logger, consumers *sync.Map, runners *map[string]struct{}, maxWorkers int) *processor {
	p := &processor{
		log:        log,
		queueCh:    make(chan *pjob, 100),
		maxWorkers: maxWorkers,
		consumers:  consumers,
		runners:    runners,
		wg:         sync.WaitGroup{},
		mu:         sync.Mutex{},
		errs:       make([]error, 0, 1),
		stopped:    ptrTo(int64(0)),
	}

	// start the processor
	p.run()

	return p
}

func (p *processor) run() {
	for i := 0; i < p.maxWorkers; i++ {
		go func() {
			for job := range p.queueCh {
				p.log.Debug("initializing driver", zap.String("pipeline", job.pipe.Name()), zap.String("driver", job.pipe.Driver()))
				t := time.Now().UTC()
				initializedDriver, err := job.jc.DriverFromConfig(job.configKey, job.queue, job.pipe)
				if err != nil {
					p.mu.Lock()
					p.errs = append(p.errs, err)
					p.mu.Unlock()
					p.wg.Done()
					p.log.Error("failed to initialize driver",
						zap.String("pipeline", job.pipe.Name()),
						zap.String("driver", job.pipe.Driver()),
						zap.Error(err))
					continue
				}

				// add a driver to the set of the consumers (name - pipeline name, value - associated driver)
				p.consumers.Store(job.pipe.Name(), initializedDriver)

				p.log.Debug("driver ready", zap.String("pipeline", job.pipe.Name()), zap.String("driver", job.pipe.Driver()), zap.Time("start", t), zap.Int64("elapsed", time.Since(t).Milliseconds()))
				// if a pipeline initialized to be consumed, call Run on it
				if _, ok := (*p.runners)[job.pipe.Name()]; ok {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(job.timeout))
					err = initializedDriver.Run(ctx, job.pipe)
					if err != nil {
						p.mu.Lock()
						p.errs = append(p.errs, err)
						p.mu.Unlock()
					}
					cancel()
				}
				p.wg.Done()
			}

			p.log.Debug("exited from jobs pipeline processor")
		}()
	}
}

func (p *processor) add(pjob *pjob) {
	if atomic.LoadInt64(p.stopped) == 1 {
		p.log.Warn("processor was stopped, can't add a new job")
		return
	}
	p.wg.Add(1)
	p.queueCh <- pjob
}

func (p *processor) errors() []error {
	p.mu.Lock()
	defer p.mu.Unlock()
	errs := make([]error, len(p.errs))
	copy(errs, p.errs)
	// clear the original errors
	p.errs = make([]error, 0, 1)
	return errs
}

func (p *processor) wait() {
	p.wg.Wait()
}

func (p *processor) stop() {
	atomic.StoreInt64(p.stopped, 1)
	close(p.queueCh)
}
