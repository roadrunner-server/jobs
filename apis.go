package jobs

import (
	"context"
	"log/slog"

	"github.com/roadrunner-server/pool/v2/payload"
	poolConfig "github.com/roadrunner-server/pool/v2/pool"
	staticPool "github.com/roadrunner-server/pool/v2/pool/static_pool"
	"github.com/roadrunner-server/pool/v2/state/process"
	"github.com/roadrunner-server/pool/v2/worker"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type Logger interface {
	NamedLogger(name string) *slog.Logger
}

type Informer interface {
	Workers() []*process.State
}

type Tracer interface {
	Tracer() *sdktrace.TracerProvider
}

type Pool interface {
	// Workers returns worker list associated with the pool.
	Workers() (workers []*worker.Process)
	// Exec payload
	Exec(ctx context.Context, p *payload.Payload, stopCh chan struct{}) (chan *staticPool.PExec, error)
	// RemoveWorker removes worker from the pool.
	RemoveWorker(ctx context.Context) error
	// AddWorker adds worker to the pool.
	AddWorker() error
	// Reset kill all workers inside the watcher and replaces with new
	Reset(ctx context.Context) error
	// Destroy all underlying stack (but let them complete the task).
	Destroy(ctx context.Context)
}

type Configurer interface {
	// Experimental checks if there are any experimental features enabled.
	Experimental() bool
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if config section exists.
	Has(name string) bool
}

// Server creates workers for the application.
type Server interface {
	NewPool(ctx context.Context, cfg *poolConfig.Config, env map[string]string, _ *slog.Logger) (*staticPool.Pool, error)
}
