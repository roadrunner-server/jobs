package helpers

import (
	"context"
	"log/slog"
	"net"
	"net/rpc"
	"sync"
	"testing"
	"time"

	mocklogger "tests/mock"

	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v1"
	"github.com/roadrunner-server/config/v6"
	"github.com/roadrunner-server/endure/v2"
	goridgeRpc "github.com/roadrunner-server/goridge/v4/pkg/rpc"
	"github.com/roadrunner-server/logger/v6"
	"github.com/stretchr/testify/require"
)

const (
	// defaultConfigVersion is the config schema version used by the test configs.
	defaultConfigVersion = "2024.2.0"
	// probeTimeout caps how long Start waits for the server to answer the probe.
	probeTimeout = time.Second * 30
	probeTick    = time.Millisecond * 50
	probeDial    = time.Second
	// containerLogLevel is the level endure itself logs at.
	containerLogLevel = slog.LevelDebug
)

// bootCfg holds the options applied to a container before it is started.
type bootCfg struct {
	version string
	inline  string
	logger  loggerKind
	probe   func(ctx context.Context) bool
}

// loggerKind selects which logger plugin Start registers.
type loggerKind int

const (
	realLogger loggerKind = iota
	observedLogger
)

// Option customizes the container built by Start and its error-path variants.
type Option func(*bootCfg)

// WithConfigVersion overrides the config schema version.
func WithConfigVersion(v string) Option {
	return func(b *bootCfg) { b.version = v }
}

// WithInlineConfig feeds the container YAML from memory; the cfgPath argument is ignored.
func WithInlineConfig(yaml string) Option {
	return func(b *bootCfg) { b.inline = yaml }
}

// WithObservedLogger registers an in-memory logger instead of the real logger
// plugin and exposes the captured records as RR.Logs.
func WithObservedLogger() Option {
	return func(b *bootCfg) { b.logger = observedLogger }
}

// WithRPCProbe makes Start return only once the rpc plugin at addr answers a
// jobs.List call, which proves both the listener and the jobs plugin are up.
func WithRPCProbe(addr string) Option {
	return func(b *bootCfg) {
		b.probe = func(ctx context.Context) bool {
			_, ok := listPipelines(ctx, addr)
			return ok
		}
	}
}

// WithPipelinesReady is WithRPCProbe plus a requirement that the plugin already
// holds n pipelines, so a test can push right after Start returns.
func WithPipelinesReady(addr string, n int) Option {
	return func(b *bootCfg) {
		b.probe = func(ctx context.Context) bool {
			pipes, ok := listPipelines(ctx, addr)
			return ok && len(pipes) == n
		}
	}
}

// listPipelines dials the rpc plugin and asks the jobs plugin for its pipelines.
// A fresh connection per attempt: the listener may not be bound yet.
func listPipelines(ctx context.Context, addr string) ([]string, bool) {
	d := net.Dialer{Timeout: probeDial}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, false
	}

	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	defer func() { _ = client.Close() }()

	out := &jobsProto.Pipelines{}
	if err := client.Call("jobs.List", &jobsProto.Empty{}, out); err != nil {
		return nil, false
	}

	return out.GetPipelines(), true
}

// RR is a running container.
type RR struct {
	// Logs holds the captured log records, non-nil only with WithObservedLogger.
	Logs *mocklogger.ObservedLogs
}

// Start registers the plugins, boots the container and waits for the probe, if
// any, to answer. Errors arriving on the container channel are reported through
// t.Errorf and stop the container, but they do not abort the test.
//
// The returned stop is idempotent and also registered with t.Cleanup, so tests
// asserting on logs written during shutdown can stop the container mid-test.
func Start(t *testing.T, cfgPath string, plugins []any, opts ...Option) (*RR, func()) {
	t.Helper()

	cont, rr, bc := newContainer(t, cfgPath, plugins, opts)
	require.NoError(t, cont.Init())

	ch, err := cont.Serve()
	require.NoError(t, err)

	stopCont := sync.OnceValue(cont.Stop)
	done := make(chan struct{})
	wg := &sync.WaitGroup{}

	wg.Go(func() {
		for {
			select {
			case res := <-ch:
				if res == nil {
					return
				}
				t.Errorf("plugin %s reported an error: %v", res.VertexID, res.Error)
				if errS := stopCont(); errS != nil {
					t.Errorf("container stop: %v", errS)
				}
			case <-done:
				if errS := stopCont(); errS != nil {
					t.Errorf("container stop: %v", errS)
				}
				return
			}
		}
	})

	// The drain goroutine calls t.Errorf, so it has to be joined while the test
	// is still running.
	stop := sync.OnceFunc(func() {
		close(done)
		wg.Wait()
	})
	t.Cleanup(stop)

	if bc.probe != nil {
		require.Eventually(t, func() bool { return bc.probe(t.Context()) }, probeTimeout, probeTick, "server did not become ready")
	}

	return rr, stop
}

// StartExpectInitError registers the plugins and requires Init to fail, returning its error.
func StartExpectInitError(t *testing.T, cfgPath string, plugins []any, opts ...Option) error {
	t.Helper()

	cont, _, _ := newContainer(t, cfgPath, plugins, opts)

	err := cont.Init()
	require.Error(t, err)

	return err
}

// newContainer builds the container and registers the config, a logger and the
// caller's plugins. The container is not initialized yet.
func newContainer(t *testing.T, cfgPath string, plugins []any, opts []Option) (*endure.Endure, *RR, *bootCfg) {
	t.Helper()

	bc := &bootCfg{version: defaultConfigVersion}
	for _, o := range opts {
		o(bc)
	}

	cfg := &config.Plugin{Version: bc.version}
	if bc.inline != "" {
		cfg.Type = "yaml"
		cfg.ReadInCfg = []byte(bc.inline)
	} else {
		cfg.Path = cfgPath
	}

	rr := &RR{}
	all := []any{cfg}

	switch bc.logger {
	case realLogger:
		all = append(all, &logger.Plugin{})
	case observedLogger:
		l, obs := mocklogger.SlogTestLogger(slog.LevelDebug)
		rr.Logs = obs
		all = append(all, l)
	}

	cont := endure.New(containerLogLevel)
	require.NoError(t, cont.RegisterAll(append(all, plugins...)...))

	return cont, rr, bc
}
