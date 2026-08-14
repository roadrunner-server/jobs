package jobs

import (
	"log/slog"
	"net/http"
	"os/exec"
	"testing"

	"github.com/roadrunner-server/pool/v2/fsm"
	"github.com/roadrunner-server/pool/v2/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newWorkerInState builds a worker process that is never started, then walks its
// fsm to the requested state. The fsm only accepts working after ready.
func newWorkerInState(t *testing.T, state int64) *worker.Process {
	t.Helper()

	w, err := worker.InitBaseWorker(exec.CommandContext(t.Context(), "php", "-v"), worker.WithLog(slog.New(slog.DiscardHandler)))
	require.NoError(t, err)

	if state == fsm.StateWorking {
		w.State().Transition(fsm.StateReady)
	}
	w.State().Transition(state)
	require.True(t, w.State().Compare(state))

	return w
}

func TestPluginStatusAndReady(t *testing.T) {
	tests := []struct {
		name       string
		plugin     func(t *testing.T) *Plugin
		statusCode int
		readyCode  int
	}{
		{
			name:       "no pools",
			plugin:     func(*testing.T) *Plugin { return &Plugin{} },
			statusCode: http.StatusServiceUnavailable,
			readyCode:  http.StatusServiceUnavailable,
		},
		{
			name: "inactive worker",
			plugin: func(t *testing.T) *Plugin {
				return &Plugin{workersPool: &fakePool{workers: []*worker.Process{newWorkerInState(t, fsm.StateInactive)}}}
			},
			statusCode: http.StatusServiceUnavailable,
			readyCode:  http.StatusServiceUnavailable,
		},
		{
			name: "ready worker",
			plugin: func(t *testing.T) *Plugin {
				return &Plugin{workersPool: &fakePool{workers: []*worker.Process{newWorkerInState(t, fsm.StateReady)}}}
			},
			statusCode: http.StatusOK,
			readyCode:  http.StatusOK,
		},
		{
			// a busy worker is active but not ready, which is what separates the two
			name: "working worker",
			plugin: func(t *testing.T) *Plugin {
				return &Plugin{workersPool: &fakePool{workers: []*worker.Process{newWorkerInState(t, fsm.StateWorking)}}}
			},
			statusCode: http.StatusOK,
			readyCode:  http.StatusServiceUnavailable,
		},
		{
			name: "named pools are walked as one",
			plugin: func(t *testing.T) *Plugin {
				return &Plugin{workersPools: map[string]Pool{
					"default": &fakePool{},
					"high":    &fakePool{workers: []*worker.Process{newWorkerInState(t, fsm.StateReady)}},
				}}
			},
			statusCode: http.StatusOK,
			readyCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.plugin(t)

			st, err := p.Status()
			require.NoError(t, err)
			assert.Equal(t, tt.statusCode, st.Code)

			rd, err := p.Ready()
			require.NoError(t, err)
			assert.Equal(t, tt.readyCode, rd.Code)
		})
	}
}
