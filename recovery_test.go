package jobs

import (
	stderr "errors"
	"testing"

	jobsApi "github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestartRetainsConsumption(t *testing.T) {
	tests := []struct {
		name       string
		configured bool
		consume    bool
		action     string
		actionErr  bool
		wantRuns   int
	}{
		{name: "declared resumed", action: "resume", wantRuns: 1},
		{name: "configured resumed", configured: true, action: "resume", wantRuns: 1},
		{name: "declared paused", consume: true, action: "pause"},
		{name: "configured paused", configured: true, consume: true, action: "pause"},
		{name: "declared resume failed", action: "resume", actionErr: true},
		{name: "configured resume failed", configured: true, action: "resume", actionErr: true},
		{name: "declared pause failed", consume: true, action: "pause", actionErr: true, wantRuns: 1},
		{name: "configured pause failed", configured: true, consume: true, action: "pause", actionErr: true, wantRuns: 1},
		{name: "declared idle"},
		{name: "configured idle", configured: true},
		{name: "declared consuming", consume: true, wantRuns: 1},
		{name: "configured consuming", configured: true, consume: true, wantRuns: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const pipeline = "test-restart-state"
			cfg := &Config{}
			if tt.consume {
				cfg.Consume = []string{pipeline}
			}
			p := newTestPlugin(t, cfg)
			old := &fakeDriver{state: &jobsApi.State{Ready: false}}
			jc := &fakeConstructor{name: "memory", driver: old}
			p.jobConstructors[jc.name] = jc
			pipe := Pipeline{name: pipeline, driver: jc.name}
			if tt.configured {
				pipe.With(createdWithConfig, "jobs.pipelines."+pipeline+".config")
				p.pipelines.Store(pipeline, pipe)
				p.consumers.Store(pipeline, jobsApi.Driver(old))
			} else {
				require.NoError(t, p.Declare(t.Context(), pipe))
			}

			var err error
			switch tt.action {
			case "pause":
				if tt.actionErr {
					old.pauseErr = stderr.New("pause failed")
				}
				err = p.Pause(t.Context(), pipeline)
			case "resume":
				if tt.actionErr {
					old.resumeErr = stderr.New("resume failed")
				}
				err = p.Resume(t.Context(), pipeline)
			}
			if tt.actionErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			replacement := &fakeDriver{}
			jc.driver = replacement
			_, join := startCommands(t, p)
			p.eventsCh <- driverCommand(t, pipeline, restartSrt)
			join()

			assert.Equal(t, 1, old.Stops())
			assert.Equal(t, tt.wantRuns, replacement.Runs())
			actual, _, err := p.pipelineExists(pipeline)
			require.NoError(t, err)
			assert.Same(t, replacement, actual)
		})
	}
}

func TestDestroyClearsRuntimeConsumptionState(t *testing.T) {
	tests := []struct {
		name     string
		consume  bool
		wantRuns int
	}{
		{name: "manual resume is cleared"},
		{name: "manual pause is cleared", consume: true, wantRuns: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const pipeline = "test-redeclare-state"
			cfg := &Config{}
			if tt.consume {
				cfg.Consume = []string{pipeline}
			}
			p := newTestPlugin(t, cfg)
			jc := declareTestPipeline(t, p, pipeline)
			if tt.consume {
				require.NoError(t, p.Pause(t.Context(), pipeline))
			} else {
				require.NoError(t, p.Resume(t.Context(), pipeline))
			}
			require.NoError(t, p.Destroy(t.Context(), pipeline))

			replacement := &fakeDriver{}
			jc.driver = replacement
			require.NoError(t, p.Declare(t.Context(), Pipeline{name: pipeline, driver: jc.Name()}))
			require.Equal(t, tt.wantRuns, replacement.Runs())
		})
	}
}
