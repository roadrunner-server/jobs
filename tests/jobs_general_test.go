package general

import (
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"tests/helpers"
	mocklogger "tests/mock"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/roadrunner-server/amqp/v6"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v2"
	"github.com/roadrunner-server/beanstalk/v6"
	"github.com/roadrunner-server/config/v6"
	"github.com/roadrunner-server/endure/v2"
	"github.com/roadrunner-server/informer/v6"
	"github.com/roadrunner-server/jobs/v6"
	"github.com/roadrunner-server/kafka/v6"
	"github.com/roadrunner-server/logger/v6"
	"github.com/roadrunner-server/memory/v6"
	"github.com/roadrunner-server/metrics/v6"
	"github.com/roadrunner-server/nats/v6"
	"github.com/roadrunner-server/resetter/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/roadrunner-server/sqs/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobsInit(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2023.3.0",
		Path:    "configs/.rr-jobs-init.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
	err := cont.RegisterAll(
		l,
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&memory.Plugin{},
		&amqp.Plugin{},
		&sqs.Plugin{},
		&nats.Plugin{},
		&kafka.Plugin{},
		&beanstalk.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 5)

	t.Run("memory", helpers.PushToPipe("test-1-memory", false, "127.0.0.1:6001", []byte("memory1")))
	t.Run("memory", helpers.PushToPipe("test-2-memory", false, "127.0.0.1:6001", []byte("memory2")))
	t.Run("memory", helpers.PushToPipe("test-3-memory", false, "127.0.0.1:6001", []byte("memory3")))
	t.Run("amqp", helpers.PushToPipe("test-4-amqp", false, "127.0.0.1:6001", []byte("amqp1")))
	t.Run("amqp", helpers.PushToPipe("test-5-amqp", false, "127.0.0.1:6001", []byte("amqp2")))
	t.Run("beanstalk", helpers.PushToPipe("test-6-beanstalk", false, "127.0.0.1:6001", []byte("beanstalk1")))
	t.Run("sqs", helpers.PushToPipe("test-7-sqs", false, "127.0.0.1:6001", []byte("sqs1")))
	t.Run("kafka", helpers.PushToPipe("test-8-kafka", false, "127.0.0.1:6001", []byte("kafka1")))
	t.Run("nats", helpers.PushToPipe("test-9-nats", false, "127.0.0.1:6001", []byte("nats1")))

	time.Sleep(time.Second * 15)

	t.Run("DestroyPipeline", helpers.DestroyPipelines(
		"127.0.0.1:6001",
		"test-1-memory",
		"test-2-memory",
		"test-3-memory",
		"test-4-amqp",
		"test-5-amqp",
		"test-6-beanstalk",
		"test-7-sqs",
		"test-8-kafka",
		"test-9-nats",
	))

	stopCh <- struct{}{}
	wg.Wait()

	time.Sleep(time.Second)

	require.Equal(t, 9, oLogger.FilterMessageSnippet("pipeline was started").Len())
	require.Equal(t, 9, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
	require.Equal(t, 9, oLogger.FilterMessageSnippet("job processing was started").Len())
	require.Equal(t, 9, oLogger.FilterMessageSnippet("job was processed successfully").Len())
}

func TestIssue2085(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2024.2.0",
		Path:    "configs/.rr-issue2085.yaml",
	}

	err := cont.RegisterAll(
		&logger.Plugin{},
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&memory.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 5)

	stopCh <- struct{}{}
	wg.Wait()

	time.Sleep(time.Second)
}

func TestJOBSMetrics(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2024.2.0",
		Path:    "configs/.rr-jobs-metrics.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&rpcPlugin.Plugin{},
		&server.Plugin{},
		&jobs.Plugin{},
		&logger.Plugin{},
		&metrics.Plugin{},
		&memory.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	assert.NoError(t, err)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	tt := time.NewTimer(time.Minute * 3)
	wg := &sync.WaitGroup{}

	wg.Go(func() {
		defer tt.Stop()
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-tt.C:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 2)

	t.Run("DeclareEphemeralPipeline", declareMemoryPipe)
	t.Run("ConsumeEphemeralPipeline", consumeMemoryPipe)

	genericOut, err := get(t)
	assert.NoError(t, err)

	assert.Contains(t, genericOut, `rr_jobs_jobs_err 0`)
	assert.Contains(t, genericOut, `rr_jobs_jobs_ok 0`)
	assert.Contains(t, genericOut, `rr_jobs_jobs_requeue 0`)
	assert.Contains(t, genericOut, `rr_jobs_push_err 0`)
	assert.Contains(t, genericOut, `rr_jobs_push_ok 0`)
	assert.Contains(t, genericOut, `workers_memory_bytes`)
	assert.Contains(t, genericOut, `state="ready"}`)
	assert.Contains(t, genericOut, `{pid=`)
	assert.Contains(t, genericOut, `rr_jobs_total_workers 1`)
	assert.NotContains(t, genericOut, `rr_jobs_requests_total`)
	assert.NotContains(t, genericOut, `rr_jobs_push_latency`)

	t.Run("PushInMemoryPipeline", helpers.PushToPipe("test-3", false, "127.0.0.1:6001", []byte("foo")))
	time.Sleep(time.Second)
	t.Run("PushInMemoryPipeline", helpers.PushToPipeDelayed("127.0.0.1:6001", "test-3", 5))
	time.Sleep(time.Second)
	t.Run("PushInMemoryPipeline", helpers.PushToPipe("test-3", false, "127.0.0.1:6001", []byte("foo")))
	time.Sleep(time.Second * 5)

	genericOut, err = get(t)
	assert.NoError(t, err)

	assert.Contains(t, genericOut, `rr_jobs_jobs_err 0`)
	assert.Contains(t, genericOut, `rr_jobs_jobs_ok 3`)
	assert.Contains(t, genericOut, `rr_jobs_jobs_requeue 0`)
	assert.Contains(t, genericOut, `rr_jobs_push_err 0`)
	assert.Contains(t, genericOut, `rr_jobs_push_ok 3`)
	assert.Contains(t, genericOut, `rr_jobs_requests_total{driver="memory",job="test-3",source="single"} 3`)
	assert.NotContains(t, genericOut, `rr_jobs_requests_total{driver="memory",job="test-3",source="batch"}`)

	t.Run("PushInMemoryPipeline", helpers.PushToPipeBatch("127.0.0.1:6001", "test-3", 2, false, []byte("foo")))
	t.Run("PushInMemoryPipeline", helpers.PushToPipeBatch("127.0.0.1:6001", "test-3", 5, false, []byte("foo")))

	time.Sleep(time.Second)

	genericOut, err = get(t)
	assert.NoError(t, err)

	assert.Contains(t, genericOut, `rr_jobs_jobs_err 0`)
	assert.Contains(t, genericOut, `rr_jobs_jobs_ok 10`)
	assert.Contains(t, genericOut, `rr_jobs_jobs_requeue 0`)
	assert.Contains(t, genericOut, `rr_jobs_push_err 0`)
	assert.Contains(t, genericOut, `rr_jobs_push_ok 10`)
	assert.Contains(t, genericOut, `rr_jobs_requests_total{driver="memory",job="test-3",source="single"} 3`)
	assert.Contains(t, genericOut, `rr_jobs_requests_total{driver="memory",job="test-3",source="batch"} 7`)
	assert.Contains(t, genericOut, `rr_jobs_push_latency_bucket{driver="memory",job="test-3"`)

	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:6001", "test-3"))

	close(sig)
	wg.Wait()
}

func TestJobsPools(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2025.2.0",
		Path:    "configs/.rr-pools.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
	err := cont.RegisterAll(
		l,
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&memory.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 5)

	t.Run("memory", helpers.PushToPipe("test-1-memory", false, "127.0.0.1:6001", []byte("memory1")))
	t.Run("memory", helpers.PushToPipe("test-2-memory", false, "127.0.0.1:6001", []byte("memory2")))
	t.Run("memory", helpers.PushToPipe("test-3-memory", false, "127.0.0.1:6001", []byte("memory3")))

	time.Sleep(time.Second * 15)

	t.Run("DestroyPipeline", helpers.DestroyPipelines(
		"127.0.0.1:6001",
		"test-1-memory",
		"test-2-memory",
		"test-3-memory",
	))

	stopCh <- struct{}{}
	wg.Wait()

	time.Sleep(time.Second)

	assert.Equal(t, 3, oLogger.FilterMessageSnippet("pipeline was started").Len())
	assert.Equal(t, 3, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
	assert.Equal(t, 3, oLogger.FilterMessageSnippet("job processing was started").Len())
	assert.Equal(t, 3, oLogger.FilterMessageSnippet("job was processed successfully").Len())
}

const getAddr = "http://127.0.0.1:2112/metrics"

// get request and return body
func get(t *testing.T) (string, error) {
	r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, getAddr, nil)
	if err != nil {
		return "", err
	}

	client := http.DefaultClient
	resp, err := client.Do(r)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	// unsafe
	return string(b), err
}

func declareMemoryPipe(t *testing.T) {
	client := helpers.NewJobsClient(t, "127.0.0.1:6001")

	_, err := client.Declare(t.Context(), connect.NewRequest(&jobsProto.DeclareRequest{Pipeline: map[string]string{
		"driver":   "memory",
		"name":     "test-3",
		"prefetch": "10000",
	}}))
	assert.NoError(t, err)
}

func consumeMemoryPipe(t *testing.T) {
	client := helpers.NewJobsClient(t, "127.0.0.1:6001")

	_, err := client.Resume(t.Context(), connect.NewRequest(&jobsProto.Pipelines{Pipelines: []string{"test-3"}}))
	assert.NoError(t, err)
}

func TestTracePropagation(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2024.2.0",
		Path:    "configs/.rr-trace-propagation.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
	err := cont.RegisterAll(
		l,
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&memory.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	// Push a job with a known W3C traceparent header
	client := helpers.NewJobsClient(t, "127.0.0.1:6001")

	req := &jobsProto.PushRequest{Job: &jobsProto.Job{
		Job:     "test/trace",
		Id:      uuid.NewString(),
		Payload: []byte(`{"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}`),
		Headers: map[string]*jobsProto.JobHeaderValue{
			"traceparent": {Values: []string{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}},
		},
		Options: &jobsProto.Options{
			Priority: 1,
			Pipeline: "test-trace",
		},
	}}

	_, err = client.Push(t.Context(), connect.NewRequest(req))
	require.NoError(t, err)

	// Wait for PHP worker to process the job
	time.Sleep(time.Second * 3)

	// If traceparent was missing, PHP worker would have errored the task
	require.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was processed successfully").Len(), 1)
	require.Equal(t, 0, oLogger.FilterMessageSnippet("jobs protocol error").Len())

	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:6001", "test-trace"))

	stopCh <- struct{}{}
	wg.Wait()
}
