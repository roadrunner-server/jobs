package tests

import (
	"testing"

	"tests/helpers"

	"github.com/roadrunner-server/amqp/v6"
	"github.com/roadrunner-server/beanstalk/v6"
	"github.com/roadrunner-server/informer/v6"
	"github.com/roadrunner-server/jobs/v6"
	"github.com/roadrunner-server/kafka/v6"
	"github.com/roadrunner-server/memory/v6"
	"github.com/roadrunner-server/nats/v6"
	"github.com/roadrunner-server/resetter/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/roadrunner-server/sqs/v6"
	"github.com/stretchr/testify/require"
)

// One pipeline per driver, each pushed a job and each destroyed afterwards. This
// is the only test that needs the broker stack.
func TestAllDriversPushAndConsume(t *testing.T) {
	helpers.RequireBrokers(t, brokerAddrs()...)

	pipes := []string{
		"test-1-memory",
		"test-2-memory",
		"test-3-memory",
		"test-4-amqp",
		"test-5-amqp",
		"test-6-beanstalk",
		"test-7-sqs",
		"test-8-kafka",
		"test-9-nats",
	}

	rr, stop := helpers.Start(t, "configs/.rr-jobs-init.yaml", []any{
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
	}, helpers.WithObservedLogger(), helpers.WithConfigVersion("2023.3.0"), helpers.WithPipelinesReady(rpcAddr, len(pipes)))

	client := helpers.NewJobsClient(t, rpcAddr)

	for _, pipe := range pipes {
		helpers.Push(t, client, pipe, []byte(pipe))
	}

	helpers.WaitLogged(t, rr.Logs, "job was processed successfully", len(pipes))
	helpers.DestroyPipelines(t, client, pipes...)

	stop()

	require.Equal(t, len(pipes), rr.Logs.FilterMessageSnippet("pipeline was started").Len())
	require.Equal(t, len(pipes), rr.Logs.FilterMessageSnippet("pipeline was stopped").Len())
	require.Equal(t, len(pipes), rr.Logs.FilterMessageSnippet("job processing was started").Len())
	require.Equal(t, len(pipes), rr.Logs.FilterMessageSnippet("job was processed successfully").Len())
}
