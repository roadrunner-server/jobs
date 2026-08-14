package tests

const (
	// rpcAddr is the rpc listener every config in configs/ binds. The tests share
	// it, so none of them may run in parallel.
	rpcAddr = "127.0.0.1:6381"
	// metricsURL is served by the metrics plugin configured in .rr-jobs-metrics.yaml.
	metricsURL = "http://127.0.0.1:2381/metrics"
)

// brokerAddrs are the queue brokers .rr-jobs-init.yaml dials, published by
// tests/env/docker-compose-jobs.yaml.
func brokerAddrs() []string {
	return []string{
		"127.0.0.1:4222",  // nats
		"127.0.0.1:5672",  // rabbitmq
		"127.0.0.1:11300", // beanstalkd
		"127.0.0.1:4566",  // localstack sqs
		"127.0.0.1:9092",  // kafka
	}
}
