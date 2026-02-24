// Package jobs implements the RoadRunner jobs plugin, providing asynchronous
// job processing with support for multiple queue backends (memory, AMQP, Kafka,
// SQS, NATS, Beanstalk).
//
// The plugin manages job pipelines, worker pools, and priority-based scheduling.
// Jobs are pushed via RPC from PHP applications and executed by PHP workers
// through the RoadRunner server. OpenTelemetry tracing is integrated for
// distributed trace context propagation across the PHP-Go boundary.
//
// Key components:
//   - Plugin: the main entry point, implementing the endure lifecycle
//   - Pipeline: named queue configuration bound to a specific driver
//   - Job/Options: domain model for jobs and their execution options
//   - RPC methods: Push, PushBatch, Pause, Resume, Declare, Destroy, Stat, List
package jobs
