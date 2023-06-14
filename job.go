package jobs

import (
	"time"
)

type Job struct {
	// Job contains name of job broker (usually PHP class).
	Job string `json:"job"`

	// Ident is unique identifier of the job, should be provided from outside
	Ident string `json:"id"`

	// Payload is string data (usually JSON) passed to Job broker.
	Pld string `json:"payload"`

	// Headers with key-value pairs
	Hdr map[string][]string `json:"headers"`

	// Options contains set of PipelineOptions specific to job execution. Can be empty.
	Options *Options `json:"options,omitempty"`
}

// Options carry information about how to handle given job.
type Options struct {
	// Priority is job priority, default - 10
	// pointer to distinguish 0 as a priority and nil as priority not set
	Priority int64 `json:"priority"`

	// Pipeline manually specified pipeline.
	Pipeline string `json:"pipeline,omitempty"`

	// Delay defines time duration to delay execution for. Defaults to none.
	Delay int64 `json:"delay,omitempty"`

	// AutoAck use to ack a job right after it arrived from the driver
	AutoAck bool `json:"auto_ack"`

	// kafka specific fields
	// Topic is kafka topic
	Topic string `json:"topic"`
	// Optional metadata
	Metadata string `json:"metadata"`
	// Kafka partition
	Partition int32 `json:"partition"`
	// Kafka offset
	Offset int64 `json:"offset"`
}

// DelayDuration returns delay duration in a form of time.Duration.
func (o *Options) DelayDuration() time.Duration {
	return time.Second * time.Duration(o.Delay)
}

func (j *Job) Offset() int64 {
	if j.Options == nil {
		return 0
	}

	return j.Options.Offset
}

func (j *Job) Partition() int32 {
	if j.Options == nil {
		return 0
	}

	return j.Options.Partition
}

func (j *Job) Topic() string {
	if j.Options == nil {
		return ""
	}

	return j.Options.Topic
}

func (j *Job) Metadata() string {
	if j.Options == nil {
		return ""
	}

	return j.Options.Metadata
}

func (j *Job) ID() string {
	return j.Ident
}

func (j *Job) UpdatePriority(p int64) {
	if j.Options == nil {
		j.Options = &Options{}
	}

	j.Options.Priority = p
}

func (j *Job) Priority() int64 {
	if j.Options == nil {
		return 10
	}

	return j.Options.Priority
}

func (j *Job) Pipeline() string {
	if j.Options == nil {
		return ""
	}

	return j.Options.Pipeline
}

func (j *Job) Name() string {
	return j.Job
}

func (j *Job) Payload() string {
	return j.Pld
}

func (j *Job) Headers() map[string][]string {
	return j.Hdr
}

func (j *Job) Delay() int64 {
	return j.Options.Delay
}

func (j *Job) AutoAck() bool {
	return j.Options.AutoAck
}
