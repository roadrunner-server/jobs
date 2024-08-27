package jobs

import (
	poolImpl "github.com/roadrunner-server/pool/pool"
)

const (
	// name used to set pipeline name
	pipelineName string = "name"
	priorityKey  string = "priority"

	// createdWithConfig and createdWithDeclare are used to determine how the pipeline was created
	createdWithConfig  string = "created_with_config"
	createdWithDeclare string = "created_with_declare"
)

// Config defines settings for job broker, workers and job-pipeline mapping.
type Config struct {
	// NumPollers configures number of priority queue pollers
	// Default - num logical cores
	NumPollers int `mapstructure:"num_pollers"`
	// Options contain additional configuration options for the job plugin
	CfgOptions *CfgOptions `mapstructure:"options"`
	// PipelineSize is the limit of a main jobs queue which consumes Items from the driver's pipeline
	// a Driver pipeline might be much larger than a main jobs queue
	PipelineSize uint64 `mapstructure:"pipeline_size"`
	// Timeout in seconds is the per-push limit to put the job into the queue
	Timeout int `mapstructure:"timeout"`
	// Pool configures roadrunner workers pool.
	Pool *poolImpl.Config `mapstructure:"pool"`
	// Pipelines define mapping between PHP job pipeline and associated job broker.
	Pipelines map[string]Pipeline `mapstructure:"pipelines"`
	// Consuming specifies names of pipelines to be consumed on service start.
	Consume []string `mapstructure:"consume"`
}

type CfgOptions struct {
	// Parallelism configures the number of pipelines to be started at the same time
	Parallelism int `mapstructure:"parallelism"`
}

func (c *Config) InitDefaults() {
	if c.Pool == nil {
		c.Pool = &poolImpl.Config{}
	}

	if c.CfgOptions == nil {
		c.CfgOptions = &CfgOptions{
			Parallelism: 10,
		}
	}

	if c.CfgOptions.Parallelism == 0 {
		c.CfgOptions.Parallelism = 5
	}

	if c.PipelineSize == 0 {
		c.PipelineSize = 1_000_000
	}

	for k := range c.Pipelines {
		// set the pipeline name
		c.Pipelines[k].With(pipelineName, k)
		c.Pipelines[k].With(priorityKey, int64(c.Pipelines[k].Int(priorityKey, 10)))
	}

	if c.Timeout == 0 {
		c.Timeout = 60
	}

	c.Pool.InitDefaults()

	// number of pollers should be slightly more than the number of workers
	// overwrite user value, TODO: deprecate this configuration option
	c.NumPollers = int(c.Pool.NumWorkers) + 2 //nolint:gosec
}
