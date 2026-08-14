package jobs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Options is optional on the wire, and every driver reads the accessors without
// checking for it first.
func TestJobAccessorsWithoutOptions(t *testing.T) {
	j := &Job{Job: "some/php/namespace", Ident: "id-1", Pld: []byte("payload")}

	assert.Equal(t, "some/php/namespace", j.Name())
	assert.Equal(t, "id-1", j.ID())
	assert.Equal(t, []byte("payload"), j.Payload())
	assert.Nil(t, j.Headers())

	assert.Empty(t, j.GroupID())
	assert.Equal(t, defaultPriority, j.Priority())
	assert.Zero(t, j.Delay())
	assert.False(t, j.AutoAck())
	assert.Zero(t, j.Offset())
	assert.Zero(t, j.Partition())
	assert.Empty(t, j.Topic())
	assert.Empty(t, j.Metadata())
}

func TestJobAccessorsWithOptions(t *testing.T) {
	j := &Job{
		Job:   "some/php/namespace",
		Ident: "id-1",
		Hdr:   map[string][]string{"test": {"test2"}},
		Options: &Options{
			Priority:  3,
			Pipeline:  "test-1-memory",
			Delay:     5,
			AutoAck:   true,
			Topic:     "topic",
			Metadata:  "metadata",
			Offset:    12,
			Partition: 2,
		},
	}

	assert.Equal(t, map[string][]string{"test": {"test2"}}, j.Headers())
	assert.Equal(t, "test-1-memory", j.GroupID())
	assert.Equal(t, int64(3), j.Priority())
	assert.Equal(t, int64(5), j.Delay())
	assert.True(t, j.AutoAck())
	assert.Equal(t, int64(12), j.Offset())
	assert.Equal(t, int32(2), j.Partition())
	assert.Equal(t, "topic", j.Topic())
	assert.Equal(t, "metadata", j.Metadata())
}

func TestJobUpdatePriority(t *testing.T) {
	t.Run("allocates the options", func(t *testing.T) {
		j := &Job{}
		j.UpdatePriority(3)

		require.NotNil(t, j.Options)
		assert.Equal(t, int64(3), j.Priority())
	})

	t.Run("overwrites the priority", func(t *testing.T) {
		j := &Job{Options: &Options{Priority: 1, Pipeline: "test-1-memory"}}
		j.UpdatePriority(3)

		assert.Equal(t, int64(3), j.Priority())
		assert.Equal(t, "test-1-memory", j.GroupID())
	})
}

func TestOptionsDelayDuration(t *testing.T) {
	assert.Equal(t, time.Second*5, (&Options{Delay: 5}).DelayDuration())
	assert.Zero(t, (&Options{}).DelayDuration())
}
