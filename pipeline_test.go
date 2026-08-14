package jobs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A key present at the top level wins over the same key nested under `config`,
// which is where the driver options sent by the PHP client end up.
func TestPipelineResolve(t *testing.T) {
	tests := []struct {
		name  string
		pipe  Pipeline
		key   string
		value any
		found bool
	}{
		{
			name:  "top level wins",
			pipe:  Pipeline{"prefetch": "10", config: map[string]any{"prefetch": "20"}},
			key:   "prefetch",
			value: "10",
			found: true,
		},
		{
			name:  "nested when top level is absent",
			pipe:  Pipeline{config: map[string]any{"prefetch": "20"}},
			key:   "prefetch",
			value: "20",
			found: true,
		},
		{
			name: "missing in both",
			pipe: Pipeline{config: map[string]any{"prefetch": "20"}},
			key:  "queue",
		},
		{
			name: "nested config is not a map",
			pipe: Pipeline{config: "prefetch=20"},
			key:  "prefetch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, ok := tt.pipe.resolve(tt.key)

			assert.Equal(t, tt.found, ok)
			assert.Equal(t, tt.value, v)
			assert.Equal(t, tt.found, tt.pipe.Has(tt.key))
		})
	}
}

func TestPipelineString(t *testing.T) {
	tests := []struct {
		name  string
		pipe  Pipeline
		value string
	}{
		{name: "missing key falls back", pipe: Pipeline{}, value: "default"},
		{name: "nil value falls back", pipe: Pipeline{"queue": nil}, value: "default"},
		{name: "empty value falls back", pipe: Pipeline{"queue": ""}, value: "default"},
		{name: "non string value falls back", pipe: Pipeline{"queue": 10}, value: "default"},
		{name: "value is returned", pipe: Pipeline{"queue": "jobs"}, value: "jobs"},
		{name: "nested value is returned", pipe: Pipeline{config: map[string]any{"queue": "jobs"}}, value: "jobs"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.value, tt.pipe.String("queue", "default"))
		})
	}
}

// Drivers receive numbers as strings from the PHP client and as typed values
// from the YAML config, so both have to convert.
func TestPipelineInt(t *testing.T) {
	tests := []struct {
		name  string
		pipe  Pipeline
		value int
	}{
		{name: "missing key falls back", pipe: Pipeline{}, value: 7},
		{name: "nil value falls back", pipe: Pipeline{"prefetch": nil}, value: 7},
		{name: "unparsable string falls back", pipe: Pipeline{"prefetch": "ten"}, value: 7},
		{name: "unsupported type falls back", pipe: Pipeline{"prefetch": []int{1}}, value: 7},
		{name: "string", pipe: Pipeline{"prefetch": "10"}, value: 10},
		{name: "int", pipe: Pipeline{"prefetch": 10}, value: 10},
		{name: "int8", pipe: Pipeline{"prefetch": int8(10)}, value: 10},
		{name: "int16", pipe: Pipeline{"prefetch": int16(10)}, value: 10},
		{name: "int32", pipe: Pipeline{"prefetch": int32(10)}, value: 10},
		{name: "int64", pipe: Pipeline{"prefetch": int64(10)}, value: 10},
		{name: "float32", pipe: Pipeline{"prefetch": float32(10.9)}, value: 10},
		{name: "float64", pipe: Pipeline{"prefetch": 10.9}, value: 10},
		{name: "nested", pipe: Pipeline{config: map[string]any{"prefetch": 10}}, value: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.value, tt.pipe.Int("prefetch", 7))
		})
	}
}

func TestPipelineBool(t *testing.T) {
	tests := []struct {
		name  string
		pipe  Pipeline
		value bool
	}{
		{name: "missing key falls back", pipe: Pipeline{}, value: true},
		{name: "nil value falls back", pipe: Pipeline{"auto_ack": nil}, value: true},
		{name: "non string value falls back", pipe: Pipeline{"auto_ack": false}, value: true},
		{name: "true", pipe: Pipeline{"auto_ack": trueStr}, value: true},
		{name: "false", pipe: Pipeline{"auto_ack": falseStr}, value: false},
		{name: "any other string is false", pipe: Pipeline{"auto_ack": "yes"}, value: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.value, tt.pipe.Bool("auto_ack", true))
		})
	}
}

func TestPipelinePriority(t *testing.T) {
	tests := []struct {
		name  string
		pipe  Pipeline
		value int64
	}{
		{name: "missing key falls back", pipe: Pipeline{}, value: defaultPriority},
		{name: "nil value falls back", pipe: Pipeline{priority: nil}, value: defaultPriority},
		{name: "unconvertible falls back", pipe: Pipeline{priority: []int{1}}, value: defaultPriority},
		{name: "int64", pipe: Pipeline{priority: int64(3)}, value: 3},
		{name: "string", pipe: Pipeline{priority: "3"}, value: 3},
		{name: "nested", pipe: Pipeline{config: map[string]any{priority: 3}}, value: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.value, tt.pipe.Priority())
		})
	}
}

// The PHP client sends the pipeline name under `queue`, so `name` is preferred
// and `queue` is the fallback.
func TestPipelineName(t *testing.T) {
	tests := []struct {
		name  string
		pipe  Pipeline
		value string
	}{
		{name: "name wins", pipe: Pipeline{name: "first", queue: "second"}, value: "first"},
		{name: "queue is the fallback", pipe: Pipeline{queue: "second"}, value: "second"},
		{name: "neither", pipe: Pipeline{}, value: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.value, tt.pipe.Name())
		})
	}
}

func TestPipelineMap(t *testing.T) {
	tests := []struct {
		name    string
		pipe    Pipeline
		decoded map[string]string
		wantErr bool
	}{
		{
			name:    "json object",
			pipe:    Pipeline{"tags": `{"test":"tag"}`},
			decoded: map[string]string{"test": "tag"},
		},
		{
			name:    "nested json object",
			pipe:    Pipeline{config: map[string]any{"tags": `{"test":"tag"}`}},
			decoded: map[string]string{"test": "tag"},
		},
		{name: "missing key", pipe: Pipeline{}, decoded: map[string]string{}},
		{name: "nil value", pipe: Pipeline{"tags": nil}, decoded: map[string]string{}},
		{name: "non string value", pipe: Pipeline{"tags": 10}, decoded: map[string]string{}},
		{name: "empty string", pipe: Pipeline{"tags": ""}, wantErr: true},
		{name: "malformed json", pipe: Pipeline{"tags": `{"test"`}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := make(map[string]string)
			err := tt.pipe.Map("tags", out)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.decoded, out)
		})
	}
}

func TestPipelineAccessors(t *testing.T) {
	pipe := Pipeline{}
	pipe.With(driver, "memory")
	pipe.With(pool, "high")

	assert.Equal(t, "memory", pipe.Driver())
	assert.Equal(t, "high", pipe.Pool())
	assert.Equal(t, "memory", pipe.Get(driver))
	assert.Nil(t, pipe.Get("missing"))

	assert.Empty(t, Pipeline{}.Driver())
	assert.Empty(t, Pipeline{}.Pool())
}

func TestStrToBytes(t *testing.T) {
	assert.Nil(t, strToBytes(""))
	assert.Equal(t, []byte("payload"), strToBytes("payload"))
}
