package jobs

import (
	"encoding/json"
	"math"
	"strconv"
	"unsafe"
)

// Pipeline defines pipeline options.
type Pipeline map[string]any

const (
	priority        string = "priority"
	defaultPriority int64  = 10
	driver          string = "driver"
	name            string = "name"
	queue           string = "queue"
	pool            string = "pool"

	// config
	config string = "config"

	trueStr  string = "true"
	falseStr string = "false"
)

// resolve performs a two-phase lookup: first checks the top-level map,
// then falls back to the nested "config" sub-map.
func (p Pipeline) resolve(key string) (any, bool) {
	if v, ok := p[key]; ok {
		return v, true
	}
	if val, ok := p[config]; ok {
		if rv, ok := val.(map[string]any); ok {
			if v, ok := rv[key]; ok {
				return v, true
			}
		}
	}
	return nil, false
}

// With pipeline value
func (p Pipeline) With(name string, value any) {
	p[name] = value
}

func (p Pipeline) Pool() string {
	return p.String(pool, "")
}

// Name returns pipeline name.
func (p Pipeline) Name() string {
	// https://github.com/spiral/roadrunner-jobs/blob/master/src/Queue/CreateInfo.php#L81
	// In the PHP client library used the wrong key name
	// should be "name" instead of "queue"
	if n := p.String(name, ""); n != "" {
		return n
	}

	return p.String(queue, "")
}

// Driver associated with the pipeline.
func (p Pipeline) Driver() string {
	return p.String(driver, "")
}

// Has checks if value presented in pipeline.
func (p Pipeline) Has(key string) bool {
	_, ok := p.resolve(key)
	return ok
}

// String must return option value as string or return default value.
func (p Pipeline) String(key string, d string) string {
	v, ok := p.resolve(key)
	if !ok || v == nil {
		return d
	}
	if str, ok := v.(string); ok {
		if str != "" {
			return str
		}
	}
	return d
}

// Int must return option value as int or return default value.
func (p Pipeline) Int(key string, d int) int {
	v, ok := p.resolve(key)
	if !ok || v == nil {
		return d
	}
	switch val := v.(type) {
	case string:
		res, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return d
		}
		if res > math.MaxInt32 || res < math.MinInt32 {
			return d
		}
		return int(res)
	case float32:
		return int(val)
	case float64:
		return int(val)
	case int:
		return val
	case int64:
		return int(val)
	case int32:
		return int(val)
	case int16:
		return int(val)
	case int8:
		return int(val)
	default:
		return d
	}
}

// Bool must return option value as bool or return default value.
func (p Pipeline) Bool(key string, d bool) bool {
	v, ok := p.resolve(key)
	if !ok || v == nil {
		return d
	}
	if i, ok := v.(string); ok {
		switch i {
		case trueStr:
			return true
		case falseStr:
			return false
		default:
			return false
		}
	}
	return d
}

// Map must return nested map value or empty config.
// There might be sqs attributes or tags, for example
func (p Pipeline) Map(key string, out map[string]string) error {
	v, ok := p.resolve(key)
	if !ok || v == nil {
		return nil
	}
	if m, ok := v.(string); ok {
		err := json.Unmarshal(strToBytes(m), &out)
		if err != nil {
			return err
		}
	}
	return nil
}

// Priority returns default pipeline priority
func (p Pipeline) Priority() int64 {
	v, ok := p.resolve(priority)
	if !ok || v == nil {
		return defaultPriority
	}
	switch val := v.(type) {
	case string:
		res, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return defaultPriority
		}
		return res
	case float32:
		return int64(val)
	case float64:
		return int64(val)
	case int:
		return int64(val)
	case int64:
		return val
	case int32:
		return int64(val)
	case int16:
		return int64(val)
	case int8:
		return int64(val)
	default:
		return defaultPriority
	}
}

// Get used to get the data associated with the key
func (p Pipeline) Get(key string) any {
	return p[key]
}

func strToBytes(data string) []byte {
	if data == "" {
		return nil
	}

	return unsafe.Slice(unsafe.StringData(data), len(data))
}
