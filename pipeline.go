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
	if p.String(name, "") != "" {
		return p.String(name, "")
	}

	return p.String(queue, "")
}

// Driver associated with the pipeline.
func (p Pipeline) Driver() string {
	return p.String(driver, "")
}

// Has checks if value presented in pipeline.
func (p Pipeline) Has(name string) bool {
	if _, ok := p[name]; ok {
		return true
	}

	// check the config section if exists
	if val, ok := p[config]; ok {
		if rv, ok := val.(map[string]any); ok {
			if _, ok := rv[name]; ok {
				return true
			}
			return false
		}
	}

	return false
}

// String must return option value as string or return default value.
func (p Pipeline) String(name string, d string) string {
	if value, ok := p[name]; ok {
		if str, ok := value.(string); ok {
			return str
		}
	} else {
		// check the config section if exists
		if val, ok := p[config]; ok {
			if rv, ok := val.(map[string]any); ok {
				if rv[name] == nil {
					return d
				}

				switch v := rv[name].(type) {
				case string:
					if v != "" {
						return v
					}
					return d
				case nil:
					return d
				default:
					return d
				}
			}
		}
	}

	return d
}

// Int must return option value as string or return default value.
func (p Pipeline) Int(name string, d int) int {
	if value, ok := p[name]; ok {
		switch v := value.(type) {
		// the most probable case
		case string:
			res, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				// return default on failure
				return d
			}

			if res > math.MaxInt32 || res < math.MinInt32 {
				// return default if out of bounds
				return d
			}

			return int(res)
		case int:
			return v
		case int64:
			return int(v)
		case int32:
			return int(v)
		case int16:
			return int(v)
		case int8:
			return int(v)
		default:
			return d
		}
	} else {
		// check the config section if exists
		if val, ok := p[config]; ok {
			if rv, ok := val.(map[string]any); ok {
				if rv[name] != nil {
					switch v := rv[name].(type) {
					case float32:
						return int(v)
					case float64:
						return int(v)
					case int:
						return v
					default:
						return 0
					}
				}
			}
		}
	}

	return d
}

// Bool must return option value as bool or return default value.
func (p Pipeline) Bool(name string, d bool) bool {
	if value, ok := p[name]; ok {
		if i, ok := value.(string); ok {
			switch i {
			case trueStr:
				return true
			case falseStr:
				return false
			default:
				return false
			}
		}
	} else {
		// check the config section if exists
		if val, ok := p[config]; ok {
			if rv, ok := val.(map[string]any); ok {
				if rv[name] != nil {
					if i, ok := value.(string); ok {
						switch i {
						case trueStr:
							return true
						case falseStr:
							return false
						default:
							return false
						}
					}
				}
			}
		}
	}

	return d
}

// Map must return nested map value or empty config.
// There might be sqs attributes or tags, for example
func (p Pipeline) Map(name string, out map[string]string) error {
	if value, ok := p[name]; ok {
		if m, ok := value.(string); ok {
			err := json.Unmarshal(strToBytes(m), &out)
			if err != nil {
				return err
			}
		}
	} else {
		// check the config section if exists
		if val, ok := p[config]; ok {
			if rv, ok := val.(map[string]any); ok {
				if val, ok := rv[name]; ok {
					if m, ok := val.(string); ok {
						err := json.Unmarshal(strToBytes(m), &out)
						if err != nil {
							return err
						}
					}
					return nil
				}
			}
		}
	}

	return nil
}

// Priority returns default pipeline priority
func (p Pipeline) Priority() int64 {
	if value, ok := p[priority]; ok {
		switch v := value.(type) {
		// the most probable case
		case string:
			res, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				// return default on failure
				return defaultPriority
			}

			return res
		case int:
			return int64(v)
		case int64:
			return v
		case int32:
			return int64(v)
		case int16:
			return int64(v)
		case int8:
			return int64(v)
		default:
			return defaultPriority
		}
	} else {
		// check the config section if exists
		if val, ok := p[config]; ok {
			if rv, ok := val.(map[string]any); ok {
				if rv[name] != nil {
					switch v := rv[name].(type) {
					case float32:
						return int64(v)
					case float64:
						return int64(v)
					case int:
						return int64(v)
					case int64:
						return v
					default:
						return defaultPriority
					}
				}
			}
		}
	}

	return defaultPriority
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
