package observe

import (
	"fmt"
	"strconv"
)

// Attr is a structured log/metric/trace attribute (OpenTelemetry compatible key naming).
type Attr struct {
	Key   string
	Value any
}

// String returns a string attribute.
func String(k, v string) Attr { return Attr{Key: k, Value: v} }

// Int64 returns an int64 attribute.
func Int64(k string, v int64) Attr { return Attr{Key: k, Value: v} }

// Int returns an int attribute.
func Int(k string, v int) Attr { return Attr{Key: k, Value: int64(v)} }

// Bool returns a bool attribute.
func Bool(k string, v bool) Attr { return Attr{Key: k, Value: v} }

// Float64 returns a float64 attribute.
func Float64(k string, v float64) Attr { return Attr{Key: k, Value: v} }

// Error returns an error attribute.
func Error(err error) Attr { return Attr{Key: "error", Value: err} }

func formatValue(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case int:
		return strconv.Itoa(x)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case error:
		if x == nil {
			return ""
		}
		return x.Error()
	default:
		return fmt.Sprint(x)
	}
}
