package observe

import (
	"encoding/json"
	"errors"
	"reflect"
)

// ErrorDetail allows errors to expose structured fields for logging and APM.
type ErrorDetail interface {
	ErrorDetail() map[string]any
}

// ErrorObject converts an error into a structured map for JSON/ECS logging.
func ErrorObject(err error) map[string]any {
	if err == nil {
		return nil
	}
	out := map[string]any{
		"type":    errorTypeName(err),
		"message": err.Error(),
	}
	if ed, ok := err.(ErrorDetail); ok {
		for k, v := range ed.ErrorDetail() {
			out[k] = v
		}
	}
	return out
}

// ErrorJSON marshals an error as JSON bytes.
func ErrorJSON(err error) []byte {
	b, _ := json.Marshal(ErrorObject(err))
	return b
}

func errorTypeName(err error) string {
	if err == nil {
		return ""
	}
	var ed ErrorDetail
	if errors.As(err, &ed) {
		t := reflect.TypeOf(ed)
		if t != nil {
			if t.Kind() == reflect.Ptr {
				return t.Elem().Name()
			}
			return t.Name()
		}
	}
	return "error"
}
