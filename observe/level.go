package observe

import "strings"

// Level is a log severity level (syslog-style ordering).
type Level int8

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	default:
		return "info"
	}
}

// ParseLevel parses a log level string (case-insensitive).
func ParseLevel(s string) Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug", "trace":
		return LevelDebug
	case "warn", "warning":
		return LevelWarn
	case "error", "fatal":
		return LevelError
	default:
		return LevelInfo
	}
}

func (l Level) enabled(min Level) bool { return l >= min }
