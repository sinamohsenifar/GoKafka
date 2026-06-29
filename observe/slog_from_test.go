package observe_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/sinamohsenifar/gokafka/observe"
)

func TestNewSlogLoggerFromRoutesToHandler(t *testing.T) {
	var buf bytes.Buffer
	// A user-configured slog.Logger with a base attribute and Warn level.
	base := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})).
		With("service", "myapp")
	lg := observe.NewSlogLoggerFrom(base)

	// Debug/Info are filtered by the handler's Warn level.
	lg.Log(context.Background(), observe.LevelInfo, "should be filtered")
	if buf.Len() != 0 {
		t.Fatalf("expected info to be filtered by handler, got: %s", buf.String())
	}

	lg.Log(context.Background(), observe.LevelWarn, "boom", observe.String("topic", "t1"))
	out := buf.String()
	for _, want := range []string{`"service":"myapp"`, `"msg":"boom"`, `"topic":"t1"`, `"level":"WARN"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("log output missing %q: %s", want, out)
		}
	}
}
