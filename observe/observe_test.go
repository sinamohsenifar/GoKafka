package observe_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/sinamohsenifar/gokafka"
	"github.com/sinamohsenifar/gokafka/observe"
)

func TestJSONLogger(t *testing.T) {
	var buf bytes.Buffer
	l := observe.NewLogger(observe.LoggerConfig{
		Level: observe.LevelInfo, Format: observe.LogFormatJSON, Output: &buf,
	})
	l.Log(context.Background(), observe.LevelInfo, "hello", observe.String("kafka.topic", "t"))
	if !strings.Contains(buf.String(), `"message":"hello"`) {
		t.Fatalf("json log: %s", buf.String())
	}
}

func TestECSLogger(t *testing.T) {
	var buf bytes.Buffer
	l := observe.NewLogger(observe.LoggerConfig{
		Level: observe.LevelError, Format: observe.LogFormatECS, Output: &buf, ServiceName: "svc",
	})
	l.Log(context.Background(), observe.LevelError, "fail", observe.Error(gokafka.ErrClosed))
	s := buf.String()
	if !strings.Contains(s, `"log.level":"error"`) || !strings.Contains(s, `"service.name":"svc"`) {
		t.Fatalf("ecs log: %s", s)
	}
}

func TestPrometheusExport(t *testing.T) {
	c := observe.NewCollector(observe.MetricsConfig{Enabled: true, Namespace: "gokafka"})
	c.OnProduce(10, nil)
	c.OnRequest(0, 0, nil)
	var buf bytes.Buffer
	if err := observe.WritePrometheus(&buf, c.Snapshot(), "gokafka"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "gokafka_produce_records_total 1") {
		t.Fatalf("prom: %s", buf.String())
	}
}

func TestKafkaErrorDetail(t *testing.T) {
	err := &gokafka.KafkaError{Code: gokafka.ErrCodeNotLeaderForPart, Topic: "t", Partition: 1, Msg: "x"}
	obj := observe.ErrorObject(err)
	if obj["kafka.error_code"] != int(gokafka.ErrCodeNotLeaderForPart) {
		t.Fatalf("obj=%v", obj)
	}
}

func TestRecordingTracer(t *testing.T) {
	tr := &observe.RecordingTracer{}
	ctx, sp := tr.Start(context.Background(), "op", observe.String("k", "v"))
	sp.End()
	if len(tr.Spans) != 1 {
		t.Fatal("expected span")
	}
	if observe.TraceFromContext(ctx).TraceID == "" {
		t.Fatal("expected trace id in context")
	}
}
