package gokafka

import (
	"context"
	"sync"
	"time"
)

// BatchProducer accumulates records until batch size or linger timeout, then flushes.
type BatchProducer struct {
	client   *Client
	inner    *Producer
	mu       sync.Mutex
	pending  []Record
	timer    *time.Timer
	linger   time.Duration
	batchMax int
}

// NewBatchProducer creates a producer that respects BatchSize and Linger settings.
func (c *Client) NewBatchProducer() *BatchProducer {
	linger := c.cfg.Producer.Linger
	if linger <= 0 {
		linger = 5 * time.Millisecond
	}
	batchMax := c.cfg.Producer.BatchSize
	if batchMax <= 0 {
		batchMax = 100
	}
	return &BatchProducer{
		client:   c,
		inner:    c.Producer(),
		linger:   linger,
		batchMax: batchMax,
	}
}

// Send adds a record to the batch; flushes when full or after linger.
func (b *BatchProducer) Send(ctx context.Context, r Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending = append(b.pending, r)
	if len(b.pending) >= b.batchMax {
		return b.flushLocked(ctx)
	}
	if b.timer == nil {
		b.timer = time.AfterFunc(b.linger, func() {
			_ = b.Flush(context.Background())
		})
	}
	return nil
}

// Flush sends all pending records immediately.
func (b *BatchProducer) Flush(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.flushLocked(ctx)
}

func (b *BatchProducer) flushLocked(ctx context.Context) error {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	if len(b.pending) == 0 {
		return nil
	}
	batch := b.pending
	b.pending = nil
	return b.inner.ProduceSync(ctx, batch...)
}

// Close flushes pending records.
func (b *BatchProducer) Close(ctx context.Context) error {
	return b.Flush(ctx)
}

