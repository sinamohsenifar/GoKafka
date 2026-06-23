package gokafka

import (
	"context"
	"sync"
)

// ProduceResult is the delivery report for async produce.
type ProduceResult struct {
	Record Record
	Result ProduceRecordResult
	Err    error
}

// AsyncProducer sends records concurrently via a buffered channel.
type AsyncProducer struct {
	client      *Client
	prod        *Producer
	in          chan Record
	out         chan ProduceResult
	wg          sync.WaitGroup
	once        sync.Once
	closed      chan struct{}
	partitioner Partitioner
}

// NewAsyncProducer creates a producer with worker pool. Call Run to start workers.
func (c *Client) NewAsyncProducer() *AsyncProducer {
	buf := c.cfg.Concurrency.ChannelBuffer
	if buf <= 0 {
		buf = 256
	}
	return &AsyncProducer{
		client:      c,
		prod:        c.Producer(),
		in:          make(chan Record, buf),
		out:         make(chan ProduceResult, buf),
		closed:      make(chan struct{}),
		partitioner: partitionerFromConfig(c.cfg.Producer),
	}
}

// Input returns the channel for sending records.
func (a *AsyncProducer) Input() chan<- Record { return a.in }

// Results returns delivery reports with offsets when successful.
func (a *AsyncProducer) Results() <-chan ProduceResult { return a.out }

// Run starts producer workers until ctx is cancelled.
func (a *AsyncProducer) Run(ctx context.Context) {
	a.prod.partitioner = a.partitioner
	workers := a.client.cfg.Concurrency.ProducerWorkers
	for i := 0; i < workers; i++ {
		a.wg.Add(1)
		go a.worker(ctx)
	}
	a.wg.Wait()
	close(a.out)
}

func (a *AsyncProducer) worker(ctx context.Context) {
	defer a.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.closed:
			return
		case r, ok := <-a.in:
			if !ok {
				return
			}
			results, err := a.prod.ProduceSyncResult(ctx, r)
			res := ProduceResult{Record: r, Err: err}
			if err == nil && len(results) > 0 {
				res.Result = results[0]
			}
			select {
			case a.out <- res:
			case <-ctx.Done():
				return
			}
		}
	}
}

// Close stops accepting new records.
func (a *AsyncProducer) Close() {
	a.once.Do(func() {
		close(a.closed)
		close(a.in)
	})
}

// Send enqueues a record.
func (a *AsyncProducer) Send(ctx context.Context, r Record) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.closed:
		return ErrClosed
	case a.in <- r:
		return nil
	}
}
