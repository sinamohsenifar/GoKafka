package retry

import (
	"context"
	"time"
)

// Do executes fn with exponential backoff until success or attempts exhausted.
func Do(ctx context.Context, max int, base, maxWait time.Duration, fn func() error) error {
	if max < 1 {
		max = 1
	}
	wait := base
	var err error
	for attempt := 0; attempt < max; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = fn()
		if err == nil {
			return nil
		}
		if attempt == max-1 {
			break
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		wait *= 2
		if wait > maxWait {
			wait = maxWait
		}
	}
	return err
}
