package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/sh-rest/conveyor/internal/queue"
)

type Pool struct {
	dispatcher *Dispatcher
	consumer   *queue.Consumer
	scheduler  *queue.Scheduler
	workCh     chan queue.Message
	wg         sync.WaitGroup
}

func NewPool(d *Dispatcher, c *queue.Consumer, s *queue.Scheduler, workerCount int) *Pool {
	return &Pool{
		dispatcher: d,
		consumer:   c,
		scheduler:  s,
		workCh:     make(chan queue.Message, workerCount*2), // buffer = 2× workers for burst absorption
	}
}

// Start launches all goroutines. Blocks until the pool's context is cancelled,
// then drains in-flight work before returning.
func (p *Pool) Start(ctx context.Context, workerCount int) {
	// N worker goroutines — drain workCh, dispatch, ack
	for i := 0; i < workerCount; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for msg := range p.workCh {
				p.safeDispatch(ctx, msg)
			}
		}()
	}

	// Consumer goroutine — XREADGROUP → workCh
	// Closes workCh when it exits, which signals workers to drain and stop.
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(p.workCh)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			msgs, err := p.consumer.Read(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("consumer read error, backing off", "err", err)
				time.Sleep(time.Second)
				continue
			}

			for _, msg := range msgs {
				select {
				case p.workCh <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Scheduler goroutine — two tickers:
	//   retryTicker (5s): moves due items from zset:retry back into the stream
	//   reclaimTicker (30s): XAUTOCLAIM stale PEL entries (crash recovery)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		retryTicker := time.NewTicker(5 * time.Second)
		reclaimTicker := time.NewTicker(30 * time.Second)
		defer retryTicker.Stop()
		defer reclaimTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-retryTicker.C:
				n, err := p.scheduler.ProcessDue(ctx)
				if err != nil {
					slog.Error("scheduler error", "err", err)
				} else if n > 0 {
					slog.Info("scheduler re-enqueued retries", "count", n)
				}
			case <-reclaimTicker.C:
				n, err := p.scheduler.ReclaimStale(ctx, 60*time.Second)
				if err != nil {
					slog.Error("reclaim error", "err", err)
				} else if n > 0 {
					slog.Info("reclaimed stale deliveries", "count", n)
				}
			}
		}
	}()
}

// Wait blocks until all goroutines have exited.
func (p *Pool) Wait() {
	p.wg.Wait()
}

// safeDispatch wraps Dispatch in a recover so one bad delivery can't crash the worker goroutine.
// Uses context.WithoutCancel so in-flight deliveries finish cleanly even after SIGTERM.
func (p *Pool) safeDispatch(ctx context.Context, msg queue.Message) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("dispatcher panic — delivery skipped", "delivery_id", msg.DeliveryID, "panic", r)
		}
	}()
	// WithoutCancel: inherits values but not cancellation.
	// In-flight HTTP calls and DB writes complete even when the pool context is cancelled.
	dispatchCtx := context.WithoutCancel(ctx)
	p.dispatcher.Dispatch(dispatchCtx, msg.DeliveryID)
	p.consumer.Ack(dispatchCtx, msg.StreamID)
}
