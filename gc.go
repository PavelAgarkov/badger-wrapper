package badger_sdk

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func (engine *Engine) runGC(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	// В InMemory-режиме GC value log недоступен и не нужен.
	if engine.db.Opts().InMemory {
		log.Println("[badger] in-memory mode: skipping value log GC")
		return
	}

	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping Badger GC due to context cancellation")
			return

		case <-engine.stopGC:
			log.Println("Stopping Badger GC")
			return

		case <-timer.C:
			// Несколько попыток за один «тик» — пока реально что-то перепаковывается.
			const (
				discard = 0.5 // 50% порог
				maxRuns = 3   // не более 3 файлов за сработку таймера
			)

			for i := 0; i < maxRuns; i++ {
				err := engine.db.RunValueLogGC(discard)
				if err == nil {
					log.Printf("[badger] RunValueLogGC: pass=%d ok (discard=%.2f)", i+1, discard)
					continue
				}
				if errors.Is(err, badger.ErrNoRewrite) {
					// Нечего собирать — выходим из внутреннего цикла без шума.
					break
				}
				log.Printf("[badger] RunValueLogGC: pass=%d error: %v", i+1, err)
				break
			}

			timer.Reset(interval)
		}
	}
}
