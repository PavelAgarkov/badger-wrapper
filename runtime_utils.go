package badger_sdk

import (
	"context"
	"log"
)

func GoRecover(ctx context.Context, fn func(ctx context.Context)) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("panic recovered: %v", r)
			}
		}()
		select {
		case <-ctx.Done():
			log.Println("goroutine cancelled before start")
			return
		default:
		}
		fn(ctx)
	}()
}
