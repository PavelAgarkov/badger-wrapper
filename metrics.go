package badger_sdk

import (
	"context"
	"log"
	"time"
)

func (engine *Engine) BadgerMemStats() {
	bc := engine.db.BlockCacheMetrics()
	ic := engine.db.IndexCacheMetrics()

	// текущая загрузка (не уходим в минус)
	blockUsed := int64(0)
	if a, e := int64(bc.CostAdded()), int64(bc.CostEvicted()); a > e {
		blockUsed = a - e
	}
	indexUsed := int64(0)
	if a, e := int64(ic.CostAdded()), int64(ic.CostEvicted()); a > e {
		indexUsed = a - e
	}

	blockCap := engine.db.Opts().BlockCacheSize
	indexCap := engine.db.Opts().IndexCacheSize
	lsmSize, vlogSize := engine.db.Size() // байты

	pct := func(used, cap int64) int {
		if cap <= 0 {
			return 0
		}
		return int((float64(used) / float64(cap)) * 100.0)
	}
	mib := func(b int64) int64 { return b >> 20 }

	log.Printf(
		"[Badger]"+
			" BlockCache: used=%d MiB / %d MiB (%d%%), hits=%d, misses=%d"+
			" IndexCache: used=%d MiB / %d MiB (%d%%), hits=%d, misses=%d"+
			" OnDisk: LSM=%d MiB, VLog=%d MiB",
		mib(blockUsed), mib(blockCap), pct(blockUsed, blockCap), bc.Hits(), bc.Misses(),
		mib(indexUsed), mib(indexCap), pct(indexUsed, indexCap), ic.Hits(), ic.Misses(),
		mib(lsmSize), mib(vlogSize),
	)
}

func (engine *Engine) runFileModeMonitoring(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			engine.BadgerMemStats()
		}
	}
}
