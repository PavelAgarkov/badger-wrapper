package main

import (
	"context"
	"fmt"
	"time"

	sdk "github.com/PavelAgarkov/badger-wrapper"
)

func main() {
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	badgerStorageEngine, err := sdk.OpenOnlyInMemoryConnection(
		baseCtx,
		sdk.BadgerDBMaster{
			InMemory:             true,
			ReadOnly:             false,
			WithMetrics:          true,
			GCInterval:           600 * time.Second,
			NumGoroutines:        2,
			NumCompactors:        4,
			ZstdCompressionLevel: 3,
			DetectConflicts:      true,
			Encoder:              "proto",
			NumVersionsToKeep:    1,
			ValueThreshold:       1024 * sdk.B,
			ValueLogFileSize:     128 * sdk.MiB,
			BaseTableSize:        64 * sdk.MiB,
			RamLimitMemory:       10 * sdk.GiB,
		},
		sdk.TxnManagerOptions{
			MaxRetries:  5,
			BaseBackoff: 5 * time.Millisecond,
			MaxBackoff:  150 * time.Millisecond,
		},
		sdk.GetLevelByName("ERROR"),
		sdk.ReadWriteLoad,
	)
	if err != nil {
		fmt.Println("Failed to open Badger storage:", err)
		return
	}
	defer badgerStorageEngine.Close()

	const (
		DefaultBD        = "badger_in_memory"
		DefaultVersion   = "v1"
		DefaultUserTable = "user"
	)
	db := DefaultBD
	ver := DefaultVersion
	table := DefaultUserTable
	parts := []sdk.IndexPart{{Field: "type", Value: "admin"}, {Field: "office", Value: "508"}}
	//parts := []sdk.IndexPart{{Field: "type", Value: "manager"}}
	//parts := []sdk.IndexPart{{Field: "office", Value: "507"}} // ничего не найдет т.к. office не в начале префикса
	//parts := []sdk.IndexPart{{Field: "type", Value: "admin"}}
	prefix := sdk.BuildCompositeIndexPrefix(db, ver, table, parts)
	fmt.Println(string(prefix) + " <- index prefix") // idx:badger_in_memory:v1:user:type==admin#

	pkprefix := sdk.BuildPKPrefix(db, ver, table)
	fmt.Println(string(pkprefix) + " <- pk prefix")
	sdk.Demonstrate(badgerStorageEngine, db, ver, table, prefix, pkprefix)

	if err := badgerStorageEngine.Close(); err != nil {
		fmt.Println("Failed to close Badger storage:", err)
	} else {
		fmt.Println("Badger storage closed successfully.")
	}
}
