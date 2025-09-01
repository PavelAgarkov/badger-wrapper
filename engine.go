package badger_sdk

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/google/uuid"
)

//данные нужно хранить в виде - idx : pk, pk : data. т.е. отдельно индекс и отдельно данные. Они связаны по PK.
//Все они лежат в одном большом ведре, например badger_in_memory, разделены только префиксом типа данных:
//data - pk основной ключ сущности, сам по себе является уникальным ключом, например: data:badger_in_memory:v1:user:id==000000000010;
//idx - это индекс или составной индекс для префиксного сканирования, например idx:badger_in_memory:v1:user:type==admin:office==508#id==000000000033;
//uniq - это индекс уникальный или составной уникальный индекс для поиска по ключу, например:uniq:badger_in_memory:v1:user:type==admin:office==507;
//например (pk) []byte{data:badger_in_memory:v1:user:id==000000000010} : (objects) []byte{v1model.User{Id: 33, Name: "Updated User 33", Type: "admin", Office: "508"}}
//(idx) - []byte{badger_in_memory:v1:user:type==admin:office==507#id==000000000010} : (pk) []byte{data:badger_in_memory:v1:user:id==000000000010}
//
//Искать данные перебором прификсов индекса через итераторы, а не по полям в данных.
//Или же прямым поиском по индексу, если он уникальный - 1 операция
//
//Правило запоминания
//Порядок полей в ключе индекса = порядок фильтров, по которым вы будете начинать поиск.
//Хотите искать сначала по office → делайте индекс, где office стоит первым.
//Для поддержки разных шаблонов запросов нужны отдельные индексы (обычно 1-2 на сущность под самые частые паттерны).
//
//Правила формирования составных индексов для префиксного поиска:
//индекс idx:badger_in_memory:v1:user:type==admin:office==507#id==000000000010
//префиксный поиск будет работать в таком случае только так idx:badger_in_memory:v1:user:type==admin
//или так idx:badger_in_memory:v1:user:type==admin:office==507.
//если хочется искать так idx:badger_in_memory:v1:user:office==507, но ничего не найдется.
//для такого запроса нужно сделать отдельный индекс, где office будет первым полем префикса после обязательных полей
//например idx:badger_in_memory:v1:user:office==507:type==admin#id==000000000010
//
//правила поиска по уникальному индексу
//если нам нужно напрямую найти idx:badger_in_memory:v1:user:type==admin:office==507, но у нас есть
//комбинированнй индекс idx:badger_in_memory:v1:user:type==admin:office==507#id==000000000010, но поиск по ключу не сработает.
//Т.к. поиск по ключу будет работать только по полному ключу, а не по префиксу (для такого случая префиксный поиск, описан выше).
//Чтобы попасть в tx.Get нужно сделать уникальный индекс и скать по нему, рекомендуется делать
//такой индекс uniq:badger_in_memory:v1:user:type==admin:office==507.
//badgerDb не умеет либо попасть сразу через tx.Get в ключ, либо делать фулскан всех индексов в БД,
//либо префиксный интератор по правильно составленным ключам. Описано выше.
//
//при вставке нужно пересоздавать все индексы, которые могут быть затронуты изменением полем и удалять старые индексы.
//считается нормой иметь 1 pk и 1-2 составных индексов на сущность, чтобы покрыть основные паттерны запросов.

var ErrNotFound = badger.ErrKeyNotFound

// BadgerEngine basic operations - группа методов вставки/удаления не защищена от конфликтов конкурентной вставки и
// не имеет механизма повторов при DetectConflicts = true. Но если DetectConflicts = false и все вставки/удаления
// синхронищируются на стороне приложения, то можно использовать эти методы.
type BadgerStorageEngine interface {
	// basic operations ------------

	// Set незащищен от конфликтов конкурентной вставки, ExecuteReadWriteWithContext -защищен и имеет механизм повторов при DetectConflicts = true
	Set(key, value []byte, ttl time.Duration) error
	Get(key []byte) ([]byte, error)
	// Delete тоже не защищен от конфликтов конкурентной вставки, ExecuteReadWriteWithContext - защищен и имеет механизм повторов при DetectConflicts = true
	Delete(key []byte) error
	SetObject(key []byte, v any, ttl time.Duration) error
	GetObject(key []byte, v any) error

	// basic operations ------------

	Close() error
	DB() *badger.DB
	// todo только для режима TempFS, удаляет артефакты на диске для всей бд
	//RemoveTempFSArtefacts(sure bool, accept bool, removeVlog bool) error

	TransactionManager
	Iterator
	Locker
	Encoder
}

type Engine struct {
	db *badger.DB
	Encoder
	TransactionManager
	Iterator
	Locker
	stopGC       chan struct{}
	cleanupToken string
}

func (engine *Engine) DB() *badger.DB {
	return engine.db
}

func OpenTempFSConnection(
	ctx context.Context,
	cfg BadgerDBMaster,
	limit MemoryLimit,
	txnManagerOptions TxnManagerOptions,
	loggingLevel LogLevel,
) (BadgerStorageEngine, func(engine *Engine), error) {
	opt := Options{
		Dir:                  cfg.Dir,
		ValueDir:             cfg.ValueDir,
		InMemory:             cfg.InMemory,
		ReadOnly:             cfg.ReadOnly,
		WithMetrics:          cfg.WithMetrics,
		GCInterval:           cfg.GCInterval,
		NumGoroutines:        cfg.NumGoroutines,
		ValueThreshold:       cfg.ValueThreshold,
		ValueLogFileSize:     cfg.ValueLogFileSize,
		BaseTableSize:        cfg.BaseTableSize,
		NumCompactors:        cfg.NumCompactors,
		ZSTDCompressionLevel: cfg.ZstdCompressionLevel,
		DetectConflicts:      cfg.DetectConflicts,
		LoggingLevel:         loggingLevel,
		NumVersionsToKeep:    cfg.NumVersionsToKeep,
		SyncWrites:           cfg.SyncWrites,
		Compression:          cfg.Compression,
	}

	if !opt.InMemory && !opt.ReadOnly {
		if opt.Dir == "" {
			return nil, nil, fmt.Errorf("empty Dir is unsafe")
		}
		// Безопасно создадим каталоги
		if err := os.MkdirAll(opt.Dir, 0o700); err != nil {
			return nil, nil, fmt.Errorf("mkdir dir %s: %w", opt.Dir, err)
		}
		if opt.ValueDir != "" && opt.ValueDir != opt.Dir {
			if err := os.MkdirAll(opt.ValueDir, 0o700); err != nil {
				return nil, nil, fmt.Errorf("mkdir value dir %s: %w", opt.ValueDir, err)
			}
		}
	}

	storage, err := open(ctx, opt, &limit)
	if err != nil {
		return nil, nil, fmt.Errorf("open badger temp fs storage: %w", err)
	}

	storage.TransactionManager = NewTransactionManager(storage, txnManagerOptions)
	storage.Iterator = NewRawIterator(storage)
	storage.Locker = NewPkLocker()
	storage.Encoder = NewEncoderByName(cfg.Encoder)

	storage.cleanupToken = uuid.New().String()
	cleanupTempFS := func(engine *Engine) {
		if storage.cleanupToken == engine.cleanupToken {
			err := removeTempFSArtefacts(engine)
			if err != nil {
				log.Printf("remove temp fs artefacts: %v", err)
			}
		}
	}

	return storage, cleanupTempFS, nil
}

func removeTempFSArtefacts(engine *Engine) error {
	dir := engine.db.Opts().Dir
	vdir := engine.db.Opts().ValueDir

	if dir == "" || dir == "/" {
		return fmt.Errorf("refuse to remove unsafe path: %q", dir)
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("remove badger dir %s: %w", dir, err)
	}
	if vdir != "" && vdir != dir {
		if vdir == "/" {
			return fmt.Errorf("refuse to remove unsafe value dir: %q", vdir)
		}
		if err := os.RemoveAll(vdir); err != nil {
			return fmt.Errorf("remove badger value dir %s: %w", vdir, err)
		}
	}
	return nil
}

func OpenOnlyInMemoryConnection(
	ctx context.Context,
	cfg BadgerDBMaster,
	txnManagerOptions TxnManagerOptions,
	loggingLevel LogLevel,
	profile string,
) (BadgerStorageEngine, error) {
	opt := Options{
		InMemory:             cfg.InMemory,
		ReadOnly:             cfg.ReadOnly,
		WithMetrics:          cfg.WithMetrics,
		GCInterval:           cfg.GCInterval,
		NumGoroutines:        cfg.NumGoroutines,
		ValueThreshold:       cfg.ValueThreshold,
		ValueLogFileSize:     cfg.ValueLogFileSize,
		BaseTableSize:        cfg.BaseTableSize,
		NumCompactors:        cfg.NumCompactors,
		ZSTDCompressionLevel: cfg.ZstdCompressionLevel,
		DetectConflicts:      cfg.DetectConflicts,
		LoggingLevel:         loggingLevel,
		NumVersionsToKeep:    cfg.NumVersionsToKeep,
	}
	memLimits := ComputeMemoryPreset(cfg.RamLimitMemory, profile)
	storage, err := open(ctx, opt, memLimits)
	if err != nil {
		return nil, fmt.Errorf("open badger in-memory storage: %w", err)
	}

	storage.TransactionManager = NewTransactionManager(storage, txnManagerOptions)
	storage.Iterator = NewRawIterator(storage)
	storage.Locker = NewPkLocker()
	storage.Encoder = NewEncoderByName(cfg.Encoder)
	//storage.temp = false

	return storage, nil
}

func open(ctx context.Context, opts Options, limit *MemoryLimit) (*Engine, error) {
	bo := badger.DefaultOptions(opts.Dir)

	// Уровень логов
	switch opts.LoggingLevel {
	case LogDebug:
		bo = bo.WithLoggingLevel(badger.DEBUG)
	case LogInfo:
		bo = bo.WithLoggingLevel(badger.INFO)
	case LogWarning:
		bo = bo.WithLoggingLevel(badger.WARNING)
	default:
		bo = bo.WithLoggingLevel(badger.ERROR)
	}

	if opts.NumVersionsToKeep > 0 {
		bo = bo.WithNumVersionsToKeep(opts.NumVersionsToKeep)
	}

	if opts.WithMetrics {
		bo = bo.WithMetricsEnabled(true)
	}

	if opts.InMemory {
		bo = bo.WithInMemory(true)
	}
	if opts.ReadOnly {
		bo = bo.WithReadOnly(true)
	}
	if opts.ValueDir != "" {
		bo = bo.WithValueDir(opts.ValueDir)
	}

	bo = bo.WithSyncWrites(opts.SyncWrites)

	if opts.NumGoroutines > 0 {
		bo = bo.WithNumGoroutines(opts.NumGoroutines)
	}

	// Кеши
	if limit != nil && limit.BlockCacheSize > 0 {
		bo = bo.WithBlockCacheSize(limit.BlockCacheSize)
	} else {
		if opts.BlockCacheSize > 0 {
			bo = bo.WithBlockCacheSize(opts.BlockCacheSize)
		}
	}

	if limit != nil && limit.IndexCacheSize > 0 {
		bo = bo.WithIndexCacheSize(limit.IndexCacheSize)
	} else {
		if opts.IndexCacheSize > 0 {
			bo = bo.WithIndexCacheSize(opts.IndexCacheSize)
		}
	}

	if limit != nil && limit.MemTableSize > 0 {
		bo = bo.WithMemTableSize(limit.MemTableSize)
	} else {
		if opts.MemTableSize > 0 {
			bo = bo.WithMemTableSize(opts.MemTableSize)
		}
	}

	if limit != nil && limit.NumMemtables > 0 {
		bo = bo.WithNumMemtables(limit.NumMemtables)
	} else {
		if opts.NumMemtables > 0 {
			bo = bo.WithNumMemtables(opts.NumMemtables)
		}
	}

	if opts.ValueThreshold > 0 {
		bo = bo.WithValueThreshold(opts.ValueThreshold)
	}
	if opts.ValueLogFileSize > 0 {
		bo = bo.WithValueLogFileSize(opts.ValueLogFileSize)
	}
	if opts.BaseTableSize > 0 {
		bo = bo.WithBaseTableSize(opts.BaseTableSize)
	}

	if opts.NumCompactors > 0 {
		bo = bo.WithNumCompactors(opts.NumCompactors)
	}

	switch opts.Compression {
	case "snappy":
		bo = bo.WithCompression(options.Snappy)
	case "zstd":
		bo = bo.WithCompression(options.ZSTD).WithZSTDCompressionLevel(opts.ZSTDCompressionLevel)
	default:
		bo = bo.WithCompression(options.None)
	}

	bo = bo.WithDetectConflicts(opts.DetectConflicts)

	if len(opts.EncryptionKey) > 0 {
		bo = bo.WithEncryptionKey(opts.EncryptionKey)
	}

	db, err := badger.Open(bo)
	if err != nil {
		return nil, err
	}

	s := &Engine{
		db:      db,
		Encoder: opts.Encoder,
		stopGC:  make(chan struct{}),
	}

	if opts.GCInterval > 0 && !opts.ReadOnly {
		GoRecover(ctx, func(ctx context.Context) {
			s.runGC(ctx, opts.GCInterval)
		})
	}

	if opts.WithMetrics && !opts.InMemory {
		GoRecover(ctx, func(ctx context.Context) {
			s.runFileModeMonitoring(ctx)
		})
	}

	return s, nil
}

func (engine *Engine) Close() error {
	close(engine.stopGC)
	err := engine.db.Close()
	if err != nil {
		return fmt.Errorf("[Close] db.Close: %w", err)
	}
	return nil
}

func (engine *Engine) Set(key, value []byte, ttl time.Duration) error {
	return engine.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(key, value)
		if ttl > 0 {
			e = e.WithTTL(ttl)
		}
		err := txn.SetEntry(e)
		if err != nil {
			return fmt.Errorf("[Set] txn.SetEntry: %w", err)
		}
		return nil
	})
}

func (engine *Engine) Get(key []byte) ([]byte, error) {
	var out []byte
	err := engine.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return fmt.Errorf("[Get] txn.Get: %w", err)
		}
		err = item.Value(func(val []byte) error {
			out = append(out[:0], val...)
			return nil
		})
		if err != nil {
			return fmt.Errorf("[Get] item.Value: %w", err)
		}
		return nil
	})
	return out, err
}

func (engine *Engine) Delete(key []byte) error {
	err := engine.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		if err != nil {
			return fmt.Errorf("[Delete] txn.Delete: %w", err)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		return fmt.Errorf("[Delete] db.Update: %w", err)
	}
	return nil
}

func (engine *Engine) SetObject(key []byte, v any, ttl time.Duration) error {
	data, err := engine.Marshal(v)
	if err != nil {
		return fmt.Errorf("[SetObject] codec.Marshal: %w", err)
	}
	err = engine.Set(key, data, ttl)
	if err != nil {
		return fmt.Errorf("[SetObject] Set: %w", err)
	}
	return nil
}

func (engine *Engine) GetObject(key []byte, v any) error {
	data, err := engine.Get(key)
	if err != nil {
		return err
	}
	if err := engine.Unmarshal(data, v); err != nil {
		return fmt.Errorf("[GetObject] codec.Unmarshal: %w", err)
	}
	return nil
}
