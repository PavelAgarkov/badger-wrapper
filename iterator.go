package badger_sdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
)

type RawIterator struct {
	engine *Engine
}

func NewRawIterator(engine *Engine) *RawIterator {
	return &RawIterator{
		engine: engine,
	}
}

type (
	OnLanesFunc = func(ctx context.Context, pks [][]byte, vals [][]byte) error
	OnRow       = func(ctx context.Context, pk []byte, value []byte) error
	Iterator    interface {
		ScanByIndexPrefixRaw(ctx context.Context, indexPrefix []byte, limit int, onRow OnRow) error
		ScanAllByPKPrefixRaw(ctx context.Context, pkPrefix []byte, limit int, onRow OnRow) error
		PageByPKPrefixRaw(ctx context.Context, pkPrefix []byte, cursor []byte, limit int) (pks [][]byte, values [][]byte, nextCursor []byte, err error)
		PageByIndexPrefixRaw(ctx context.Context, indexPrefix []byte, cursor []byte, limit int) (pks [][]byte, values [][]byte, nextCursor []byte, err error)
		GetOneByPKRaw(ctx context.Context, pk []byte) ([]byte, error)

		IterationByPkPrefix(ctx context.Context, prefix []byte, onLanes OnLanesFunc, cursor []byte, pageSize int) ([]byte, error)
		IterationByIndexPrefix(ctx context.Context, prefix []byte, onLanes OnLanesFunc, cursor []byte, pageSize int) ([]byte, error)

		FirstByPKPrefixRaw(ctx context.Context, prefix []byte) (pk []byte, value []byte, err error)
		LastByPKPrefixRaw(ctx context.Context, prefix []byte) (pk []byte, value []byte, err error)
		FirstByIndexPrefixRaw(ctx context.Context, prefix []byte) (pk []byte, value []byte, err error)
		LastByIndexPrefixRaw(ctx context.Context, prefix []byte) (pk []byte, value []byte, err error)
	}
)

// GetOneByPKRaw — получить запись по PK и вернуть байты значения.
// Возвращает ErrNotFound, если ключа нет.
func (ri *RawIterator) GetOneByPKRaw(ctx context.Context, pk []byte) ([]byte, error) {
	var out []byte
	err := ri.engine.DB().View(func(txn *badger.Txn) error {
		item, err := txn.Get(pk)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return fmt.Errorf("[GetOneByPKRaw] GetOneByPKRaw: %w", err)
			}
			return fmt.Errorf("txn.Get: %w", err)
		}
		err = item.Value(func(v []byte) error {
			out = append(out[:0], v...)
			return nil
		})
		if err != nil {
			return fmt.Errorf("[GetOneByPKRaw] item.Value: %w", err)
		}
		return nil
	})
	return out, err
}

// ScanByIndexPrefixRaw — итерирует по индексным ключам с заданным префиксом.
// Значение индексной записи — это PK. Для каждой записи вызывает onRow(pk, valueBytes_of_PK).
// Параметр limit — максимум записей за проход (<=0 = без лимита).
func (ri *RawIterator) ScanByIndexPrefixRaw(
	ctx context.Context,
	indexPrefix []byte,
	limit int,
	onRow OnRow,
) error {
	return ri.engine.DB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true // value индексного ключа — это PK
		opts.Prefix = indexPrefix  // оптимизация итератора

		it := txn.NewIterator(opts)
		defer it.Close()

		count := 0
		for it.Seek(indexPrefix); it.ValidForPrefix(indexPrefix); it.Next() {
			item := it.Item()

			// 1) достаём PK из value индексного ключа
			var pk []byte
			if err := item.Value(func(v []byte) error {
				pk = append(pk[:0], v...)
				return nil
			}); err != nil {
				return fmt.Errorf("[ScanByIndexPrefixRaw] item.Value: %w", err)
			}

			// 2) читаем основную запись по PK
			dataItem, err := txn.Get(pk)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					// «висячий» индекс — пропускаем
					continue
				}
				return fmt.Errorf("[ScanByIndexPrefixRaw] txn.Get: %w", err)
			}

			var val []byte
			if err := dataItem.Value(func(v []byte) error {
				val = append(val[:0], v...)
				return nil
			}); err != nil {
				return fmt.Errorf("[ScanByIndexPrefixRaw] dataItem.Value: %w", err)
			}

			if err := onRow(ctx, pk, val); err != nil {
				return fmt.Errorf("[ScanByIndexPrefixRaw] onRow: %w", err)
			}
			count++
			if limit > 0 && count >= limit {
				break
			}
		}
		return nil
	})
}

// ScanAllByPKPrefixRaw — итерирует по записям с PK, начинающимися с prefix.
// Для каждой записи отдаёт (pk, valueBytes) в onRow.
func (ri *RawIterator) ScanAllByPKPrefixRaw(
	ctx context.Context,
	pkPrefix []byte,
	limit int,
	onRow OnRow,
) error {
	return ri.engine.DB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.Prefix = pkPrefix

		it := txn.NewIterator(opts)
		defer it.Close()

		count := 0
		for it.Seek(pkPrefix); it.ValidForPrefix(pkPrefix); it.Next() {
			item := it.Item()

			// Ключ (PK)
			pk := item.KeyCopy(nil)

			// Значение
			var val []byte
			if err := item.Value(func(v []byte) error {
				val = append(val[:0], v...)
				return nil
			}); err != nil {
				return fmt.Errorf("[ScanAllByPKPrefixRaw] item.Value: %w", err)
			}

			if err := onRow(ctx, pk, val); err != nil {
				return fmt.Errorf("[ScanAllByPKPrefixRaw] onRow: %w", err)
			}
			count++
			if limit > 0 && count >= limit {
				break
			}
		}
		return nil
	})
}

// PageByPKPrefixRaw — постраничный обход PK-пространства.
// Возвращает до limit элементов: pks[i] — полный PK-ключ, values[i] — байты значения.
// nextCursor — ПОЛНЫЙ PK-ключ, с которого продолжать следующий вызов (nil/[] — больше нет страниц).
// example:
// var (
//
//	cursor []byte // nil для первой страницы
//	pageSize   = 100
//
// )
// for {
// // PK-вариант:
// pks, vals, next, err := PageByPKPrefixRaw(ctx, storage, pkPrefix, cursor, pageSize)
// if err != nil { /* handle */ break }
//
// // обработка текущей страницы
// for i := range pks {
// // pks[i] — полный PK; vals[i] — bytes value
// }
//
// if len(next) == 0 { // nextCursor пуст — страниц больше нет
// break
// }
// cursor = next // курсор на следующую итерацию
// }
func (ri *RawIterator) PageByPKPrefixRaw(
	ctx context.Context,
	pkPrefix []byte,
	cursor []byte, // полный PK-ключ, после которого начать; nil/[] — с начала
	limit int,
) (pks [][]byte, values [][]byte, nextCursor []byte, err error) {

	err = ri.engine.DB().View(func(txn *badger.Txn) error {
		itOpts := badger.DefaultIteratorOptions
		itOpts.PrefetchValues = true
		itOpts.Prefix = pkPrefix

		it := txn.NewIterator(itOpts)
		defer it.Close()

		// Точка старта
		if len(cursor) > 0 {
			it.Seek(cursor)
			if it.Valid() && bytes.Equal(it.Item().Key(), cursor) {
				it.Next() // начать ПОСЛЕ cursor
			}
		} else {
			it.Seek(pkPrefix)
		}

		count := 0
		var lastKey []byte
		for ; it.ValidForPrefix(pkPrefix); it.Next() {
			item := it.Item()

			pk := item.KeyCopy(nil)
			var val []byte
			if err := item.Value(func(v []byte) error {
				val = append(val[:0], v...)
				return nil
			}); err != nil {
				return fmt.Errorf("[PageByPKPrefixRaw] item.Value: %w", err)
			}

			pks = append(pks, pk)
			values = append(values, val)
			lastKey = pk // запоминаем последний реально отданный PK
			count++

			if limit > 0 && count >= limit {
				nextCursor = lastKey
				break
			}
		}

		// если страница короткая, но что-то отдали — тоже вернём lastKey
		if nextCursor == nil && count > 0 {
			nextCursor = lastKey
		}
		return nil
	})
	return
}

// PageByIndexPrefixRaw — постраничный обход ИНДЕКС-префикса.
// Возвращает до limit элементов: pks[i] — PK основной записи, values[i] — байты значения основной записи.
// nextCursor — ПОЛНЫЙ ИНДЕКСНЫЙ ключ текущего элемента (его и передавайте в следующий вызов как cursor).
// example:
// var cursor []byte
//
//	for {
//	   pks, vals, next, err := PageByIndexPrefixRaw(ctx, storage, indexPrefix, cursor, 100)
//	   if err != nil { /* handle */ break }
//
//	   for i := range pks {
//	       // pks[i] — PK основной записи; vals[i] — bytes основной записи
//	   }
//
//	   if len(next) == 0 { break }
//	   cursor = next // здесь это ПОЛНЫЙ ИНДЕКСНЫЙ ключ
//	}
func (ri *RawIterator) PageByIndexPrefixRaw(
	ctx context.Context,
	indexPrefix []byte,
	cursor []byte, // ПОЛНЫЙ индексный ключ, после которого начать; nil/[] — с начала
	limit int,
) (pks [][]byte, values [][]byte, nextCursor []byte, err error) {

	err = ri.engine.DB().View(func(txn *badger.Txn) error {
		itOpts := badger.DefaultIteratorOptions
		itOpts.PrefetchValues = true // value индексной записи — это PK
		itOpts.Prefix = indexPrefix

		it := txn.NewIterator(itOpts)
		defer it.Close()

		// Старт
		if len(cursor) > 0 {
			it.Seek(cursor)
			if it.Valid() && bytes.Equal(it.Item().Key(), cursor) {
				it.Next()
			}
		} else {
			it.Seek(indexPrefix)
		}

		count := 0
		var lastKey []byte

		for ; it.ValidForPrefix(indexPrefix); it.Next() {
			item := it.Item()

			// 1) достаём PK из значения индексного ключа
			var pk []byte
			if err := item.Value(func(v []byte) error {
				pk = append(pk[:0], v...)
				return nil
			}); err != nil {
				return fmt.Errorf("[PageByIndexPrefixRaw] item.Value: %w", err)
			}

			// 2) читаем основную запись по PK
			dataItem, err := txn.Get(pk)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					// висячий индекс
					continue
				}
				return fmt.Errorf("[PageByIndexPrefixRaw] txn.Get: %w", err)
			}
			var val []byte
			if err := dataItem.Value(func(v []byte) error {
				val = append(val[:0], v...)
				return nil
			}); err != nil {
				return fmt.Errorf("[PageByIndexPrefixRaw] dataItem.Value: %w", err)
			}

			pks = append(pks, append([]byte(nil), pk...)) // копия pk
			values = append(values, val)
			lastKey = item.KeyCopy(nil) // запоминаем ключ той записи, которую реально вернули
			count++

			if limit > 0 && count >= limit {
				nextCursor = lastKey // ПОЛНЫЙ индексный ключ
				break
			}
		}
		// Если дошли до конца (меньше limit), тоже вернём указатель на последний обработанный ключ.
		// Это удобно для продолжения позже (после появления новых записей).
		if nextCursor == nil && count > 0 {
			nextCursor = lastKey
		}

		return nil
	})
	return
}

func (ri *RawIterator) IterationByPkPrefix(
	ctx context.Context,
	prefix []byte,
	onLanes OnLanesFunc,
	cursor []byte,
	pageSize int,
) ([]byte, error) {
	for {
		pks, vals, next, err := ri.engine.PageByPKPrefixRaw(
			ctx,
			prefix,
			cursor,
			pageSize, // limit
		)
		if err != nil {
			return nil, fmt.Errorf("IterationByPkPrefix error: %v", err)
		}

		err = onLanes(ctx, pks, vals)
		if err != nil {
			return nil, fmt.Errorf("onLanes error: %w", err)
		}

		if len(next) == 0 {
			break
		}
		cursor = next // ПОЛНЫЙ PK-ключ текущего элемента: начнём со следующего на следующей итерации
	}
	return cursor, nil
}

func (ri *RawIterator) IterationByIndexPrefix(
	ctx context.Context,
	prefix []byte,
	onLanes OnLanesFunc,
	cursor []byte,
	pageSize int,
) ([]byte, error) {
	for {
		pks, vals, next, err := ri.engine.PageByIndexPrefixRaw(
			ctx,
			prefix,
			cursor,   // cursor (полный индексный ключ предыдущего элемента страницы)
			pageSize, // limit
		)
		if err != nil {
			return nil, fmt.Errorf("IterationByIndexPrefix error: %v", err)
		}

		err = onLanes(ctx, pks, vals)
		if err != nil {
			return nil, fmt.Errorf("onLanes error: %w", err)
		}

		if len(next) == 0 {
			break
		}
		cursor = next // ПОЛНЫЙ индексный ключ текущего элемента: начнём со следующего на следующей итерации
	}
	return cursor, nil
}

func (ri *RawIterator) FirstByPKPrefixRaw(ctx context.Context, prefix []byte) (pk []byte, value []byte, err error) {
	err = ri.engine.DB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.Prefix = prefix

		it := txn.NewIterator(opts)
		defer it.Close()

		it.Seek(prefix)
		if !it.ValidForPrefix(prefix) {
			return ErrNotFound
		}
		item := it.Item()

		pk = item.KeyCopy(nil)
		if err := item.Value(func(v []byte) error {
			value = append(value[:0], v...)
			return nil
		}); err != nil {
			return fmt.Errorf("[FirstByPKPrefixRaw] item.Value: %w", err)
		}
		return nil
	})
	return
}

func (ri *RawIterator) LastByPKPrefixRaw(ctx context.Context, prefix []byte) (pk []byte, value []byte, err error) {
	err = ri.engine.DB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.Prefix = prefix
		opts.Reverse = true

		it := txn.NewIterator(opts)
		defer it.Close()

		// seek «чуть правее» диапазона, чтобы попасть на последний ключ с префиксом
		end := append(append([]byte(nil), prefix...), 0xFF)
		it.Seek(end)
		if !it.ValidForPrefix(prefix) {
			return ErrNotFound
		}
		item := it.Item()

		pk = item.KeyCopy(nil)
		if err := item.Value(func(v []byte) error {
			value = append(value[:0], v...)
			return nil
		}); err != nil {
			return fmt.Errorf("[LastByPKPrefixRaw] item.Value: %w", err)
		}
		return nil
	})
	return
}

func (ri *RawIterator) FirstByIndexPrefixRaw(ctx context.Context, prefix []byte) (pk []byte, value []byte, err error) {
	err = ri.engine.DB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true // value индексного ключа = PK
		opts.Prefix = prefix

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			// достаём PK из value индексной записи
			var gotPK []byte
			if err := item.Value(func(v []byte) error {
				gotPK = append(gotPK[:0], v...)
				return nil
			}); err != nil {
				return fmt.Errorf("[FirstByIndexPrefixRaw] idx.Value: %w", err)
			}

			// читаем основную запись
			dataItem, err := txn.Get(gotPK)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					// висячий индекс — пропустим и пойдём дальше
					continue
				}
				return fmt.Errorf("[FirstByIndexPrefixRaw] txn.Get: %w", err)
			}

			var val []byte
			if err := dataItem.Value(func(v []byte) error {
				val = append(val[:0], v...)
				return nil
			}); err != nil {
				return fmt.Errorf("[FirstByIndexPrefixRaw] dataItem.Value: %w", err)
			}

			pk = append(pk[:0], gotPK...)
			value = append(value[:0], val...)
			return nil
		}
		return ErrNotFound
	})
	return
}

func (ri *RawIterator) LastByIndexPrefixRaw(ctx context.Context, prefix []byte) (pk []byte, value []byte, err error) {
	err = ri.engine.DB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.Prefix = prefix
		opts.Reverse = true

		it := txn.NewIterator(opts)
		defer it.Close()

		end := append(append([]byte(nil), prefix...), 0xFF)
		for it.Seek(end); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			// PK из value индексной записи
			var gotPK []byte
			if err := item.Value(func(v []byte) error {
				gotPK = append(gotPK[:0], v...)
				return nil
			}); err != nil {
				return fmt.Errorf("[LastByIndexPrefixRaw] idx.Value: %w", err)
			}

			// основная запись
			dataItem, err := txn.Get(gotPK)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					// висячий индекс — пробуем предыдущий (мы в Reverse, it.Next идёт влево)
					continue
				}
				return fmt.Errorf("[LastByIndexPrefixRaw] txn.Get: %w", err)
			}

			var val []byte
			if err := dataItem.Value(func(v []byte) error {
				val = append(val[:0], v...)
				return nil
			}); err != nil {
				return fmt.Errorf("[LastByIndexPrefixRaw] dataItem.Value: %w", err)
			}

			pk = append(pk[:0], gotPK...)
			value = append(value[:0], val...)
			return nil
		}
		return ErrNotFound
	})
	return
}
