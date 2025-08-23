package badger_sdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	v1model "github.com/PavelAgarkov/badger-wrapper/protobuf_example/badger_interface/v1/core"
	"github.com/dgraph-io/badger/v4"
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

//	const (
//		DefaultBD        = "badger_in_memory"
//		DefaultVersion   = "v1"
//		DefaultUserTable = "user"
//	)
//	db := DefaultBD
//	ver := DefaultVersion
//	table := DefaultUserTable
//	parts := []sdk.IndexPart{{Field: "type", Value: "admin"}, {Field: "office", Value: "507"}}
//	//parts := []sdk.IndexPart{{Field: "type", Value: "manager"}}
//	//parts := []sdk.IndexPart{{Field: "office", Value: "507"}} // ничего не найдет т.к. office не в начале префикса
//	//parts := []sdk.IndexPart{{Field: "type", Value: "admin"}}
//	prefix := sdk.BuildCompositeIndexPrefix(db, ver, table, parts)
//	fmt.Println(string(prefix) + " <- index prefix") // idx:badger_in_memory:v1:user:type==admin#
//
//	pkprefix := sdk.BuildPKPrefix(db, ver, table)
//	fmt.Println(string(pkprefix) + " <- pk prefix")

// // Demonstrate - демонстрация работы с BadgerStorageEngine, вставка, обновление, удаление, индексы и итераторы
func Demonstrate(engine BadgerStorageEngine, db, ver, table string, prefix, pkprefix []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := engine.ExecuteReadWriteWithContext(context.Background(), func(_ context.Context, tx *badger.Txn) error {
		newUser := v1model.User{
			Id:     10,
			Name:   "Updated User 10",
			Type:   "admin",
			Office: "507",
		}

		//idStr := ZeroPadID(uint64(newUser.Id), 12)
		pk := BuildPKKey(db, ver, table, Uint64ToFixedWidthBytes(uint64(newUser.Id), 12))
		fmt.Println(string(pk) + " <- PK key")

		var old v1model.User
		if item, err := tx.Get(pk); err == nil {
			if err := item.Value(
				func(v []byte) error {
					return engine.Unmarshal(v, &old)
				},
			); err != nil {
				return fmt.Errorf("unmarshal old: %w", err)
			}
		} else if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("get pk: %w", err)
		}

		if old.Type != "" && old.Type != newUser.Type {
			oldIdxParts := []IndexPart{{Field: "type", Value: old.Type}, {Field: "office", Value: old.Office}}
			oldIdx := BuildCompositeIndexKey(db, ver, table, oldIdxParts, Uint64ToFixedWidthBytes(uint64(newUser.Id), 12))
			fmt.Println(string(oldIdx) + " <- old index key")
			if err := tx.Delete(oldIdx); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return fmt.Errorf("delete old index(type=%q): %w", old.Type, err)
			}
		}

		payload, err := engine.Marshal(&newUser)
		if err != nil {
			return fmt.Errorf("marshal user: %w", err)
		}
		if err := tx.Set(pk, payload); err != nil {
			return fmt.Errorf("set pk: %w", err)
		}

		if newUser.Type != "" {
			newIdxParts := []IndexPart{{Field: "type", Value: newUser.Type}, {Field: "office", Value: newUser.Office}}
			newIdx := BuildCompositeIndexKey(db, ver, table, newIdxParts, Uint64ToFixedWidthBytes(uint64(newUser.Id), 12))
			fmt.Println(string(newIdx) + " <- new index key") // idx:badger_in_memory:v1:user:type==admin#id==000000000010

			if err := tx.Set(newIdx, pk); err != nil {
				return fmt.Errorf("set index(type): %w", err)
			}

			uniq := BuildCompositeUniqueIndexKey(db, ver, table,
				[]IndexPart{{Field: "type", Value: newUser.Type}, {Field: "office", Value: newUser.Office}, {Field: "id", Value: strconv.FormatInt(newUser.Id, 10)}})

			if err := tx.Set(uniq, pk); err != nil {
				return fmt.Errorf("set unique index(type, office): %w", err)
			}
			fmt.Println(string(uniq) + " <- unique key") // uniq:badger_in_memory:v1:user:type==admin:office==507

			var foundPk []byte
			if item, err := tx.Get(uniq); err == nil {
				if err := item.Value(
					func(v []byte) error {
						foundPk = append(foundPk[:0], v...)
						return nil
					},
				); err != nil {
					return fmt.Errorf("unmarshal old: %w", err)
				}
			} else if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return fmt.Errorf("get pk: %w", err)
			}

			fmt.Println(string(foundPk) + " <- unique key found pk")
		}
		return nil
	}); err != nil {
		fmt.Println("upsert(10):", err)
	}

	_ = engine.ExecuteReadWriteWithContext(context.Background(), func(_ context.Context, tx *badger.Txn) error {
		u := v1model.User{Id: 22, Name: "Updated User 22", Type: "admin", Office: "507"}
		pk := BuildPKKey(db, ver, table, Uint64ToFixedWidthBytes(uint64(u.Id), 12))
		b, _ := engine.Marshal(&u)
		if err := tx.Set(pk, b); err != nil {
			return err
		}
		idx := BuildCompositeIndexKey(db, ver, table, []IndexPart{{Field: "type", Value: u.Type}, {Field: "office", Value: u.Office}}, Uint64ToFixedWidthBytes(uint64(u.Id), 12))
		return tx.Set(idx, pk)
	})
	// вставка с составным индексом в транзакции
	_ = engine.ExecuteReadWriteWithContext(context.Background(), func(_ context.Context, tx *badger.Txn) error {
		u := v1model.User{Id: 33, Name: "Updated User 33", Type: "admin", Office: "508"}
		pk := BuildPKKey(db, ver, table, Uint64ToFixedWidthBytes(uint64(u.Id), 12))
		b, _ := engine.Marshal(&u)
		if err := tx.Set(pk, b); err != nil {
			return err
		}
		idx := BuildCompositeIndexKey(db, ver, table, []IndexPart{{Field: "type", Value: u.Type}, {Field: "office", Value: u.Office}}, Uint64ToFixedWidthBytes(uint64(u.Id), 12))
		return tx.Set(idx, pk)
	})

	{
		// вставка с составным уникальным индексом без транзакции
		u := v1model.User{Id: 44, Name: "Updated User 44", Type: "manager", Office: "508"}
		pk := BuildPKKey(db, ver, table, Uint64ToFixedWidthBytes(uint64(u.Id), 12))
		err := engine.SetObject(pk, &u, 0) // без индекса
		if err != nil {
			fmt.Println("set pk:", err)
		}
		idx := BuildCompositeIndexKey(db, ver, table, []IndexPart{{Field: "type", Value: u.Type}, {Field: "office", Value: u.Office}}, Uint64ToFixedWidthBytes(uint64(u.Id), 12))
		if err := engine.Set(idx, pk, 0); err != nil {
			fmt.Println("set index:", err)
		}
	}

	// прификсный поиск по составному индексу
	//parts := []IndexPart{{Field: "type", Value: "admin"}, {Field: "office", Value: "507"}}
	//parts := []sdk.IndexPart{{Field: "type", Value: "manager"}}
	//parts := []sdk.IndexPart{{Field: "office", Value: "507"}} // ничего не найдет т.к. office не в начале префикса
	//parts := []sdk.IndexPart{{Field: "type", Value: "admin"}}
	//prefix := BuildCompositeIndexPrefix(db, ver, table, parts)
	//fmt.Println(string(prefix) + " <- index prefix") // idx:badger_in_memory:v1:user:type==admin#

	//pkprefix := BuildPKPrefix(db, ver, table)
	//fmt.Println(string(pkprefix) + " <- pk prefix")

	DemonstrateSimpleIterator(ctx, engine, prefix)

	DemonstrateCommonIndexIterator(ctx, engine, prefix)
	DemonstrateIndexIterator(ctx, engine, prefix)

	DemonstrateCommonPkIterator(ctx, engine, pkprefix)
	DemonstratePkIterator(ctx, engine, pkprefix)

	DemonstrateLockerByPkPrefix(ctx, engine, pkprefix)
	DemonstrateTryLockUnlockByPkPrefix(ctx, engine, pkprefix)

	DemonstrateFirstLastByPKPrefix(ctx, engine, pkprefix)
	DemonstrateFirstLastByIndexPrefix(ctx, engine, prefix)

	fmt.Println("all..")
	if _, err := AuditKeyspace(ctx, engine); err != nil {
		// тест «палится», если нашли мусор или висячие индексы
		log.Fatalf("audit failed: %v", err)
	}
	fmt.Println("all done.")
}

// AuditReport — краткая сводка.
type AuditReport struct {
	Total                 uint64
	Data, Idx, Uniq, Lock uint64
	Unknown               uint64
	DanglingIdx           uint64
	DanglingUniq          uint64
	BySpace               map[string]SpaceStats // статистика по "пространствам" (db-name)
}

type SpaceStats struct {
	Data, Idx, Uniq, Lock uint64
}

func AuditKeyspace(ctx context.Context, eng BadgerStorageEngine) (*AuditReport, error) {
	rep := &AuditReport{BySpace: make(map[string]SpaceStats)}

	err := eng.DB().View(func(txn *badger.Txn) error {
		itOpts := badger.DefaultIteratorOptions
		itOpts.PrefetchValues = false // значения нужны только для idx/uniq
		it := txn.NewIterator(itOpts)
		defer it.Close()

		// обходим всё keyspace
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()

			rep.Total++

			switch {
			case bytes.HasPrefix(k, []byte("data:")):
				rep.Data++
				sp := spaceFromKey(k) // db-name между "data:" и следующим ':'
				ss := rep.BySpace[sp]
				ss.Data++
				rep.BySpace[sp] = ss

			case bytes.HasPrefix(k, []byte("idx:")):
				rep.Idx++
				sp := spaceFromKey(k)
				ss := rep.BySpace[sp]
				ss.Idx++
				rep.BySpace[sp] = ss

				// значение индексного ключа — это PK; проверим наличие основной записи
				var pk []byte
				if err := item.Value(func(v []byte) error {
					pk = append(pk[:0], v...)
					return nil
				}); err != nil {
					return fmt.Errorf("idx.Value: %w", err)
				}
				if _, err := txn.Get(pk); errors.Is(err, badger.ErrKeyNotFound) {
					rep.DanglingIdx++
				} else if err != nil {
					return fmt.Errorf("idx -> txn.Get(pk): %w", err)
				}

			case bytes.HasPrefix(k, []byte("uniq:")):
				rep.Uniq++
				sp := spaceFromKey(k)
				ss := rep.BySpace[sp]
				ss.Uniq++
				rep.BySpace[sp] = ss

				// предполагаем, что в uniq:value тоже хранится PK
				var pk []byte
				if err := item.Value(func(v []byte) error {
					pk = append(pk[:0], v...)
					return nil
				}); err != nil {
					return fmt.Errorf("uniq.Value: %w", err)
				}
				if _, err := txn.Get(pk); errors.Is(err, badger.ErrKeyNotFound) {
					rep.DanglingUniq++
				} else if err != nil {
					return fmt.Errorf("uniq -> txn.Get(pk): %w", err)
				}

			case bytes.HasPrefix(k, []byte("lock:")):
				rep.Lock++
				sp := spaceFromKey(k)
				ss := rep.BySpace[sp]
				ss.Lock++
				rep.BySpace[sp] = ss

			default:
				rep.Unknown++
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// печать сводки
	fmt.Printf("[audit] total=%d | data=%d idx=%d uniq=%d lock=%d unknown=%d\n",
		rep.Total, rep.Data, rep.Idx, rep.Uniq, rep.Lock, rep.Unknown)
	fmt.Printf("[audit] dangling: idx=%d uniq=%d\n", rep.DanglingIdx, rep.DanglingUniq)
	for space, st := range rep.BySpace {
		fmt.Printf("[audit][%s] data=%d idx=%d uniq=%d lock=%d\n",
			space, st.Data, st.Idx, st.Uniq, st.Lock)
	}

	// если хочется «валидировать», что нет вообще ничего лишнего:
	if rep.Unknown > 0 {
		return rep, fmt.Errorf("unknown keys present: %d", rep.Unknown)
	}
	if rep.DanglingIdx > 0 || rep.DanglingUniq > 0 {
		return rep, fmt.Errorf("dangling indexes detected: idx=%d uniq=%d", rep.DanglingIdx, rep.DanglingUniq)
	}
	return rep, nil
}

// spaceFromKey пытается вытащить имя «пространства» (db-name) из ключа вида "data:<db>:<ver>:..."
// Если формат неожиданно другой — вернёт пустую строку, это ок для отчёта.
func spaceFromKey(k []byte) string {
	s := string(k)
	// скипаем первый сегмент ("data"/"idx"/"uniq"/"lock")
	i := strings.IndexByte(s, ':')
	if i < 0 || i+1 >= len(s) {
		return ""
	}
	rest := s[i+1:]
	j := strings.IndexByte(rest, ':')
	if j < 0 {
		return rest
	}
	return rest[:j]
}

func DemonstrateFirstLastByIndexPrefix(ctx context.Context, engine BadgerStorageEngine, prefix []byte) {
	fmt.Println("Starting DemonstrateFirstLastByIndexPrefix...")
	fmt.Printf(">> scan index prefix=%q\n", string(prefix))

	firstPK, firstVal, err := engine.FirstByIndexPrefixRaw(ctx, prefix)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			fmt.Println("no records for this index prefix (first)")
		} else {
			fmt.Printf("FirstByIndexPrefixRaw error: %v\n", err)
		}
	} else {
		fmt.Printf("FIRST: pk=%q, valueLen=%d%s\n",
			string(firstPK), len(firstVal), previewSuffix(firstVal))
	}

	// Последний по индексу
	lastPK, lastVal, err := engine.LastByIndexPrefixRaw(ctx, prefix)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			fmt.Println("no records for this index prefix (last)")
		} else {
			fmt.Printf("LastByIndexPrefixRaw error: %v\n", err)
		}
	} else {
		fmt.Printf("LAST:  pk=%q, valueLen=%d%s\n",
			string(lastPK), len(lastVal), previewSuffix(lastVal))
	}

	fmt.Println("Stopped DemonstrateFirstLastByIndexPrefix.")
}

func DemonstrateFirstLastByPKPrefix(ctx context.Context, engine BadgerStorageEngine, prefix []byte) {
	fmt.Println("Starting DemonstrateFirstLastByPKPrefix...")
	fmt.Printf(">> scan prefix=%q\n", string(prefix))

	// Первый
	firstPK, firstVal, err := engine.FirstByPKPrefixRaw(ctx, prefix)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			fmt.Println("no keys with this prefix (first)")
		} else {
			fmt.Printf("FirstByPKPrefixRaw error: %v\n", err)
		}
	} else {
		fmt.Printf("FIRST: pk=%q, valueLen=%d%s\n",
			string(firstPK), len(firstVal), previewSuffix(firstVal))
	}

	// Последний
	lastPK, lastVal, err := engine.LastByPKPrefixRaw(ctx, prefix)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			fmt.Println("no keys with this prefix (last)")
		} else {
			fmt.Printf("LastByPKPrefixRaw error: %v\n", err)
		}
	} else {
		fmt.Printf("LAST:  pk=%q, valueLen=%d%s\n",
			string(lastPK), len(lastVal), previewSuffix(lastVal))
	}
	fmt.Println("Stopped DemonstrateFirstLastByPKPrefix.")
}

func previewSuffix(v []byte) string {
	const max = 64
	if len(v) == 0 {
		return ""
	}
	if len(v) > max {
		v = v[:max]
	}
	buf := make([]byte, len(v))
	for i, b := range v {
		if b >= 32 && b <= 126 { // печатаемые ASCII
			buf[i] = b
		} else {
			buf[i] = '.'
		}
	}
	return fmt.Sprintf(", ascii=%q...", string(buf))
}

func DemonstrateTryLockUnlockByPkPrefix(ctx context.Context, engine BadgerStorageEngine, prefix []byte) {
	fmt.Println("Starting DemonstrateTryLockUnlockByPkPrefix...")

	const (
		owner = "demo-worker"
		ttl   = 30 * time.Second
	)

	var cursor []byte
	users := make([]*v1model.User, 0)

	f := OnLanesFunc(func(ctx context.Context, pks [][]byte, vals [][]byte) error {
		for i := range pks {
			pk := pks[i]

			// 1) Пытаемся взять lock в короткой RW-транзакции
			var token string
			err := engine.ExecuteReadWriteWithContext(ctx, func(_ context.Context, tx *badger.Txn) error {
				tok, err := engine.TryLock(ctx, tx, pk, owner, ttl)
				if err != nil {
					return err
				}
				token = tok
				return nil
			})
			if err != nil {
				if errors.Is(err, ErrLocked) {
					fmt.Printf("pk=%s is locked by someone else, skip\n", string(pk))
					continue
				}
				fmt.Printf("try-lock failed for pk=%s: %v\n", string(pk), err)
				continue
			}

			// 2) Работа с объектом (вне транзакции блокировки)
			var u v1model.User
			if err := engine.Unmarshal(vals[i], &u); err != nil {
				fmt.Printf("unmarshal failed for pk=%s: %v\n", string(pk), err)
				// даже при ошибке — обязательно снять лок
				_ = engine.ExecuteReadWriteWithContext(ctx, func(_ context.Context, tx *badger.Txn) error {
					return engine.Unlock(ctx, tx, pk, owner, token)
				})
				continue
			}
			users = append(users, &u)
			fmt.Printf("processed pk=%s id=%d name=%q type=%q office=%q\n",
				string(pk), u.GetId(), u.GetName(), u.GetType(), u.GetOffice())

			// 3) Снимаем лок в отдельной короткой RW-транзакции
			if err := engine.ExecuteReadWriteWithContext(ctx, func(_ context.Context, tx *badger.Txn) error {
				return engine.Unlock(ctx, tx, pk, owner, token)
			}); err != nil {
				fmt.Printf("unlock failed for pk=%s: %v\n", string(pk), err)
			}
		}
		return nil
	})

	if _, err := engine.IterationByPkPrefix(ctx, prefix, f, cursor, 0); err != nil {
		fmt.Println("iterByPkPrefix error:", err)
	}

	fmt.Printf("Total processed users: %d\n", len(users))
	fmt.Println("Stopped DemonstrateTryLockUnlockByPkPrefix.")
}

func DemonstrateLockerByPkPrefix(ctx context.Context, engine BadgerStorageEngine, prefix []byte) {
	fmt.Println("Starting DemonstrateLockerByPkPrefix demonstration...")
	var pkcursor []byte // пустой — с начала
	pkUsers1 := make([]*v1model.User, 0)
	f := OnLanesFunc(func(ctx context.Context, pks [][]byte, vals [][]byte) error {
		// наивная проверка блокировок
		err := engine.ExecuteReadWithContext(ctx, func(_ context.Context, tx *badger.Txn) error {
			for i := range vals {
				currentPk := pks[i]
				ok, err := engine.IsLockedTx(ctx, tx, currentPk)
				if err != nil {
					fmt.Printf("isLockedTx failed for pk=%s: %v\n", string(currentPk), err)
					continue
				}
				if ok {
					fmt.Printf("pk=%s is already locked, skipping...\n", string(currentPk))
					continue // пропускаем уже заблокированные ключи
				}

				var u v1model.User
				if err := engine.Unmarshal(vals[i], &u); err != nil {
					fmt.Printf("unmarshal entity failed for pk=%s: %v\n", string(pks[i]), err)
					continue
				}
				pkUsers1 = append(pkUsers1, &u)

				// Логируем PK и сущность (индексный ключ тут недоступен)
				fmt.Printf("pk=%s id=%d name=%q type=%q office=%q\n",
					string(pks[i]), u.GetId(), u.GetName(), u.GetType(), u.GetOffice())
			}
			return nil
		})
		if err != nil {
			fmt.Println("ExecuteReadWriteWithContext error:", err)
			return err
		}

		return nil
	})
	if _, err := engine.IterationByPkPrefix(ctx, prefix, f, pkcursor, 0); err != nil {
		fmt.Println("iterByPkPrefix error:", err)
	}
	fmt.Println("Total pk users found:", len(pkUsers1))
	fmt.Println("Stopped DemonstrateLockerByPkPrefix demonstration.")
}

func DemonstrateSimpleIterator(ctx context.Context, engine BadgerStorageEngine, prefix []byte) {
	fmt.Println("Starting DemonstrateSimpleIterator demonstration...")
	var (
		users     []*v1model.User
		cursor    []byte // для пагинации; пустой — первая страница
		pageLimit = 50
	)

	err := engine.DB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true // value индексного ключа — это PK
		opts.Prefix = prefix       // хинт для итератора

		it := txn.NewIterator(opts)
		defer it.Close()

		// стартовая позиция
		if len(cursor) > 0 {
			it.Seek(cursor)
			if it.Valid() && bytes.Equal(it.Item().Key(), cursor) {
				it.Next()
			}
		} else {
			it.Seek(prefix)
		}

		count := 0
		for ; it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			fmt.Println(string(item.Key()))

			// достаём PK из value
			var pk []byte
			if err := item.Value(func(v []byte) error {
				pk = append(pk[:0], v...)
				return nil
			}); err != nil {
				return err
			}

			// читаем основную запись
			dataItem, err := txn.Get(pk)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					continue // висячий индекс
				}
				return err
			}
			var u v1model.User
			if err := dataItem.Value(func(b []byte) error { return engine.Unmarshal(b, &u) }); err != nil {
				return err
			}
			users = append(users, &u)
			count++

			if count >= pageLimit {
				cursor = item.KeyCopy(nil)
				break
			}
		}
		return nil
	})
	if err != nil {
		fmt.Println("list by index:", err)
	}

	fmt.Println("found users:", len(users))
	fmt.Println("Stopped DemonstrateSimpleIterator demonstration.")
}

func DemonstrateCommonIndexIterator(ctx context.Context, engine BadgerStorageEngine, prefix []byte) {
	fmt.Println("Starting DemonstrateCommonIndexIterator demonstration...")
	var (
		users        []*v1model.User
		pageSize     = 1
		indexcursor1 []byte
	)

	for {
		// pks: PK основной записи; vals: bytes основной записи (payload)
		pks, vals, next, err := engine.PageByIndexPrefixRaw(
			context.Background(),
			prefix,
			indexcursor1, // cursor1 (полный индексный ключ предыдущего элемента страницы)
			pageSize,     // limit
		)
		if err != nil {
			fmt.Println("PageByIndexPrefixRaw error:", err)
			break
		}

		for i := range vals {
			var u v1model.User
			if err := engine.Unmarshal(vals[i], &u); err != nil {
				fmt.Printf("unmarshal entity failed for pk=%s: %v\n", string(pks[i]), err)
				continue
			}
			users = append(users, &u)

			// Логируем PK и сущность (индексный ключ тут недоступен)
			fmt.Printf("pk=%s id=%d name=%q type=%q office=%q\n",
				string(pks[i]), u.GetId(), u.GetName(), u.GetType(), u.GetOffice())
		}

		if len(next) == 0 {
			break
		}
		indexcursor1 = next // ПОЛНЫЙ индексный ключ текущего элемента: начнём со следующего на следующей итерации
	}
	fmt.Println("Total users found:", len(users))
	fmt.Println("Stopped DemonstrateCommonIndexIterator demonstration.")
}

func DemonstrateCommonPkIterator(ctx context.Context, engine BadgerStorageEngine, prefix []byte) {
	fmt.Println("Starting DemonstrateCommonPkIterator demonstration...")
	var pkcursor1 []byte // пустой — с начала
	var pageSize = 1
	pkUsers := make([]*v1model.User, 0)

	for {
		pks, vals, next, err := engine.PageByPKPrefixRaw(
			context.Background(),
			prefix,
			pkcursor1, // startAfter (полный индексный ключ предыдущего элемента страницы)
			pageSize,  // limit
		)
		if err != nil {
			fmt.Println("PageByIndexPrefixRaw error:", err)
			break
		}

		for i := range vals {
			var u v1model.User
			if err := engine.Unmarshal(vals[i], &u); err != nil {
				fmt.Printf("unmarshal entity failed for pk=%s: %v\n", string(pks[i]), err)
				continue
			}
			pkUsers = append(pkUsers, &u)
			fmt.Printf("pk=%s id=%d name=%q type=%q office=%q\n",
				string(pks[i]), u.GetId(), u.GetName(), u.GetType(), u.GetOffice())
		}

		if len(next) == 0 {
			break
		}
		pkcursor1 = next // ПОЛНЫЙ индексный ключ текущего элемента: начнём со следующего на следующей итерации
	}

	fmt.Println("Total pkUsers found:", len(pkUsers))
	fmt.Println("Stopped DemonstrateCommonPkIterator demonstration.")
}

func DemonstratePkIterator(ctx context.Context, engine BadgerStorageEngine, prefix []byte) {
	fmt.Println("Starting DemonstratePkIterator demonstration...")
	var pkcursor []byte // пустой — с начала
	pkUsers1 := make([]*v1model.User, 0)
	f := OnLanesFunc(func(ctx context.Context, pks [][]byte, vals [][]byte) error {
		for i := range vals {
			var u v1model.User
			if err := engine.Unmarshal(vals[i], &u); err != nil {
				fmt.Printf("unmarshal entity failed for pk=%s: %v\n", string(pks[i]), err)
				continue
			}
			pkUsers1 = append(pkUsers1, &u)

			// Логируем PK и сущность (индексный ключ тут недоступен)
			fmt.Printf("pk=%s id=%d name=%q type=%q office=%q\n",
				string(pks[i]), u.GetId(), u.GetName(), u.GetType(), u.GetOffice())
		}
		return nil
	})
	if _, err := engine.IterationByPkPrefix(ctx, prefix, f, pkcursor, 0); err != nil {
		fmt.Println("iterByPkPrefix error:", err)
	}
	fmt.Println("Total pk users found:", len(pkUsers1))
	fmt.Println("Stopped DemonstratePkIterator demonstration.")
}

func DemonstrateIndexIterator(ctx context.Context, engine BadgerStorageEngine, prefix []byte) {
	fmt.Println("Starting DemonstrateIndexIterator demonstration...")

	var indexcursor []byte // пустой — с начала
	indexUsers := make([]*v1model.User, 0)
	f := OnLanesFunc(func(ctx context.Context, pks [][]byte, vals [][]byte) error {
		for i := range vals {
			var u v1model.User
			if err := engine.Unmarshal(vals[i], &u); err != nil {
				fmt.Printf("unmarshal entity failed for pk=%s: %v\n", string(pks[i]), err)
				continue
			}
			indexUsers = append(indexUsers, &u)

			// Логируем PK и сущность (индексный ключ тут недоступен)
			fmt.Printf("pk=%s id=%d name=%q type=%q office=%q\n",
				string(pks[i]), u.GetId(), u.GetName(), u.GetType(), u.GetOffice())
		}
		return nil
	})
	if _, err := engine.IterationByIndexPrefix(ctx, prefix, f, indexcursor, 0); err != nil {
		fmt.Println("iterByPkPrefix error:", err)
	}

	fmt.Println("Total indexUsers users found:", len(indexUsers))
	fmt.Println("Stopped DemonstrateIndexIterator demonstration.")
}
