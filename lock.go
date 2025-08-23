package badger_sdk

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type Locker interface {
	TryLock(ctx context.Context, tx *badger.Txn, pk []byte, owner string, ttl time.Duration) (string, error)
	Unlock(ctx context.Context, tx *badger.Txn, pk []byte, owner, tokenHex string) error
	RenewLock(ctx context.Context, tx *badger.Txn, pk []byte, owner, tokenHex string, ttl time.Duration) error
	IsLockedTx(ctx context.Context, tx *badger.Txn, pk []byte) (bool, error)
}

type PkLocker struct{}

func NewPkLocker() *PkLocker {
	return &PkLocker{}
}

var (
	ErrLocked   = errors.New("locked")
	ErrNotOwner = errors.New("not lock owner")
)

// lock:<pk>
func lockKeyForPK(pk []byte) []byte {
	return append([]byte("lock:"), pk...)
}

func genToken(n int) ([]byte, string) {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b, hex.EncodeToString(b)
}

// TryLock ставит lock:<pk> с TTL внутри ПЕРЕДАННОЙ RW-транзакции tx.
// Значение: "owner|tokenHex". Возвращает tokenHex.
func (pkl *PkLocker) TryLock(ctx context.Context, tx *badger.Txn, pk []byte, owner string, ttl time.Duration) (string, error) {
	lk := lockKeyForPK(pk)

	_, tokenHex := genToken(16)
	val := []byte(owner + "|" + tokenHex)

	// если ключ уже есть — занято
	if _, err := tx.Get(lk); err == nil {
		return "", ErrLocked
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		return "", fmt.Errorf("trylock: get lock key: %w", err)
	}

	// пишем лок
	if err := tx.SetEntry(badger.NewEntry(lk, val).WithTTL(ttl)); err != nil {
		return "", fmt.Errorf("trylock: set lock key: %w", err)
	}
	return tokenHex, nil
}

// Unlock снимает замок, только если совпадают owner и tokenHex.
// Работает внутри ПЕРЕДАННОЙ RW-транзакции tx.
func (pkl *PkLocker) Unlock(ctx context.Context, tx *badger.Txn, pk []byte, owner, tokenHex string) error {
	lk := lockKeyForPK(pk)
	expect := []byte(owner + "|" + tokenHex)

	it, err := tx.Get(lk)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil // уже снят/истёк
	}
	if err != nil {
		return fmt.Errorf("unlock: get lock key: %w", err)
	}

	var got []byte
	if err := it.Value(func(v []byte) error {
		got = append(got[:0], v...)
		return nil
	}); err != nil {
		return fmt.Errorf("unlock: read value: %w", err)
	}

	if !equalBytes(got, expect) {
		return ErrNotOwner
	}
	if err := tx.Delete(lk); err != nil {
		return fmt.Errorf("unlock: delete: %w", err)
	}
	return nil
}

// RenewLock продлевает TTL, если замок принадлежит (owner, tokenHex).
// Работает внутри ПЕРЕДАННОЙ RW-транзакции tx.
func (pkl *PkLocker) RenewLock(ctx context.Context, tx *badger.Txn, pk []byte, owner, tokenHex string, ttl time.Duration) error {
	lk := lockKeyForPK(pk)
	expect := []byte(owner + "|" + tokenHex)

	it, err := tx.Get(lk)
	if err != nil {
		return fmt.Errorf("renew: get lock key: %w", err)
	}

	var got []byte
	if err := it.Value(func(v []byte) error {
		got = append(got[:0], v...)
		return nil
	}); err != nil {
		return fmt.Errorf("renew: read value: %w", err)
	}

	if !equalBytes(got, expect) {
		return ErrNotOwner
	}

	// перезаписываем то же значение с новым TTL
	if err := tx.SetEntry(badger.NewEntry(lk, got).WithTTL(ttl)); err != nil {
		return fmt.Errorf("renew: set with ttl: %w", err)
	}
	return nil
}

// IsLockedTx — быстрый чек внутри ПЕРЕДАННОЙ транзакции.
// true, если lock-ключ существует (не истёк).
func (pkl *PkLocker) IsLockedTx(ctx context.Context, tx *badger.Txn, pk []byte) (bool, error) {
	_, err := tx.Get(lockKeyForPK(pk))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	return false, err
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
