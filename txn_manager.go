package badger_sdk

import (
	"context"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"time"
)

type RWTx func(ctx context.Context, tx *badger.Txn) error
type RTx func(ctx context.Context, tx *badger.Txn) error

type TransactionManager interface {
	ExecuteReadWriteWithContext(ctx context.Context, fn RWTx) error
	ExecuteReadWithContext(ctx context.Context, action RTx) error
}

type Manager struct {
	engine *Engine
	opt    TxnManagerOptions
}

type TxnManagerOptions struct {
	MaxRetries  int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

func NewTransactionManager(engine *Engine, opts TxnManagerOptions) *Manager {
	options := TxnManagerOptions{
		MaxRetries:  5,
		BaseBackoff: 5 * time.Millisecond,
		MaxBackoff:  150 * time.Millisecond,
	}

	if opts.MaxRetries > 0 {
		options.MaxRetries = opts.MaxRetries
	}
	if opts.BaseBackoff > 0 {
		options.BaseBackoff = opts.BaseBackoff
	}
	if opts.MaxBackoff > 0 {
		options.MaxBackoff = opts.MaxBackoff
	}

	return &Manager{
		engine: engine,
		opt:    options,
	}
}

func (m *Manager) ExecuteReadWithContext(ctx context.Context, action RTx) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("[ExecuteReadWithContext] context error: %w", err)
	}

	// read-only транзакция (false)
	tx := m.engine.db.NewTransaction(false)
	defer tx.Discard()

	// защита от паники внутри action
	if err := func() (err error) {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("[ExecuteReadWithContext] panic in RO txn: %v", p)
			}
		}()
		return action(ctx, tx)
	}(); err != nil {
		return fmt.Errorf("[ExecuteReadWithContext] RO txn failed: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("[ExecuteReadWithContext] context error: %w", err)
	}

	return nil
}

func (m *Manager) ExecuteReadWriteWithContext(ctx context.Context, action RWTx) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("[ExecuteReadWriteWithContext] context error: %w", err)
		}

		tx := m.engine.db.NewTransaction(true)

		runErr := func() (err error) {
			defer func() {
				if p := recover(); p != nil {
					err = fmt.Errorf("[ExecuteReadWriteWithContext] panic in RW txn: %v", p)
				}
			}()
			return action(ctx, tx)
		}()

		if runErr != nil {
			tx.Discard()
			return fmt.Errorf("[ExecuteReadWriteWithContext] RW txn failed: %w", runErr)
		}

		if err := ctx.Err(); err != nil {
			tx.Discard()
			return fmt.Errorf("[ExecuteReadWriteWithContext] context error: %w", err)
		}

		if err := tx.Commit(); err != nil {
			if errors.Is(err, badger.ErrConflict) && attempt < m.opt.MaxRetries {
				tx.Discard()
				if serr := sleepWithJitter(ctx, m.opt.BaseBackoff, m.opt.MaxBackoff, attempt+1); serr != nil {
					return serr
				}
				continue
			}
			tx.Discard()
			return fmt.Errorf("[ExecuteReadWriteWithContext] RW txn failed: %w", err)
		}

		return nil
	}
}

func sleepWithJitter(ctx context.Context, base, max time.Duration, attempt int) error {
	if attempt < 1 {
		attempt = 1
	}

	backoff := base * time.Duration(attempt)
	if max > 0 && backoff > max {
		backoff = max
	}
	if backoff <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	t := time.NewTimer(backoff)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("[sleepWithJitter] context error: %w", ctx.Err())
	case <-t.C:
		return nil
	}
}
