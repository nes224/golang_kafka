package repo

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

type Event struct {
	EventID   string    `db:"event_id"`
	EventType string    `db:"event_type"`
	Timestamp time.Time `db:"timestamp"`
}

type EventRepo struct {
	db        *sqlx.DB
	tableName string
}

func NewEventRepo(db *sqlx.DB) *EventRepo {
	return &EventRepo{db: db, tableName: "events"}
}

func (r *EventRepo) Insert(ctx context.Context, tx *sqlx.Tx, e *Event) (string, error) {
	q := fmt.Sprintf(
		"INSERT INTO %s (event_id, event_type, timestamp) VALUES (:event_id, :event_type, :timestamp)",
		r.tableName,
	)
	if _, err := tx.NamedExecContext(ctx, q, e); err != nil {
		return "", err
	}
	return e.EventID, nil
}

// TxClosure — generic transaction wrapper (begin/commit/rollback อัตโนมัติ)
// port ตรงจากฝั่ง Kafka · ครอบ inbox insert + business insert ให้ atomic
func TxClosure[T any](
	ctx context.Context,
	r *EventRepo,
	fn func(ctx context.Context, tx *sqlx.Tx) (T, error),
) (res T, err error) {
	tx, err := r.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return res, fmt.Errorf("begin tx: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback()
			return
		}
		if cErr := tx.Commit(); cErr != nil {
			err = fmt.Errorf("commit: %w", cErr)
		}
	}()

	res, err = fn(ctx, tx)
	return res, err
}
