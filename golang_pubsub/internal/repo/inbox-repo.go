package repo

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// InboxRepo — implement Inbox Pattern (ดู INBOX_PATTERN.md §5.2)
// ใช้ป้องกัน process ซ้ำ เพราะ Pub/Sub การันตี at-least-once เหมือน Kafka
type InboxRepo struct {
	tableName string
}

func NewInboxRepo() *InboxRepo {
	return &InboxRepo{tableName: "inbox"}
}

// MarkProcessed พยายามจอง event_id ลง inbox ภายใน tx ที่ส่งเข้ามา (atomic กับ business)
//   - true  = event ใหม่ (จองสำเร็จ → ทำ business ต่อได้)
//   - false = เคย process แล้ว (ชน PK → ON CONFLICT skip → ข้าม business)
//
// ใช้ ON CONFLICT DO NOTHING + RowsAffected แทน Get-ก่อน-Insert
// → atomic ในคำสั่งเดียว ไม่มี race window (INBOX_PATTERN.md §6)
func (r *InboxRepo) MarkProcessed(ctx context.Context, tx *sqlx.Tx, eventID, consumer string) (bool, error) {
	q := fmt.Sprintf(
		"INSERT INTO %s (event_id, consumer) VALUES ($1, $2) ON CONFLICT (event_id) DO NOTHING",
		r.tableName,
	)
	res, err := tx.ExecContext(ctx, q, eventID, consumer)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil // 1 = insert จริง (ใหม่) · 0 = ชน (ซ้ำ)
}
